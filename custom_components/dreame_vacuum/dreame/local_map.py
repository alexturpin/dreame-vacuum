"""Feed authenticated local MIoT reports and objects into Tasshack's maps."""

from copy import deepcopy
from hashlib import md5, sha256
from io import BytesIO
import json
import logging
import re
from threading import RLock
import time

from PIL import Image

from .exceptions import DeviceException
from .map import DreameMapVacuumMapEditor, DreameMapVacuumMapManager, DreameVacuumMapDecoder
from .types import CleaningHistory, MapFrameType

_LOGGER = logging.getLogger(__name__)
_OBJECT_NAME = re.compile(r"local/[0-9a-f]{32}(?:/[0-9]{1,4})?")
_OBSTACLE_FILENAME = re.compile(r"/data/record/[0-9]{1,10}\.jpg-[0-9]{1,20}")
_MAX_MAP_OBJECT_BYTES = 2 * 1024 * 1024
_MIN_WALL_CLOCK_MS = 1577826000000  # Tasshack's cutoff for robot uptime.
_UPTIME_CLOCK_SKEW_MS = 1000
_MAP_SERVICE = 6
_MAP_DATA, _OBJECT, _ROBOT_TIME, _MAP_LIST, _RECOVERY_LIST, _LEGACY_MAP = 1, 3, 5, 8, 9, 13
_I_REQUEST = {"req_type": 1, "frame_type": "I", "force_type": 1}


def _is_object_name(value):
    return isinstance(value, str) and _OBJECT_NAME.fullmatch(value) is not None


def _is_obstacle_jpeg(data):
    if not data or not (data.startswith(b"\xff\xd8") and data.endswith(b"\xff\xd9")):
        return False
    try:
        with Image.open(BytesIO(data)) as image:
            if image.format != "JPEG" or not (0 < image.width <= 2048 and 0 < image.height <= 2048):
                return False
            image.load()
            return True
    except (OSError, Image.DecompressionBombError):
        return False


class DreameLocalMapEditor(DreameMapVacuumMapEditor):
    def refresh_map(self, map_id=None):
        self._refresh_map(map_id)


class _ProjectedUptime(int):
    """Distinguish timestamps projected from uptime from genuine wall times."""


class DreameLocalMapManager(DreameMapVacuumMapManager):
    """Drive map updates from the bridge with no vendor cloud or polling thread."""

    def __init__(self, protocol):
        super().__init__(protocol)
        self.editor = DreameLocalMapEditor(self)
        self._lock = RLock()
        self._revision = 0
        self._recent_frames = []
        self._uptime_offset_ms = None
        self._pending_object = None
        self._loaded_lists = {}
        self._list_retry_at = 0.0
        self._map_request_at = 0.0
        self._last_frame_received_at = None
        self._last_gap = None

    def reset_session(self):
        with self._lock:
            self._init_data()
            self._map_list_object_name = self._map_list_md5 = self._recovery_map_list_object_name = None
            self._recent_frames.clear()
            self._uptime_offset_ms = None
            self._pending_object = None
            self._loaded_lists.clear()
            self._list_retry_at = 0.0
            self._map_request_at = 0.0
            self._last_frame_received_at = None
            self._last_gap = None
            self._device_running = self._device_docked = False
            self._ready = False
            self._revision += 1
            self._map_data_changed(True)

    def checkpoint(self):
        """Capture the state native edit methods can change optimistically."""
        with self._lock:
            return self._revision, deepcopy((
                self._map_data, self._saved_map_data, self._map_list, self._selected_map_id,
                self._current_map_id, self._current_frame_id, self._current_timestamp_ms, self._updated_frame_id,
            ))

    @property
    def revision(self) -> int:
        """Return the current revision of observed map state."""
        with self._lock:
            return self._revision

    @property
    def selected_map(self):
        """Expose a saved map even before the robot provides a live frame."""
        selected = self._saved_map_data.get(self._selected_map_id)
        if selected is not None:
            return selected
        if len(self._map_list) == 1:
            return self._saved_map_data.get(self._map_list[0])

    def restore_checkpoint(self, checkpoint):
        revision, state = checkpoint
        with self._lock:
            if self._revision == revision:
                (
                    self._map_data, self._saved_map_data, self._map_list, self._selected_map_id,
                    self._current_map_id, self._current_frame_id, self._current_timestamp_ms, self._updated_frame_id,
                ) = state
                self._map_data_changed(True)

    def schedule_update(self, wait=None):
        # The coordinator supplies state updates. The upstream background loop
        # can make requests unrelated to an explicit user action.
        pass

    def set_device_running(self, running, docked):
        self._device_running, self._device_docked = running, docked

    def _map_objects_available(self):
        return True

    def _get_interim_file_data(self, object_name="", timestamp=None):
        if not _is_object_name(object_name):
            return None
        response = self._protocol.get_map_object(object_name)
        return response if isinstance(response, bytes) and 0 < len(response) <= _MAX_MAP_OBJECT_BYTES else None

    def _get_history_file_data(self, object_name):
        return self._get_interim_file_data(object_name)

    def get_recovery_map_file(self, map_id, index):
        saved = self._saved_map_data.get(map_id)
        position = int(index) - 1
        if saved and saved.recovery_map_list and 0 <= position < len(saved.recovery_map_list):
            name = saved.recovery_map_list[position].object_name
            data = self._get_interim_file_data(name)
            if data and data.startswith(b"BZh"):
                return data, self._protocol.get_map_download(name), name
        return None, None, None

    def get_obstacle_image(self, map_data, index):
        obstacle = (map_data.obstacles or {}).get(str(index)) if map_data else None
        if (obstacle is None
                or (obstacle.picture_status is not None and obstacle.picture_status.value != 2)):
            return None, None
        name = obstacle.file_name
        if not _is_object_name(name):
            filename = f"{name}-{obstacle.object_id}"
            if not _OBSTACLE_FILENAME.fullmatch(filename):
                return None, None
            name = "local/" + sha256(filename.encode("ascii")).hexdigest()[:32]
        image = self._get_interim_file_data(name)
        if _is_obstacle_jpeg(image):
            return image, obstacle
        if image and isinstance(obstacle.key, str) and 1 < len(obstacle.key) <= 256:
            try:
                image = self._decode_obstacle_object(image, obstacle.key)
                if _is_obstacle_jpeg(image):
                    return image, obstacle
            except ValueError:
                _LOGGER.debug("Unable to decode local obstacle image")
        return None, None

    def _request_i_map(self, start_time=None):
        # Ingestion does not recurse into network requests. update() sees
        # the queued delta and chooses an I/P request on its own cadence.
        return False

    def _request_next_p_map(self, map_id, frame_id):
        return False

    def _queue_partial_map(self, map_data):
        if self._partial_map_queue_size() >= 16:
            return
        super()._queue_partial_map(map_data)
        for map_id in tuple(self._map_data_queue):
            if map_id != self._latest_map_id:
                del self._map_data_queue[map_id]

    def _frame_timestamp(self, frame_time, report_time):
        for timestamp in (frame_time, report_time):
            if timestamp is not None and timestamp >= _MIN_WALL_CLOCK_MS:
                return timestamp

        uptime = frame_time if frame_time and frame_time > 0 else report_time
        if uptime is None:
            return int(time.time() * 1000)
        if self._uptime_offset_ms is None:
            # Anchor once per session so late I/P frames keep their robot-time order.
            latest = self._latest_map_timestamp_ms or self._current_timestamp_ms or 0
            self._uptime_offset_ms = max(int(time.time() * 1000), latest + 1) - uptime
        return _ProjectedUptime(self._uptime_offset_ms + uptime)

    def _bound_uptime_projection(self, timestamp):
        lead = timestamp - int(time.time() * 1000)
        if lead <= _UPTIME_CLOCK_SKEW_MS:
            return
        # Replayed reports can span hours while arriving in one poll. Shift all
        # projected times together to keep the I/P queue in the same clock domain.
        self._uptime_offset_ms -= lead

        def shift(value):
            return _ProjectedUptime(value - lead) if isinstance(value, _ProjectedUptime) else value

        self._current_timestamp_ms = shift(self._current_timestamp_ms)
        self._latest_map_timestamp_ms = shift(self._latest_map_timestamp_ms)
        if self._map_data is not None:
            self._map_data.timestamp_ms = shift(self._map_data.timestamp_ms)
        for frames in self._map_data_queue.values():
            for frame in frames.values():
                frame.timestamp_ms = shift(frame.timestamp_ms)

    def _ingest_frame(self, raw, timestamp, key=None) -> bool | None:
        """Return whether a valid frame changed the live map, or None on failure."""
        if not isinstance(raw, str) or not 0 < len(raw) <= _MAX_MAP_OBJECT_BYTES:
            return None
        watermark = self._latest_map_timestamp_ms, self._latest_map_id
        uptime_offset = self._uptime_offset_ms
        try:
            fingerprint = sha256((raw + "," + (key or "")).encode()).digest()
            if fingerprint in self._recent_frames:
                return False
            decoder = DreameVacuumMapDecoder
            partial = decoder.decode_map_partial(raw, self.map_version, self._aes_iv, key)
            if partial is None or partial.frame_type not in (MapFrameType.I.value, MapFrameType.P.value):
                return None
            width = decoder._read_int_16_le(partial.raw, 19)
            height = decoder._read_int_16_le(partial.raw, 21)
            pixels_end = decoder.HEADER_SIZE + width * height
            if len(partial.raw) < pixels_end:
                _LOGGER.warning("Ignoring a local map frame with incomplete pixels")
                return None
            # The native decoder uses {} for both absent/empty metadata and
            # metadata it could not parse. Resolve that ambiguity when bytes exist.
            if len(partial.raw) > pixels_end and not partial.data_json:
                if json.loads(partial.raw[pixels_end:].decode("utf8")) != {}:
                    _LOGGER.warning("Ignoring a local map frame with invalid metadata")
                    return None

            partial.timestamp_ms = self._frame_timestamp(partial.timestamp_ms, timestamp)
            if self._latest_map_timestamp_ms is None or partial.timestamp_ms > self._latest_map_timestamp_ms:
                self._latest_map_timestamp_ms = partial.timestamp_ms
                self._latest_map_id = partial.map_id
            before = self._current_map_id, self._current_frame_id, self._current_timestamp_ms
            super()._add_map_data(partial)
        except (ArithmeticError, IndexError, KeyError, RecursionError, TypeError, ValueError):
            self._latest_map_timestamp_ms, self._latest_map_id = watermark
            self._uptime_offset_ms = uptime_offset
            _LOGGER.warning("Ignoring an invalid local map frame")
            return None
        self._recent_frames.append(fingerprint)
        self._recent_frames = self._recent_frames[-32:]
        changed = before != (self._current_map_id, self._current_frame_id, self._current_timestamp_ms)
        if isinstance(partial.timestamp_ms, _ProjectedUptime) and (
            changed or partial.timestamp_ms == self._latest_map_timestamp_ms
        ):
            self._bound_uptime_projection(partial.timestamp_ms)
        if changed:
            if self._map_data is not None and self._current_frame_id is not None:
                # Renderer invalidation also changes MapData.last_updated. Only
                # accepted robot frames may reset the live-map recovery clock.
                self._last_frame_received_at = time.monotonic()
                self._delete_invalid_partial_maps()
            self._revision += 1
        return changed

    def _ingest_object(self, reference, timestamp):
        object_name, _, key = reference.partition(",")
        if not _is_object_name(object_name) or len(key) > 256:
            return
        # Temporary names are reused. A report may precede the new upload,
        # leaving the previous ciphertext at the same address for this fetch.
        self._pending_object = (reference, timestamp)
        response = self._get_interim_file_data(object_name)
        if response is None:
            return
        try:
            raw = response.decode("ascii")
        except UnicodeError:
            _LOGGER.warning("Ignoring a local map object with invalid encoding")
            return
        if self._ingest_frame(raw, timestamp, key or None) is not None:
            self._pending_object = None

    def ingest_properties(self, rows):
        properties = {
            row["piid"]: row.get("value")
            for row in rows
            if row.get("siid") == _MAP_SERVICE and row.get("code", 0) == 0 and "piid" in row
        }
        with self._lock:
            timestamp = properties.get(_ROBOT_TIME)
            if isinstance(timestamp, str) and timestamp.isascii() and timestamp.isdecimal() and len(timestamp) <= 16:
                timestamp = int(timestamp)
            if type(timestamp) is not int or timestamp <= 0:
                timestamp = None

            raw = properties.get(_MAP_DATA)
            reference = properties.get(_OBJECT)
            legacy = properties.get(_LEGACY_MAP)
            if not raw and not reference and isinstance(legacy, str):
                kind, _, value = legacy.partition(",")
                if kind == "0":
                    raw = value
                elif kind == "1":
                    reference = value
            if raw:
                self._ingest_frame(raw, timestamp)
            if isinstance(reference, str) and reference:
                self._ingest_object(reference, timestamp)

            self._update_list_reference(_MAP_LIST, properties.get(_MAP_LIST))
            self._update_list_reference(_RECOVERY_LIST, properties.get(_RECOVERY_LIST))
            self._ready = True
            self._retry_objects()

    def _update_list_reference(self, piid, value):
        if not isinstance(value, str) or not 0 < len(value) <= 512:
            return
        try:
            data = json.loads(value)
        except ValueError:
            data = {"object_name": value} if piid == _RECOVERY_LIST else None
        if not isinstance(data, dict):
            return
        name = data.get("object_name", data.get("obj_name"))
        if not _is_object_name(name):
            return
        if piid == _MAP_LIST:
            self.set_map_list_object_name(name, data.get("md5"))
        else:
            self.set_recovery_map_list_object_name(name)

    def request_map_list(self):
        self._load_list(_MAP_LIST, self._map_list_object_name, self._map_list_md5)

    def request_recovery_map_list(self):
        self._load_list(_RECOVERY_LIST, self._recovery_map_list_object_name)

    def _load_list(self, piid, name, checksum=None):
        needs_refresh = (
            self._need_map_list_request if piid == _MAP_LIST else self._need_recovery_map_list_request
        )
        if not name or (not needs_refresh and self._loaded_lists.get(piid) == (name, checksum)):
            return
        # Check the descriptor before applying the upstream list reconciliation.
        # Pull referenced saved maps first so a missing object cannot clear rooms.
        try:
            response = self._get_interim_file_data(name)
            if response is None:
                return
            if checksum and md5(response, usedforsecurity=False).hexdigest() != checksum:
                return
            data = json.loads(response)
            if piid == _MAP_LIST:
                if (not isinstance(data, dict) or not isinstance(data.get("mapstr"), list)
                        or len(data["mapstr"]) > 16 or type(data.get("curr_id")) is not int):
                    return
                if any(not isinstance(row, dict) for row in data["mapstr"]):
                    return
                loaded = super().request_map_list(response)
            else:
                if not isinstance(data, list) or len(data) > 16:
                    return
                loaded = super().request_recovery_map_list(response)
        except (ArithmeticError, IndexError, KeyError, TypeError, ValueError):
            _LOGGER.warning("Ignoring an invalid local map list")
            return
        if loaded:
            self._loaded_lists[piid] = (name, checksum)
            self._revision += 1

    def update(self):
        with self._lock:
            self._retry_objects()
            now = time.monotonic()
            if self._disconnected or now < self._map_request_at:
                return
            gap = None
            if self._partial_map_queue_size() and self._current_map_id == self._latest_map_id:
                gap = (self._current_map_id, self._current_frame_id + 1)
            needs_frame = (
                self._partial_map_queue_size()
                or self._need_map_request
                or (self._device_running and (
                    self._map_data is None
                    or self._last_frame_received_at is None
                    or now - self._last_frame_received_at > 30
                ))
            )
            if not needs_frame:
                return
            parameters = _I_REQUEST
            if gap and gap != self._last_gap:
                parameters = {"req_type": 1, "frame_type": "P", "map_id": gap[0], "frame_id": gap[1]}
            self._last_gap = gap
            self._map_request_at = now + 10
            self._need_map_request = self._need_new_map = False
        payload = [{"piid": 2, "value": json.dumps(parameters, separators=(",", ":"))}]
        try:
            self._ingest_action_output(self._protocol.request_map(payload))
        except (DeviceException, OSError, ValueError):
            _LOGGER.debug("Local map repair request did not return data")

    def _retry_objects(self):
        now = time.monotonic()
        if now < self._list_retry_at:
            return
        self._list_retry_at = now + 5
        if self._pending_object:
            self._ingest_object(*self._pending_object)
        self.request_map_list()
        self.request_recovery_map_list()

    def request_new_map(self):
        result = self._request_map(_I_REQUEST)
        self._ingest_action_output(result)
        return result

    def _ingest_action_output(self, result):
        if result and result.get("code") == 0 and isinstance(result.get("out"), list):
            self.ingest_properties([dict(row, siid=_MAP_SERVICE) for row in result["out"] if isinstance(row, dict)])

    @staticmethod
    def parse_history(rows, property_mapping):
        result = {}
        for row in rows:
            if row.get("siid") != 4 or row.get("eiid") not in (1, 5):
                continue
            try:
                entry = CleaningHistory(row["arguments"], property_mapping)
                if entry.date is not None:
                    result[entry.date] = entry
            except (IndexError, KeyError, OSError, TypeError, ValueError, OverflowError):
                _LOGGER.warning("Ignoring an invalid local cleaning history record")
        return [result[date] for date in sorted(result, reverse=True)]
