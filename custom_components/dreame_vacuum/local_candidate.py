"""Native local operation for a D10s Pro through the bounded bridge API."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from ipaddress import IPv4Address, ip_address
from math import isfinite
import re
from zoneinfo import ZoneInfo
from threading import get_ident, local
from uuid import uuid4

from aiohttp import ClientError, ClientTimeout
from yarl import URL

from homeassistant.const import CONF_NAME, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import LOGGER
from .coordinator import DreameVacuumDataUpdateCoordinator
from .dreame import (
    DeviceException, DreameVacuumAIProperty, DreameVacuumAutoSwitchProperty, DreameVacuumDevice,
    DreameVacuumProperty as P, DreameVacuumStrAIProperty,
)
from .dreame.types import DreameVacuumPropertyMapping, DreameVacuumTaskStatus
from .local_bridge import (
    CONF_BRIDGE_FINGERPRINT, LocalBridgeClient, LocalBridgeError, _is_hex_id, _MAX_SEQUENCE,
)

MODEL = "dreame.vacuum.r2250"
_STATE_INTERVAL = 3
_RPC_TIMEOUT = 6
_RESPONSE_LIMIT = 512 * 1024
_OBJECT = re.compile(r"local/[0-9a-f]{32}(?:/[0-9]{1,4})?")
_DOWNLOAD = re.compile(r"/v1/robot/map-download/[0-9a-f]{32}")
_BATCH_SIZE = 15
_GOTO_TIMEOUT = 180
_GOTO_SETTINGS = (P.CLEANING_MODE, P.SUCTION_LEVEL, P.WATER_VOLUME)
_MOVEMENT_ACTIONS = {(2, 1), (2, 2), (3, 1), (4, 1), (4, 2)}
_CORE_PROPERTIES = (P.STATE, P.ERROR, P.STATUS, P.TASK_STATUS, P.CHARGING_STATUS, P.BATTERY_LEVEL)
# Candidate allowlist. Keep the optional addresses aligned with the owner registry.
_OTHER_PROPERTIES = (
    P.OFF_PEAK_CHARGING, P.CLEANING_TIME, P.CLEANED_AREA,
    P.SUCTION_LEVEL, P.WATER_VOLUME, P.WATER_TANK,
    P.CLEANING_START_TIME, P.CLEAN_LOG_FILE_NAME, P.RESUME_CLEANING,
    P.CARPET_BOOST, P.CLEAN_LOG_STATUS, P.CLEANING_PAUSED,
    P.FAULTS, P.RELOCATION_STATUS, P.OBSTACLE_AVOIDANCE,
    P.AI_OBSTACLE_DETECTION, P.CLEANING_MODE, P.CUSTOMIZED_CLEANING,
    P.CHILD_LOCK, P.CARPET_SENSITIVITY, P.TIGHT_MOPPING,
    P.CARPET_RECOGNITION, P.WARN_STATUS, P.CARPET_CLEANING,
    P.CAPABILITY, P.MAP_INDEX, P.MAP_NAME,
    P.SCHEDULED_CLEAN, P.AUTO_SWITCH_SETTINGS, P.NUMERIC_MESSAGE_PROMPT,
    P.MESSAGE_PROMPT, P.TASK_TYPE, P.CLEANING_PROGRESS,
    P.DEVICE_CAPABILITY, P.DND, P.DND_START,
    P.DND_END, P.DND_TASK, P.MULTI_FLOOR_MAP,
    P.MAP_LIST, P.RECOVERY_MAP_LIST, P.MAP_RECOVERY_STATUS,
    P.MAP_BACKUP_STATUS, P.VOLUME, P.VOICE_PACKET_ID,
    P.VOICE_CHANGE_STATUS, P.SCHEDULE, P.SCHEDULE_CANCEL_REASON,
    P.MAIN_BRUSH_TIME_LEFT, P.MAIN_BRUSH_LEFT, P.SIDE_BRUSH_TIME_LEFT,
    P.SIDE_BRUSH_LEFT, P.FILTER_LEFT, P.FILTER_TIME_LEFT,
    P.FIRST_CLEANING_DATE, P.TOTAL_CLEANING_TIME, P.CLEANING_COUNT,
    P.TOTAL_CLEANED_AREA, P.TOTAL_RUNTIME, P.SENSOR_DIRTY_LEFT,
    P.SENSOR_DIRTY_TIME_LEFT, P.MOP_PAD_LEFT, P.MOP_PAD_TIME_LEFT,
)
_ADDRESS = {
    str(prop.value): mapping for prop, mapping in DreameVacuumPropertyMapping.items() if "piid" in mapping
}
_READBACK = {
    (mapping["siid"], mapping["piid"]): prop
    for prop in (*_CORE_PROPERTIES, *_OTHER_PROPERTIES)
    if (mapping := _ADDRESS.get(str(prop.value)))
}
_ACTION_READBACK = {
    (5, 1): (P.DND_TASK,),
    (6, 2): (P.MAP_LIST,),
    (6, 3): (P.MAP_BACKUP_STATUS, P.RECOVERY_MAP_LIST),
    (8, 1): (P.SCHEDULE,),
    (9, 1): (P.MAIN_BRUSH_LEFT, P.MAIN_BRUSH_TIME_LEFT),
    (10, 1): (P.SIDE_BRUSH_LEFT, P.SIDE_BRUSH_TIME_LEFT),
    (11, 1): (P.FILTER_LEFT, P.FILTER_TIME_LEFT),
    (16, 1): (P.SENSOR_DIRTY_LEFT, P.SENSOR_DIRTY_TIME_LEFT),
    (18, 1): (P.MOP_PAD_LEFT, P.MOP_PAD_TIME_LEFT),
}
_MAP_STREAM = {str(prop.value) for prop in (P.MAP_DATA, P.OLD_MAP_DATA, P.OBJECT_NAME, P.ROBOT_TIME)}
_INT_DIDS = {str(prop.value) for prop in (*_CORE_PROPERTIES, P.CLEANING_MODE, P.SUCTION_LEVEL, P.WATER_VOLUME, P.WATER_TANK)}
_PERCENT_DIDS = {str(prop.value) for prop in (P.BATTERY_LEVEL, P.VOLUME, P.CLEANING_PROGRESS, P.MAIN_BRUSH_LEFT,
                                             P.SIDE_BRUSH_LEFT, P.FILTER_LEFT, P.SENSOR_DIRTY_LEFT, P.MOP_PAD_LEFT)}


_WRITABLE_PROPERTIES = {
    'OFF_PEAK_CHARGING', 'SUCTION_LEVEL', 'WATER_VOLUME', 'RESUME_CLEANING',
    'CARPET_BOOST', 'REMOTE_CONTROL', 'OBSTACLE_AVOIDANCE', 'AI_OBSTACLE_DETECTION',
    'CLEANING_MODE', 'CUSTOMIZED_CLEANING', 'CHILD_LOCK', 'CARPET_SENSITIVITY',
    'TIGHT_MOPPING', 'CARPET_RECOGNITION', 'CARPET_CLEANING', 'AUTO_SWITCH_SETTINGS',
    'DND', 'DND_START', 'DND_END', 'DND_TASK',
    'MULTI_FLOOR_MAP', 'MAP_RECOVERY', 'VOLUME', 'SCHEDULE',
}


_SUPPORTED_ACTIONS = {
    'START', 'PAUSE', 'CHARGE', 'START_CUSTOM',
    'STOP', 'CLEAR_WARNING', 'DELETE_DND_TASK', 'REQUEST_MAP',
    'UPDATE_MAP_DATA', 'BACKUP_MAP', 'LOCATE', 'PLAY_SOUND',
    'DELETE_SCHEDULED_TASK', 'RESET_MAIN_BRUSH', 'RESET_SIDE_BRUSH', 'RESET_FILTER',
    'RESET_SENSOR', 'RESET_MOP_PAD',
}


def _typed(value):
    return type(value) in (bool, int, str) or (type(value) is float and isfinite(value))


def _property_rows(rows, expected=None, *, require_complete=False):
    if type(rows) is not list or len(rows) > (len(_ADDRESS) if expected is None else len(expected)):
        raise LocalBridgeError("Invalid local bridge property reply")
    result = {}
    for row in rows:
        if type(row) is not dict:
            raise LocalBridgeError("Invalid local bridge property reply")
        did, code = row.get("did"), row.get("code")
        if type(did) is not str or type(code) is not int or did in result:
            raise LocalBridgeError("Invalid local bridge property identity")
        address = _ADDRESS.get(did) if expected is None else expected.get(did)
        if (
            address is None
            or any(type(row.get(field)) is not int or row[field] != address[field] for field in ("siid", "piid"))
            or row.keys() not in ({"did", "siid", "piid", "code"}, {"did", "siid", "piid", "code", "value"})
            or ("value" in row and not _typed(row["value"]))
            or (expected is None and (code != 0 or "value" not in row))
        ):
            raise LocalBridgeError("Invalid local bridge property reply")
        result[did] = row
    if require_complete and result.keys() != expected.keys():
        raise LocalBridgeError("Incomplete local bridge property reply")
    return result


@dataclass(frozen=True)
class CandidateSnapshot:
    epoch: str | None
    ready: bool
    sequence: int
    rows: dict[str, dict]
    history: list
    map_reports: dict[int, list[dict]]


class CandidateBridgeClient(LocalBridgeClient):
    """Validate candidate identity independently of the legacy snapshot API."""

    def __init__(self, session, data):
        super().__init__(session, data)
        if not ip_address(self._url.host).is_loopback and not data.get(CONF_BRIDGE_FINGERPRINT, "").strip():
            raise ValueError("Local candidate requires a pinned HTTPS bridge")

    async def async_candidate_state(self) -> CandidateSnapshot:
        data = await self._request("GET", "/v1/candidate/state", 3, max_bytes=_RESPONSE_LIMIT)
        fields = {"version", "epoch", "connected", "ready", "sequence", "properties", "history", "map_reports"}
        if data.keys() != fields:
            raise LocalBridgeError("Invalid candidate state envelope")
        epoch, ready, connected, sequence = data["epoch"], data["ready"], data["connected"], data["sequence"]
        rows = _property_rows(data["properties"])
        history = data["history"]
        if type(history) is not list or len(history) > 100 or any(
            type(event) is not dict or event.keys() != {"did", "siid", "eiid", "arguments"}
            or type(event["siid"]) is not int or event["siid"] != 4
            or type(event["eiid"]) is not int or event["eiid"] not in (1, 5)
            or type(event["arguments"]) is not list or len(event["arguments"]) > 32
            or any(type(arg) is not dict or arg.keys() != {"piid", "value"}
                   or type(arg["piid"]) is not int or not _typed(arg["value"]) for arg in event["arguments"])
            for event in history
        ):
            raise LocalBridgeError("Invalid local history")
        if (
            type(ready) is not bool
            or type(connected) is not bool
            or type(sequence) is not int
            or not 0 <= sequence <= _MAX_SEQUENCE
            or (connected and not _is_hex_id(epoch))
            or (not connected and (epoch is not None or ready))
            or (not ready and (rows or sequence))
            or (rows and not sequence)
        ):
            raise LocalBridgeError("Invalid candidate session state")
        reports = data["map_reports"]
        if type(reports) is not list or len(reports) > 16:
            raise LocalBridgeError("Invalid local map reports")
        map_reports = {}
        previous = 0
        for report in reports:
            if type(report) is not dict or report.keys() != {"sequence", "properties"}:
                raise LocalBridgeError("Invalid local map report")
            number = report["sequence"]
            map_rows = _property_rows(report["properties"])
            if type(number) is not int or not previous < number <= sequence or not map_rows or map_rows.keys() - _MAP_STREAM:
                raise LocalBridgeError("Invalid local map report")
            map_reports[number] = list(map_rows.values())
            previous = number
        return CandidateSnapshot(epoch, ready, sequence, rows, history, map_reports)

    async def async_map_object(self, name: str) -> bytes | None:
        if not isinstance(name, str) or not _OBJECT.fullmatch(name):
            return None
        url = self._url.with_path("/v1/candidate/map-object").with_query({"name": name})
        try:
            async with self._session.get(
                url, headers=self._headers, ssl=self._ssl, allow_redirects=False,
                timeout=ClientTimeout(total=_RPC_TIMEOUT), auto_decompress=False,
            ) as response:
                if response.status != 200 or response.content_type != "application/octet-stream":
                    return None
                try:
                    await response.content.readexactly(2 * 1024 * 1024 + 1)
                except asyncio.IncompleteReadError as exc:
                    return exc.partial or None
        except (ClientError, TimeoutError, ValueError):
            pass
        return None

    async def async_map_download(self, name: str) -> str | None:
        if not isinstance(name, str) or not _OBJECT.fullmatch(name):
            return None
        try:
            data = await self._request(
                "GET", "/v1/candidate/map-download", 3, query={"name": name}
            )
            if data.keys() != {"version", "url"} or type(data["url"]) is not str:
                return None
            url = URL(data["url"])
            ip = IPv4Address(url.host)
            if (
                url.scheme != "http" or url.explicit_port is None or not 1024 <= url.port <= 65535
                or url.user is not None or url.password is not None or url.query_string or url.fragment
                or not ip.is_private or ip.is_unspecified or ip.is_loopback or not _DOWNLOAD.fullmatch(url.raw_path)
            ):
                return None
            return str(url)
        except (LocalBridgeError, TypeError, ValueError):
            return None

    async def async_rpc(self, epoch, method, params):
        request_id = uuid4().hex
        payload = {"version": 1, "request_id": request_id, "epoch": epoch, "method": method, "params": params}
        try:
            reply = await self._request("POST", "/v1/rpc", _RPC_TIMEOUT, payload, max_bytes=_RESPONSE_LIMIT)
        except LocalBridgeError as exc:
            if method != "get_properties":
                raise LocalBridgeError("Local command outcome unknown. Check robot state before retrying.") from exc
            raise
        if reply.get("request_id") != request_id or reply.get("epoch") != epoch:
            raise LocalBridgeError("Local reply identity mismatch. Check robot state before retrying.")
        fields = {"version", "request_id", "epoch"}
        if reply.keys() == fields | {"error"}:
            error = reply["error"]
            if error in ("timeout", "ambiguous", "invalid_reply") and method != "get_properties":
                raise LocalBridgeError("Local command outcome unknown. Check robot state before retrying.")
            if error in ("invalid_request", "unsupported", "busy", "not_ready", "stale_epoch", "replay", "timeout", "unavailable", "invalid_reply"):
                raise LocalBridgeError(f"Local bridge RPC failed ({error})")
            raise LocalBridgeError("Invalid local bridge RPC error")
        if reply.keys() != fields | {"result"}:
            raise LocalBridgeError("Invalid local bridge RPC reply")

        result = reply["result"]
        if method == "action":
            if (
                type(result) is not dict
                or result.keys() != {"did", "siid", "aiid", "code", "out"}
                or type(result["code"]) is not int
                or any(type(result[key]) is not type(params[key]) or result[key] != params[key]
                       for key in ("did", "siid", "aiid"))
                or type(result["out"]) is not list
                or any(type(row) is not dict or row.keys() != {"piid", "value"} or type(row["piid"]) is not int
                       or not _typed(row["value"]) for row in result["out"])
            ):
                raise LocalBridgeError("Invalid local action reply. Check robot state before retrying.")
            return result

        try:
            rows = _property_rows(
                result, {row["did"]: row for row in params}, require_complete=method == "set_properties"
            )
        except LocalBridgeError as exc:
            if method == "set_properties":
                raise LocalBridgeError("Invalid local write reply. Check robot state before retrying.") from exc
            raise
        if method == "get_properties" and any(row["code"] == 0 and "value" not in row for row in rows.values()):
            raise LocalBridgeError("Missing local bridge property value")
        return [rows.get(row["did"], {**row, "code": -1}) for row in params]


@dataclass
class _Command:
    epoch: str
    success: bool = False
    failure: LocalBridgeError | None = None
    readback: set[P] = field(default_factory=set)
    writes: dict[P, object] = field(default_factory=dict)
    movement: bool = False


@dataclass
class _GoTo:
    epoch: str
    deadline: float
    start: float
    x: int
    y: int
    size: int
    map_id: int
    frame: tuple
    restore: dict[P, tuple]
    active_seen: bool = False


class CandidateProtocol:
    """Expose only the synchronous Tasshack MIoT calls used by native entities."""

    cloud = device_cloud = device = None
    dreame_cloud = prefer_cloud = False

    def __init__(self, client, loop) -> None:
        self._client = client
        self._loop = loop
        self._loop_thread = get_ident()
        self._context = local()
        self.epoch = None

    @property
    def connected(self) -> bool:
        return self.epoch is not None

    @property
    def command(self) -> _Command | None:
        return getattr(self._context, "command", None)

    @contextmanager
    def command_scope(self, command):
        self._context.command = command
        try:
            yield
        finally:
            del self._context.command

    @staticmethod
    def supports_entity(domain, description) -> bool:
        action = description.action_key
        if action is not None:
            return action.name in _SUPPORTED_ACTIONS
        prop, key = description.property_key, description.key
        if key == "firmware_version":
            return False  # No robot info RPC in the candidate contract.
        if domain == "button":
            return key in ("start_fast_mapping", "start_mapping", "clear_warning", "backup_saved_map")
        if domain in ("switch", "select", "number", "time"):
            if isinstance(prop, P):
                return prop.name in _WRITABLE_PROPERTIES
            if isinstance(prop, DreameVacuumAutoSwitchProperty):
                return prop.value in {"LessColl", "FillinLight", "MaterialDirectionClean", "SuctionMax"}
            if isinstance(prop, (DreameVacuumAIProperty, DreameVacuumStrAIProperty)):
                return prop.name in {
                    "AI_OBSTACLE_DETECTION", "AI_OBSTACLE_PICTURE", "AI_OBSTACLE_IMAGE_UPLOAD",
                    "AI_PET_DETECTION", "AI_HUMAN_DETECTION", "AI_FURNITURE_DETECTION", "AI_FLUID_DETECTION",
                }
        return True

    def disconnect(self):
        self.epoch = None

    def _sync(self, func, *args):
        if get_ident() == self._loop_thread:
            raise RuntimeError("Local candidate MIoT calls require an executor")
        future = asyncio.run_coroutine_threadsafe(func(*args), self._loop)
        try:
            return future.result(_RPC_TIMEOUT + 1)
        except TimeoutError as exc:
            future.cancel()
            raise LocalBridgeError("Local command outcome unknown. Check robot state before retrying.") from exc

    def _mutate(self, method, params):
        command = self.command
        if command is None or command.epoch != self.epoch:
            raise LocalBridgeError("Local commands require a current Home Assistant action")
        if command.failure is not None:
            raise command.failure
        writes = {}
        if method == "set_properties":
            for item in params:
                address = item["siid"], item["piid"]
                command.movement |= address == (4, 15)
                if (prop := _READBACK.get(address)) is not None:
                    command.readback.add(prop)
                    writes[prop] = item["value"]
        else:
            address = params["siid"], params["aiid"]
            command.movement |= address in _MOVEMENT_ACTIONS
            command.readback.update(_ACTION_READBACK.get(address, ()))
        try:
            result = self._sync(self._client.async_rpc, command.epoch, method, params)
            replies = result if isinstance(result, list) else [result]
            if any(reply["code"] != 0 for reply in replies):
                raise LocalBridgeError("The robot rejected a local command")
        except LocalBridgeError as exc:
            command.failure = exc
            raise
        command.success = True
        command.writes.update(writes)
        return result

    def get_map_object(self, name: str) -> bytes | None:
        if self.connected:
            try:
                return self._sync(self._client.async_map_object, name)
            except LocalBridgeError:
                pass
        return None

    def get_map_download(self, name: str) -> str | None:
        command = self.command
        if command is None or command.epoch != self.epoch:
            return None
        return self._sync(self._client.async_map_download, name)

    def request_map(self, parameters):
        """Request a map frame without entering the user-command scope."""
        epoch = self.epoch
        if epoch is None:
            return None
        try:
            return self._sync(
                self._client.async_rpc, epoch, "action",
                {"did": "6.1", "siid": 6, "aiid": 1, "in": parameters},
            )
        except LocalBridgeError:
            return None

    def set_property(self, siid, piid, value=None, retry_count=0):
        return self.set_properties([(siid, piid, value)])

    def set_properties(self, properties, retry_count=0):
        return self._mutate("set_properties", [
            {"did": f"{siid}.{piid}", "siid": siid, "piid": piid, "value": value}
            for siid, piid, value in properties
        ])

    def set_property_async(self, callback, siid, piid, value=None, retry_count=0):
        result = self.set_property(siid, piid, value)
        if callback:
            callback(result)
        return result

    def action(self, siid, aiid, parameters=None, retry_count=0):
        return self._mutate("action", {"did": f"{siid}.{aiid}", "siid": siid, "aiid": aiid, "in": parameters or []})

    def action_async(self, callback, siid, aiid, parameters=None, retry_count=0):
        result = self.action(siid, aiid, parameters)
        if callback:
            callback(result)
        return result


class LocalCandidateCoordinator(DreameVacuumDataUpdateCoordinator):
    """Feed the native device with bounded reads and current-epoch push values."""

    def __init__(self, hass, entry):
        self.bridge = CandidateBridgeClient(async_get_clientsession(hass), entry.data)
        self.protocol = CandidateProtocol(self.bridge, hass.loop)
        device = DreameVacuumDevice(entry.data[CONF_NAME], None, None, entry.entry_id, local_candidate_protocol=self.protocol)
        super().__init__(hass, entry=entry, device=device)
        self.update_interval = timedelta(seconds=_STATE_INTERVAL)
        self._lock = asyncio.Lock()
        self._closed = False
        self._sequence = -1
        self._push_values = {}
        self._seen = {}
        self._last_read = {}
        self._property_index = 0
        self._history = None
        self._map_sequence = 0
        self._initialized = False
        self._readback = set()
        self._goto = None
        self._goto_update = asyncio.Event()
        entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self._async_stop))

    @callback
    def _async_stop(self, _event=None):
        self._closed = True
        self._cancel_goto()
        self.protocol.disconnect()

    async def _async_update_data(self):
        async with self._lock:
            return await self._async_poll()

    def _reset(self, epoch):
        self._cancel_goto()
        self.protocol.epoch = epoch
        self._sequence = -1
        self._push_values.clear()
        self._seen.clear()
        self._last_read.clear()
        self._property_index = 0
        self._map_sequence = 0
        self._readback.clear()

    async def _async_poll(self):
        if self._closed:
            return self.device
        try:
            snapshot = await self.bridge.async_candidate_state()
        except LocalBridgeError:
            snapshot = CandidateSnapshot(None, False, 0, {}, self._history or [], {})
        if self._closed:
            return self.device
        epoch = snapshot.epoch if snapshot.ready else None
        bootstrap = epoch != self.protocol.epoch
        if bootstrap:
            self._reset(epoch)
        rows = {}
        history = None
        map_reports = []
        if snapshot.history != self._history:
            history = self._history = snapshot.history
        now = self.hass.loop.time()
        if epoch:
            if snapshot.sequence > self._sequence:
                self._sequence = snapshot.sequence
                # The endpoint serves a cache. An unrelated push or the same cached
                # value does not prove a fresh observation for any other property.
                rows.update(
                    (did, row) for did, row in snapshot.rows.items()
                    if did not in _MAP_STREAM and self._push_values.get(did) != row
                )
                self._push_values = snapshot.rows
            for sequence, report in snapshot.map_reports.items():
                if sequence > self._map_sequence:
                    map_reports.append(report)
                    self._map_sequence = sequence
            core = [prop for prop in _CORE_PROPERTIES if now - self._last_read.get(prop, -float("inf")) >= 12]
            limit = _BATCH_SIZE - len(core)
            other = [prop for prop in _OTHER_PROPERTIES if prop in self._readback][:limit]
            if not self._initialized:
                other = list(_OTHER_PROPERTIES)
            elif len(other) < limit and (
                now - self._last_read.get(_OTHER_PROPERTIES[self._property_index], -float("inf")) >= 45
            ):
                end = self._property_index + limit - len(other)
                other.extend(prop for prop in _OTHER_PROPERTIES[self._property_index:end] if prop not in other)
                self._property_index = 0 if end >= len(_OTHER_PROPERTIES) else end
            wanted = [*core, *other]
            for start in range(0, len(wanted), _BATCH_SIZE):
                batch = wanted[start:start + _BATCH_SIZE]
                self._last_read.update((prop, now) for prop in batch)
                params = [{"did": str(prop.value), **_ADDRESS[str(prop.value)]} for prop in batch]
                try:
                    result = await self.bridge.async_rpc(epoch, "get_properties", params)
                except LocalBridgeError:
                    # Keep the last sample until its own freshness deadline.
                    break
                if self._closed:
                    return self.device
                self._readback.difference_update(batch)
                for row in result:
                    if row["code"] == 0 or row["did"] not in rows:
                        rows[row["did"]] = row
            if wanted:
                # An earlier successful batch cannot survive an epoch change during
                # a later batch. The next tick bootstraps the new session.
                try:
                    current = await self.bridge.async_candidate_state()
                except LocalBridgeError:
                    current = CandidateSnapshot(None, False, 0, {}, [], {})
                if self._closed:
                    return self.device
                if not current.ready or current.epoch != epoch:
                    self._reset(None)
                    rows.clear()
                    map_reports.clear()
                    bootstrap = True

        self._initialized = self._initialized or self.protocol.connected
        for did, row in rows.items():
            value = row.get("value")
            if (
                row["code"] == 0 and value is not None
                and (did not in _INT_DIDS or type(value) is int)
                and (did not in _PERCENT_DIDS or (type(value) is int and 0 <= value <= 100))
            ):
                self._seen[did] = now
            else:
                self._seen.pop(did, None)
        stale = {did for did, seen in self._seen.items() if now - seen > (25 if P(int(did)) in _CORE_PROPERTIES else 150)}
        for did in stale:
            del self._seen[did]
        await self.hass.async_add_executor_job(
            self.device.apply_local_candidate_properties,
            [row for did, row in rows.items() if did in self._seen],
            set(self._seen),
            bootstrap,
            history,
            map_reports,
            ZoneInfo(self.hass.config.time_zone),
        )
        if self._goto is not None:
            self._goto_update.set()
        return self.device

    async def async_execute_command(self, func: Callable, *args, **kwargs):
        """Serialize a native user command with observations and require a reply."""
        epoch = self.protocol.epoch
        async with self._lock:
            result, _ = await self._async_run_command_locked(epoch, func, args, kwargs)
            return result

    async def _async_run_command_locked(self, epoch, func, args, kwargs, *, cancel_motion=True):
        if (
            self._closed or self.hass.is_stopping or epoch is None
            or epoch != self.protocol.epoch or not self.device.available
        ):
            raise LocalBridgeError("The local vacuum is unavailable")
        command = _Command(epoch)
        worker = self.hass.async_add_executor_job(self._execute, command, func, args, kwargs)
        try:
            result = await asyncio.shield(worker)
            return result, command
        except asyncio.CancelledError:
            await worker
            raise
        finally:
            for prop in _CORE_PROPERTIES:
                self._last_read.pop(prop, None)
            self._readback.update(command.readback)
            if cancel_motion:
                if command.movement:
                    self._cancel_goto()
                elif self._goto is not None:
                    # A later explicit setting attempt supersedes our restoration,
                    # including a same-value write or an uncertain result.
                    for prop in command.readback:
                        self._goto.restore.pop(prop, None)

    def _execute(self, command, func, args, kwargs):
        maps = self.device._map_manager
        checkpoint = maps.checkpoint()
        with self.protocol.command_scope(command):
            try:
                try:
                    result = func(*args, **kwargs)
                finally:
                    if command.failure is not None:
                        raise command.failure
                if result is False or not command.success:
                    raise LocalBridgeError("The local command did not receive robot acknowledgement")
            except Exception:
                maps.restore_checkpoint(checkpoint)
                raise
            return result

    @callback
    def _cancel_goto(self):
        if self._goto is not None:
            self._goto = None
            self._goto_update.set()

    async def async_go_to(self, x: int, y: int):
        """Start Tasshack's legacy zone travel with an explicit continuation."""
        epoch = self.protocol.epoch
        async with self._lock:
            data = self.device.status.current_map
            prior = {prop: self.device.get_property(prop) for prop in _GOTO_SETTINGS}
            if (
                not data or not data.robot_position or data.empty_map or data.dimensions is None
                or any(value is None for value in prior.values())
                or self.device.get_property(P.BATTERY_LEVEL) is None
                or self.device.get_property(P.TASK_STATUS) is None
            ):
                raise LocalBridgeError("Go to requires a current map, position and cleaning settings")
            if self.device.status.started or self.device.status.paused:
                raise LocalBridgeError("Stop the current task before using Go to")

            frame = (data.frame_id, data.timestamp_ms)
            _, command = await self._async_run_command_locked(epoch, self.device.start_local_go_to, (x, y), {})
            now = self.hass.loop.time()
            travel = _GoTo(
                epoch, now + _GOTO_TIMEOUT, now, x, y, data.dimensions.grid_size * 2, data.map_id, frame,
                {prop: (before, command.writes[prop]) for prop, before in prior.items()
                 if prop in command.writes and before != command.writes[prop]},
            )
            self._goto = travel
            self._goto_update.clear()
            self._entry.async_create_background_task(
                self.hass, self._async_continue_goto(travel), "finish local Dreame Go to",
            )

    async def _async_continue_goto(self, travel):
        try:
            while self._goto is travel and not self._closed:
                try:
                    async with asyncio.timeout_at(travel.deadline):
                        await self._goto_update.wait()
                except TimeoutError:
                    pass
                self._goto_update.clear()
                async with self._lock:
                    if self._goto is not travel or travel.epoch != self.protocol.epoch or self._closed:
                        return
                    timed_out = self.hass.loop.time() >= travel.deadline
                    task = self.device.get_property(P.TASK_STATUS)
                    active = task == DreameVacuumTaskStatus.ZONE_CLEANING and not self.device.status.paused
                    travel.active_seen |= active
                    data = self.device.status.current_map
                    if data and data.map_id != travel.map_id:
                        return
                    reached = (
                        data and data.robot_position and (data.frame_id, data.timestamp_ms) != travel.frame
                        and abs(data.robot_position.x - travel.x) <= travel.size
                        and abs(data.robot_position.y - travel.y) <= travel.size
                    )
                    stop = active and (reached or timed_out)
                    completed = travel.active_seen and task == DreameVacuumTaskStatus.COMPLETED
                    if stop or completed:
                        restore = {
                            prop: before for prop, (before, temporary) in travel.restore.items()
                            if self._seen.get(str(prop.value), 0) >= travel.start
                            and self.device.get_property(prop) == temporary
                        }
                        if stop or restore:
                            await self._async_run_command_locked(
                                travel.epoch, self.device.finish_local_go_to, (stop, restore), {},
                                cancel_motion=False,
                            )
                        return
                    if timed_out or (travel.active_seen and not active):
                        return
        except (DeviceException, LocalBridgeError):
            LOGGER.warning("The local Go to continuation could not be confirmed; check the vacuum state")
        finally:
            if self._goto is travel:
                self._cancel_goto()

    async def async_close(self):
        self._async_stop()
        await self.async_shutdown()
        async with self._lock:
            if self.device is not None:
                await self.hass.async_add_executor_job(self.device.disconnect)
