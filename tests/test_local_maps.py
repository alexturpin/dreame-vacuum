"""Synthetic objects use the documented Dreame frame layout, never a home map."""

import base64
from hashlib import md5, sha256
from io import BytesIO
import json
import struct
from types import SimpleNamespace
import zlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from PIL import Image

from custom_components.dreame_vacuum.dreame.local_map import DreameLocalMapManager
from custom_components.dreame_vacuum.dreame.map import DreameVacuumMapDataJsonRenderer, DreameVacuumMapRenderer
from custom_components.dreame_vacuum.dreame.types import (
    DreameVacuumDeviceCapability,
    DreameVacuumPropertyMapping,
)


OBJECT = "local/" + "a" * 32 + "/2"
LIST = "local/" + "b" * 32 + "/1"
HISTORY = "local/" + "c" * 32
RECOVERY_LIST = "local/" + "d" * 32 + "/8"
BACKUP = "local/" + "e" * 32 + "/11"
NOW_MS = 1780000000000
IV = "synthetic-map-iv"
KEY = "synthetic-key"


def frame(frame_id=1, kind="I", *, map_id=7, timestamp=NOW_MS, pixels=None, extra=None, key=None, robot=(50, 50)):
    width, height = 8, 8
    pixels = pixels if pixels is not None else bytes([4] * (width * height))
    header = struct.pack("<2hb11h", map_id, frame_id, ord(kind), *robot, 0, 50, 50, 0, 50, width, height, 0, 0)
    payload = zlib.compress(
        header + pixels + json.dumps({"timestamp_ms": timestamp, "ris": 2, "fsm": 1, **(extra or {})}).encode()
    )
    if key:
        padder = PKCS7(128).padder()
        encryptor = Cipher(algorithms.AES(sha256(key.encode()).hexdigest()[:32].encode()), modes.CBC(IV.encode())).encryptor()
        payload = encryptor.update(padder.update(payload) + padder.finalize()) + encryptor.finalize()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


class OfflineBridge:
    dreame_cloud = False

    def __init__(self):
        self.objects = {}
        self.requests = []
        self.repair_requests = []
        self.fetched = []
        self.download_url = None
        self.out = []

    def get_map_object(self, name):
        self.fetched.append(name)
        return self.objects.get(name)

    def get_map_download(self, name):
        assert name == BACKUP
        return self.download_url

    def action(self, siid, aiid, payload, retry_count):
        self.requests.append((siid, aiid, payload, retry_count))
        return {"code": 0, "out": self.out}

    def request_map(self, payload):
        self.repair_requests.append(json.loads(payload[0]["value"]))
        return {"code": 0, "out": self.out}


def setup_manager():
    protocol = OfflineBridge()
    manager = DreameLocalMapManager(protocol)
    capability = DreameVacuumDeviceCapability(SimpleNamespace(status=SimpleNamespace(current_segments=None)))
    capability.key = IV
    manager.set_capability(capability)
    return manager, protocol


def row(piid, value):
    return {"siid": 6, "piid": piid, "value": value}


def test_live_map_from_encrypted_upload_reaches_native_renderer():
    manager, protocol = setup_manager()
    protocol.objects[OBJECT] = frame(key=KEY).encode()
    manager.ingest_properties([row(3, f"{OBJECT},{KEY}")])
    result = manager.get_map()
    assert result.map_id == 7
    assert not result.empty_map
    assert result.segments
    renderer = DreameVacuumMapDataJsonRenderer()
    assert renderer.render_map(result) != renderer.default_map_image
    assert protocol.requests == []


def test_explicit_request_uses_native_action_and_ingests_output():
    manager, protocol = setup_manager()
    manager.ingest_properties([])
    assert manager.ready
    assert manager.get_map() is None
    assert protocol.requests == []
    protocol.out = [{"piid": 13, "value": f"0,{frame()}"}]
    assert manager.request_new_map()["code"] == 0
    siid, aiid, payload, retry_count = protocol.requests.pop()
    assert (siid, aiid, retry_count) == (6, 1, 0)
    assert payload[0]["piid"] == 2
    assert json.loads(payload[0]["value"]) == {"req_type": 1, "frame_type": "I", "force_type": 1}
    assert manager.get_map().frame_id == 1


def test_out_of_order_delta_waits_passively_and_reset_clears_floor_plan():
    manager, protocol = setup_manager()
    manager.ingest_properties([row(1, frame(2, "P", timestamp=NOW_MS + 1, pixels=bytes(64)))])
    assert manager.get_map() is None
    manager.ingest_properties([row(1, frame())])
    assert manager.get_map().frame_id == 2
    manager.reset_session()
    assert manager.get_map() is None
    assert not manager.ready
    assert protocol.requests == []


def test_poll_requests_bounded_gap_repair():
    manager, protocol = setup_manager()
    manager.update()
    assert protocol.repair_requests == []
    manager.ingest_properties([row(1, frame())])
    manager.ingest_properties([row(1, frame(3, "P", timestamp=NOW_MS + 2, pixels=bytes(64)))])
    protocol.out = [{"piid": 1, "value": frame(2, "P", timestamp=NOW_MS + 1, pixels=bytes(64))}]
    manager.update()
    manager.update()
    assert protocol.repair_requests == [{"req_type": 1, "frame_type": "P", "map_id": 7, "frame_id": 2}]
    assert manager.get_map().frame_id == 3


def test_map_list_and_history_reuse_tasshack_objects():
    manager, protocol = setup_manager()
    protocol.objects[LIST] = json.dumps({
        "mapstr": [{"map": frame(map_id=14, extra={"fsm": 0, "ris": -1}), "name": "Synthetic floor"}],
        "curr_id": 14,
    }).encode()
    protocol.objects[HISTORY] = frame(key=KEY).encode()
    descriptor = json.dumps({"object_name": LIST, "md5": md5(protocol.objects[LIST], usedforsecurity=False).hexdigest()})
    manager.ingest_properties([row(1, frame()), row(8, descriptor)])
    assert manager.get_map(1).custom_name == "Synthetic floor"
    assert protocol.fetched.count(LIST) == 1

    events = [
        {"siid": 4, "eiid": 1, "arguments": [{"piid": 8, "value": 1780000000}, {"piid": 9, "value": f"{HISTORY},{KEY}"}]},
        {"siid": 4, "eiid": 5, "arguments": [{"piid": 8, "value": 1770000000}]},
        {"siid": 4, "eiid": 2, "arguments": [{"piid": 8, "value": 1790000000}]},
    ]
    history = manager.parse_history(events, DreameVacuumPropertyMapping)
    assert len(history) == 2
    assert history[0].date > history[1].date
    assert history[0].object_name == HISTORY
    assert manager.get_history_map(history[0].object_name, history[0].key).history_map


def test_saved_backup_camera_read_and_explicit_restore_url_are_separate():
    manager, protocol = setup_manager()
    protocol.objects[LIST] = json.dumps({
        "mapstr": [{"map": frame(map_id=14, extra={"fsm": 0, "ris": -1})}],
        "curr_id": 14,
    }).encode()
    protocol.objects[RECOVERY_LIST] = json.dumps([
        {"id": 14, "info": [{"time": 1780000000, "objname": BACKUP, "first": 1}]},
    ]).encode()
    protocol.objects[BACKUP] = b"BZh synthetic archived bytes"
    manager.ingest_properties([
        row(1, frame()), row(8, json.dumps({"object_name": LIST})),
        row(9, json.dumps({"object_name": RECOVERY_LIST})),
    ])
    assert manager.get_recovery_map_file(14, 1) == (protocol.objects[BACKUP], None, BACKUP)
    protocol.download_url = "http://127.0.0.1:12345/v1/robot/map-download/" + "f" * 32
    assert manager.get_recovery_map_file(14, 1) == (protocol.objects[BACKUP], protocol.download_url, BACKUP)


def test_existing_obstacle_object_reaches_native_image_renderer():
    manager, protocol = setup_manager()
    image = BytesIO()
    Image.new("RGB", (32, 32), (240, 240, 240)).save(image, format="JPEG")
    padder = PKCS7(128).padder()
    encryptor = Cipher(algorithms.AES(md5(KEY.encode(), usedforsecurity=False).digest()), modes.ECB()).encryptor()
    filename = "/data/record/17.jpg"
    object_id = 1745000000
    name = "local/" + sha256(f"{filename}-{object_id}".encode()).hexdigest()[:32]
    protocol.objects[name] = encryptor.update(padder.update(image.getvalue()) + padder.finalize()) + encryptor.finalize()
    metadata = {"ai_obstacle": [[100, 100, 133, 0.9, object_id, filename, KEY, 0.3, 0.3, 0.4, 0.4, 2, 0]]}
    manager.ingest_properties([row(1, frame(extra=metadata))])
    map_data = manager.get_map()
    decoded, obstacle = manager.get_obstacle_image(map_data, 1)
    assert decoded == image.getvalue()
    rendered = DreameVacuumMapRenderer().render_obstacle_image(decoded, obstacle, False)
    assert Image.open(BytesIO(rendered)).size == (32, 32)
    assert rendered != decoded

    obstacle.key = "other-key"
    assert manager.get_obstacle_image(map_data, 1) == (None, None)
    protocol.objects[name] = decoded[:500] + decoded[-2:]
    assert manager.get_obstacle_image(map_data, 1) == (None, None)
    protocol.objects[name] = decoded
    assert manager.get_obstacle_image(map_data, 1) == (decoded, obstacle)
    fetched_before = list(protocol.fetched)
    obstacle.file_name = "https://invalid.test/obstacle.jpg"
    assert manager.get_obstacle_image(map_data, 1) == (None, None)
    assert protocol.fetched == fetched_before


def test_unknown_reference_and_bad_map_do_not_replace_good_map():
    manager, protocol = setup_manager()
    manager.ingest_properties([row(1, frame())])
    previous = manager.get_map()
    manager.ingest_properties([row(1, "!"), row(3, "http://invalid.test/not-an-object")])
    assert manager.get_map() is previous
    assert protocol.fetched == []


def test_checkpoint_does_not_restore_a_stale_map():
    manager, _ = setup_manager()
    manager.ingest_properties([row(1, frame())])
    checkpoint = manager.checkpoint()
    manager.get_map().rotation = 90
    manager.restore_checkpoint(checkpoint)
    assert manager.get_map().rotation == 0
    checkpoint = manager.checkpoint()
    manager.ingest_properties([row(1, frame(2, timestamp=NOW_MS + 1))])
    manager.restore_checkpoint(checkpoint)
    assert manager.get_map().frame_id == 2
