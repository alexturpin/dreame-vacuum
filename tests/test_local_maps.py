"""Synthetic objects use the documented Dreame frame layout, never a home map."""

import base64
from datetime import datetime
from hashlib import md5, sha256
from io import BytesIO
import json
import struct
from types import SimpleNamespace
import zlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from homeassistant.const import STATE_UNAVAILABLE
from PIL import Image
import pytest

from custom_components.dreame_vacuum.const import DOMAIN
from custom_components.dreame_vacuum.dreame.local_map import DreameLocalMapManager
from custom_components.dreame_vacuum.dreame.map import (
    DreameMapVacuumMapManager,
    DreameVacuumMapDataJsonRenderer,
    DreameVacuumMapRenderer,
)
from custom_components.dreame_vacuum.dreame.types import (
    DreameVacuumDeviceCapability,
    DreameVacuumPropertyMapping,
)
from test_local_bridge import _VACUUM, _load_entry, _refresh, candidate_bridge  # noqa: F401


OBJECT = "local/" + "a" * 32 + "/2"
LIST = "local/" + "b" * 32 + "/1"
HISTORY = "local/" + "c" * 32
RECOVERY_LIST = "local/" + "d" * 32 + "/8"
BACKUP = "local/" + "e" * 32 + "/11"
NOW_MS = 1780000000000
IV = "synthetic-map-iv"
KEY = "synthetic-key"


def frame(
    frame_id=1, kind="I", *, map_id=7, timestamp=NOW_MS, pixels=None, extra=None,
    key=None, robot=(50, 50), metadata=None,
):
    width, height = 8, 8
    pixels = pixels if pixels is not None else bytes([4] * (width * height))
    header = struct.pack("<2hb11h", map_id, frame_id, ord(kind), *robot, 0, 50, 50, 0, 50, width, height, 0, 0)
    if metadata is None:
        metadata = json.dumps({"timestamp_ms": timestamp, "ris": 2, "fsm": 1, **(extra or {})}).encode()
    payload = zlib.compress(header + pixels + metadata)
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


def test_uptime_map_response_replaces_wall_clock_map_and_orders_deltas(monkeypatch):
    monkeypatch.setattr("custom_components.dreame_vacuum.dreame.local_map.time.time", lambda: NOW_MS / 1000)
    manager, protocol = setup_manager()
    old_timestamp = NOW_MS + 500
    manager.ingest_properties([row(1, frame(73, map_id=2, timestamp=old_timestamp))])

    # The running robot returned a legacy inline I-frame and two uptime stamps.
    uptime = 59238825
    protocol.out = [
        {"piid": 1, "value": ""},
        {"piid": 3, "value": ""},
        {"piid": 13, "value": f"0,{frame(54, map_id=3, timestamp=uptime)}"},
        {"piid": 5, "value": 59249228},
    ]
    assert manager.request_new_map()["code"] == 0
    new_map = manager.get_map()
    assert (new_map.map_id, new_map.frame_id) == (3, 54)
    normalized = new_map.timestamp_ms
    assert normalized > old_timestamp

    revision = manager.revision
    protocol.out[-1]["value"] += 1
    manager.request_new_map()
    assert manager.revision == revision
    assert manager.get_map().timestamp_ms == normalized

    manager.ingest_properties([row(1, frame(56, "P", map_id=3, timestamp=uptime + 400, pixels=bytes(64))),
                               row(5, uptime + 12000)])
    assert manager.get_map().frame_id == 54
    protocol.out = [
        {"piid": 1, "value": frame(55, "P", map_id=3, timestamp=uptime + 200, pixels=bytes(64))},
        {"piid": 5, "value": uptime + 13000},
    ]
    manager.request_new_map()
    assert manager.get_map().frame_id == 56
    assert manager.get_map().timestamp_ms == normalized + 400


def test_cached_uptime_replay_keeps_map_timestamps_near_receipt_time(monkeypatch):
    monkeypatch.setattr("custom_components.dreame_vacuum.dreame.local_map.time.time", lambda: NOW_MS / 1000)
    manager, protocol = setup_manager()
    manager.ingest_properties([row(1, frame(71, map_id=2, timestamp=17919527))])
    manager.ingest_properties([row(1, frame(73, map_id=2, timestamp=59084828))])
    assert manager.get_map().timestamp_ms == NOW_MS

    manager.ingest_properties([row(1, frame(275, "P", map_id=3, timestamp=59834734, pixels=bytes(64)))])
    assert manager.get_map().map_id == 2  # The new map needs an I-frame.
    protocol.out = [{"piid": 1, "value": ""}, {"piid": 3, "value": ""}, {"piid": 5, "value": 59923497}]
    manager.update()
    assert protocol.repair_requests == [{"req_type": 1, "frame_type": "I", "force_type": 1}]
    assert manager.get_map().map_id == 2  # The acknowledgement contains no frame.

    protocol.out = [{"piid": 13, "value": f"0,{frame(274, map_id=3, timestamp=59834733)}"}]
    manager.request_new_map()
    assert (manager.get_map().map_id, manager.get_map().frame_id) == (3, 275)
    assert manager.get_map().timestamp_ms == NOW_MS
    manager.ingest_properties([row(1, frame(1, map_id=4, timestamp=NOW_MS + 1))])
    assert manager.get_map().map_id == 4


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_inline_map_action_output_reaches_native_camera(hass, candidate_bridge):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    camera_id = "camera.bridge_test_map"
    camera = hass.data["camera"].get_entity(camera_id)
    assert hass.states.get(camera_id).state == STATE_UNAVAILABLE
    blank = await camera.async_camera_image()

    # Docked smoke evidence established this field layout, with no saved bytes.
    candidate_bridge.rpc_out = [
        {"piid": 1, "value": frame(timestamp=1234)},
        {"piid": 3, "value": ""},
        {"piid": 5, "value": NOW_MS},
    ]
    await hass.services.async_call(DOMAIN, "vacuum_request_map", {"entity_id": _VACUUM}, blocking=True)
    await hass.async_block_till_done()

    actions = [post["params"] for post in candidate_bridge.posts if post["method"] == "action"]
    assert len(actions) == 1
    assert (actions[0]["siid"], actions[0]["aiid"]) == (6, 1)
    assert json.loads(actions[0]["in"][0]["value"]) == {"req_type": 1, "frame_type": "I", "force_type": 1}
    assert coordinator.device.status.current_map.timestamp_ms == NOW_MS
    assert hass.states.get(camera_id).state != STATE_UNAVAILABLE
    image = await camera.async_camera_image()
    assert image and image != blank
    with Image.open(BytesIO(image)) as rendered:
        rendered.verify()
    assert [post["params"] for post in candidate_bridge.posts if post["method"] == "action"] == actions
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_legacy_inline_uptime_output_replaces_camera_map_while_cleaning(hass, candidate_bridge):
    candidate_bridge.reported.update({"11": 2, "5": 19, "0": 1, "27": 1})
    candidate_bridge.sequence = 1
    candidate_bridge.map_reports = [{
        "sequence": 1,
        "properties": [{"did": "76", "siid": 6, "piid": 1, "code": 0,
                        "value": frame(73, map_id=2)}],
    }]
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    assert coordinator.device.status.running
    camera_id = "camera.bridge_test_map"
    assert hass.states.get(camera_id).attributes["map_id"] == 2
    candidate_bridge.rpc_out = [
        {"piid": 1, "value": ""},
        {"piid": 3, "value": ""},
        {"piid": 13, "value": f"0,{frame(54, map_id=3, timestamp=59238825)}"},
        {"piid": 5, "value": 59249228},
    ]
    await hass.services.async_call(DOMAIN, "vacuum_request_map", {"entity_id": _VACUUM}, blocking=True)
    await hass.async_block_till_done()

    assert coordinator.device.status.current_map.frame_id == 54
    camera_attributes = hass.states.get(camera_id).attributes
    assert (camera_attributes["map_id"], camera_attributes["frame_id"]) == (3, 54)
    assert [post["params"]["did"] for post in candidate_bridge.posts if post["method"] == "action"] == ["6.1"]
    assert await hass.config_entries.async_unload(entry.entry_id)


def test_invalid_map_frame_does_not_supersede_valid_frame_ordering():
    manager, _ = setup_manager()
    manager.ingest_properties([row(1, frame())])
    manager.ingest_properties([row(1, frame(kind="W", map_id=10, timestamp=NOW_MS + 10))])
    manager.ingest_properties([row(1, frame(2, timestamp=NOW_MS + 1))])
    assert manager.get_map().frame_id == 2


@pytest.mark.parametrize("invalid_frame", [
    frame(map_id=10, timestamp=NOW_MS + 10, pixels=b""),
    frame(map_id=10, metadata=b'{"timestamp_ms":1780000000010,"fsm":'),
], ids=["truncated-pixels", "malformed-metadata"])
def test_malformed_inline_frame_does_not_discard_good_map(invalid_frame):
    manager, protocol = setup_manager()
    manager.ingest_properties([row(1, frame())])
    previous = manager.get_map()
    protocol.out = [{"piid": 1, "value": invalid_frame}, {"piid": 5, "value": NOW_MS + 10}]
    manager.request_new_map()
    assert manager.get_map() is previous
    manager.ingest_properties([row(1, frame(2, timestamp=NOW_MS + 1))])
    assert manager.get_map().frame_id == 2


def test_failed_native_application_does_not_advance_frame_ordering(monkeypatch):
    manager, _ = setup_manager()
    apply_frame = DreameMapVacuumMapManager._add_map_data

    def apply_or_raise(manager, partial):
        if partial.map_id == 10:
            raise ValueError("synthetic application failure")
        return apply_frame(manager, partial)

    monkeypatch.setattr(DreameMapVacuumMapManager, "_add_map_data", apply_or_raise)
    manager.ingest_properties([row(1, frame())])
    manager.ingest_properties([row(1, frame(map_id=10, timestamp=NOW_MS + 10))])
    manager.ingest_properties([row(1, frame(2, timestamp=NOW_MS + 1))])
    assert manager.get_map().frame_id == 2


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


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_saved_map_camera_restores_without_live_frame(hass, candidate_bridge, monkeypatch):
    from custom_components.dreame_vacuum.local_candidate import CandidateBridgeClient

    saved_map = json.dumps({
        "mapstr": [{"map": frame(map_id=14, extra={"fsm": 0, "ris": -1})}],
        "curr_id": 14,
    }).encode()
    candidate_bridge.reported["83"] = json.dumps({
        "object_name": LIST, "md5": md5(saved_map, usedforsecurity=False).hexdigest(),
    })

    async def get_object(client, name):
        return saved_map if name == LIST else None

    monkeypatch.setattr(CandidateBridgeClient, "async_map_object", get_object)
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")

    assert coordinator.device.status.current_map is None
    assert coordinator.device.status.selected_map.map_id == 14
    assert hass.states.get("camera.bridge_test_map").state == STATE_UNAVAILABLE
    saved_camera = hass.states.get("camera.bridge_test_map_1")
    assert saved_camera is not None
    assert saved_camera.state != STATE_UNAVAILABLE
    assert saved_camera.attributes["map_id"] == 14
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_completed_history_snapshot_updates_native_sensor_without_property_push(hass, candidate_bridge):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    entity_id = "sensor.bridge_test_cleaning_history"
    assert hass.states.get(entity_id).state == STATE_UNAVAILABLE

    start = NOW_MS // 1000
    candidate_bridge.history = [{
        "did": "12345",
        "siid": 4,
        "eiid": 1,
        "arguments": [
            {"piid": 2, "value": 37},
            {"piid": 3, "value": 24},
            {"piid": 8, "value": start},
            {"piid": 9, "value": f"{HISTORY},{KEY}"},
            {"piid": 13, "value": 1},
        ],
    }]
    await _refresh(hass, coordinator)

    state = hass.states.get(entity_id)
    assert datetime.fromisoformat(state.state).timestamp() == start
    record = next(value for value in state.attributes.values() if isinstance(value, dict))
    assert record["cleaning_time"] == "37 min"
    assert record["cleaned_area"] == "24 m²"
    assert record["completed"] is True
    assert {post["method"] for post in candidate_bridge.posts} == {"get_properties"}
    assert await hass.config_entries.async_unload(entry.entry_id)


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


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_relocation_retains_rendered_map_with_explicit_stale_flag(hass, candidate_bridge, monkeypatch):
    candidate_bridge.sequence = 1
    candidate_bridge.map_reports = [{
        "sequence": 1,
        "properties": [{"did": "76", "siid": 6, "piid": 1, "code": 0, "value": frame()}],
    }]
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    camera = hass.data["camera"].get_entity("camera.bridge_test_map")
    image = await camera.async_camera_image()
    state = camera.state
    posts = len(candidate_bridge.posts)
    monkeypatch.setattr(type(coordinator.device.status), "located", property(lambda self: False))
    camera._handle_coordinator_update()
    assert camera.state == state
    assert camera.extra_state_attributes["map_stale"] is True
    assert camera.extra_state_attributes["map_stale_reason"] == "relocating"
    assert await camera.async_camera_image() == image
    assert len(candidate_bridge.posts) == posts
    monkeypatch.setattr(type(coordinator.device.status), "located", property(lambda self: True))
    camera._handle_coordinator_update()
    assert camera.extra_state_attributes["map_stale"] is False
    assert camera.extra_state_attributes["map_stale_reason"] is None
    # Losing connectivity must not be disguised as relocation.
    monkeypatch.setattr(type(coordinator.device.status), "located", property(lambda self: False))
    monkeypatch.setattr(type(coordinator.device), "map_connected", property(lambda self: False))
    camera._handle_coordinator_update()
    assert camera.state == STATE_UNAVAILABLE
    assert camera.extra_state_attributes["map_stale"] is False
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_relocation_without_rendered_map_stays_unavailable(hass, candidate_bridge, monkeypatch):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    camera = hass.data["camera"].get_entity("camera.bridge_test_map")
    monkeypatch.setattr(type(coordinator.device.status), "located", property(lambda self: False))
    camera._handle_coordinator_update()
    assert camera.state == STATE_UNAVAILABLE
    assert camera.extra_state_attributes["map_stale"] is False
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize("failure", [None, "rejected"])
async def test_native_furniture_service_sends_v1_payload_and_restores_on_failure(hass, candidate_bridge, failure):
    from copy import deepcopy
    from homeassistant.exceptions import HomeAssistantError

    candidate_bridge.sequence = 1
    candidate_bridge.map_reports = [{"sequence": 1, "properties": [{"did": "76", "siid": 6, "piid": 1, "code": 0, "value": frame(extra={
        "ai_furniture_user": [[100, 200, 1, 0, 0, 100, 200, 200, 0, 1.0]],
    })}]}]
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    manager = coordinator.device._map_manager
    manager._selected_map_id = manager._map_data.map_id
    original = deepcopy(manager._map_data.furnitures)
    candidate_bridge.rpc_error = failure
    data = {"entity_id": _VACUUM, "furnitures": [[0, 1, 0, 200, 200, 300, 400, 90, 1.0, 0, 1]]}
    if failure:
        with pytest.raises(HomeAssistantError):
            await hass.services.async_call(DOMAIN, "vacuum_set_furniture", data, blocking=True)
        assert manager._map_data.furnitures == original
    else:
        await hass.services.async_call(DOMAIN, "vacuum_set_furniture", data, blocking=True)
    edits = [p['params'] for p in candidate_bridge.posts if p['method'] == 'action' and p['params']['aiid'] == 2]
    assert len(edits) == 1
    assert edits[0]['siid'] == 6
    assert json.loads(edits[0]['in'][0]['value']) == {
        'mapfurniture': [[300, 400, 1, 0, 200, 300, 200, 200, 90, 1.0]], 'mfet': [1],
    }
    assert await hass.config_entries.async_unload(entry.entry_id)
