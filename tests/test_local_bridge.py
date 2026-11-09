"""Exercise the real Home Assistant entry, entity, and service lifecycle."""

from __future__ import annotations

from asyncio import Event, wait_for
from dataclasses import dataclass, field
from datetime import timedelta
import json
import socket
from unittest.mock import patch

from aiohttp import web
from probatio import Invalid
import pytest
import pytest_socket

from homeassistant.config_entries import ConfigEntryState, SOURCE_RECONFIGURE, SOURCE_USER
from homeassistant.const import CONF_NAME, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import Context
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import HomeAssistantError, Unauthorized
from homeassistant.helpers import device_registry as dr
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.dreame_vacuum.const import CONF_TYPE, DOMAIN
from custom_components.dreame_vacuum.diagnostics import async_get_config_entry_diagnostics
from custom_components.dreame_vacuum.dreame import (
    DreameVacuumAction,
    DreameVacuumProperty,
    InvalidActionException,
)
from custom_components.dreame_vacuum.local_bridge import (
    ACCOUNT_TYPE_LOCAL_BRIDGE,
    CONF_BRIDGE_TOKEN,
    CONF_BRIDGE_URL,
    CONF_ENTRY_ID,
    CONF_PROPERTY,
    SERVICE_READ_LOCAL_BATTERY,
    SERVICE_READ_LOCAL_PROPERTY,
    LocalBridgeError,
    validate_bridge_url,
)

_TOKEN = "A" * 32
_EPOCH = "a" * 32
_NEW_EPOCH = "b" * 32
_VACUUM = "vacuum.bridge_test"
_BATTERY = "sensor.bridge_test_battery_level"

pytestmark = [pytest.mark.usefixtures("enable_custom_integrations"), pytest.mark.asyncio]


def _property(did, siid, value, *, piid=1):
    return {"did": did, "siid": siid, "piid": piid, "code": 0, "value": value}


def _state(*properties, epoch=_EPOCH, sequence=0):
    return {
        "version": 1,
        "epoch": epoch,
        "connected": epoch is not None,
        "ready": bool(properties),
        "sequence": sequence,
        "properties": list(properties),
    }


@dataclass
class FakeBridge:
    """HTTP fixture with independently controlled reports and RPC replies."""

    state: dict = field(default_factory=lambda: _state(epoch=None))
    posts: list[dict] = field(default_factory=list)
    gets: int = 0
    token: str = _TOKEN
    url: str = ""
    status: int = 200
    raw_state: str | None = None
    read_reply: dict = field(default_factory=dict)
    read_error: str | None = None
    read_status: int = 200
    read_started: Event = field(default_factory=Event)
    read_release: Event | None = None
    read_state: dict | None = None

    async def get_state(self, request):
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            raise web.HTTPUnauthorized()
        self.gets += 1
        if self.raw_state is not None:
            return web.Response(text=self.raw_state, content_type="application/json", status=self.status)
        return web.json_response(self.state, status=self.status)

    async def post_read(self, request):
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            raise web.HTTPUnauthorized()
        payload = await request.json()
        self.posts.append(payload)
        self.read_started.set()
        if self.read_release is not None:
            await self.read_release.wait()
        if self.read_state is not None:
            self.state = self.read_state
        reply = {
            "version": 1,
            "request_id": payload["request_id"],
            "epoch": payload["epoch"],
            "result": [_property("2", 3, 67)],
        }
        if self.read_error is not None:
            del reply["result"]
            reply["error"] = self.read_error
        return web.json_response(reply | self.read_reply, status=self.read_status)


@pytest.fixture
def bridge_factory(aiohttp_server, socket_enabled):
    async def create():
        bridge = FakeBridge()
        app = web.Application()
        app.router.add_get("/v1/state", bridge.get_state)
        app.router.add_post("/v1/read", bridge.post_read)
        server = await aiohttp_server(app)
        bridge.url = str(server.make_url("/").origin())
        return bridge

    return create


@pytest.fixture
async def bridge(bridge_factory):
    return await bridge_factory()


@pytest.fixture(autouse=True)
def forbid_upstream_transports():
    with (
        patch("custom_components.dreame_vacuum.dreame.device.DreameVacuumProtocol", side_effect=AssertionError("device transport")),
        patch("custom_components.dreame_vacuum.config_flow.DreameVacuumProtocol", side_effect=AssertionError("config transport")),
        patch("custom_components.dreame_vacuum.dreame.device.DreameMapVacuumMapManager", side_effect=AssertionError("map manager")),
        patch("custom_components.dreame_vacuum.frontend.setup", side_effect=AssertionError("frontend setup")),
        patch("custom_components.dreame_vacuum.frontend.remove", side_effect=AssertionError("frontend remove")),
    ):
        yield


async def _start_flow(hass):
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    assert result["step_id"] == "user"
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_TYPE: ACCOUNT_TYPE_LOCAL_BRIDGE}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "local_bridge"
    return result["flow_id"]


async def _configure(hass, bridge, flow_id, **changes):
    return await hass.config_entries.flow.async_configure(
        flow_id,
        {CONF_NAME: "Bridge Test", CONF_BRIDGE_URL: bridge.url, CONF_BRIDGE_TOKEN: bridge.token} | changes,
    )


async def _load_entry(hass, bridge):
    result = await _configure(hass, bridge, await _start_flow(hass))
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    return entry, hass.data[DOMAIN][entry.entry_id]


async def _refresh(hass, coordinator):
    await coordinator.async_refresh()
    await hass.async_block_till_done()


async def _read_trial(hass, entry, user, property_name, *, response=True):
    return await hass.services.async_call(
        DOMAIN,
        SERVICE_READ_LOCAL_PROPERTY,
        {CONF_ENTRY_ID: entry.entry_id, CONF_PROPERTY: property_name},
        blocking=True,
        return_response=response,
        context=Context(user_id=user.id),
    )


async def test_loopback_fixture_blocks_external_sockets_and_ignores_env_proxy(hass, bridge, monkeypatch):
    # Confirm the real connection cannot run even if the blocking assertion fails.
    assert socket.socket.connect is not pytest_socket._true_connect
    with (
        socket.socket() as client,
        patch.object(pytest_socket, "_true_connect", side_effect=AssertionError("real network call")),
        pytest.raises(pytest_socket.SocketConnectBlockedError),
    ):
        client.connect(("192.0.2.1", 443))

    monkeypatch.setenv("HTTP_PROXY", "http://192.0.2.1:8443")
    monkeypatch.setenv("NO_PROXY", "")
    await _load_entry(hass, bridge)
    assert bridge.gets == 2
    assert not bridge.posts


async def test_configure_and_passive_lifecycle(hass, bridge):
    flow_id = await _start_flow(hass)
    invalid = await _configure(hass, bridge, flow_id, bridge_url="https://8.8.8.8:1234")
    assert invalid["errors"] == {"base": "invalid_bridge_settings"}
    bad_auth = await _configure(hass, bridge, flow_id, bridge_token="B" * 32)
    assert bad_auth["errors"] == {"base": "cannot_connect_bridge"}
    assert bridge.gets == 0

    result = await _configure(hass, bridge, flow_id)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    coordinator = hass.data[DOMAIN][entry.entry_id]
    assert coordinator.device._protocol is None
    assert coordinator.device._map_manager is None
    assert set(hass.states.async_entity_ids()) == {_VACUUM, _BATTERY}
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE
    assert dr.async_get(hass).async_get_device_by_identifier((DOMAIN, entry.entry_id), entry.entry_id)
    assert bridge.gets == 2  # Flow validation and initial coordinator refresh.

    bridge.state = _state(_property("0", 2, 2), _property("2", 3, 0), sequence=1)
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=12))
    await hass.async_block_till_done(wait_background_tasks=True)
    assert hass.states.get(_VACUUM).state == "idle"
    assert hass.states.get(_VACUUM).attributes["supported_features"] == 0
    assert hass.states.get(_BATTERY).state == "0"
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == "0"  # Current epoch and unchanged sequence.
    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    assert diagnostics["properties"] == {"BATTERY_LEVEL": 0, "STATE": 2}
    assert bridge.token not in json.dumps(diagnostics)
    assert bridge.url not in json.dumps(diagnostics)

    bridge.status = 503
    await _refresh(hass, coordinator)
    assert not coordinator.device.data
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE
    bridge.status = 200
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE  # No stale revival.
    bridge.state = _state(_property("2", 3, 4), sequence=2)
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == STATE_UNKNOWN
    assert hass.states.get(_BATTERY).state == "4"

    bridge.state = _state(epoch=None)
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
    assert not coordinator.device.data
    bridge.state = _state(epoch=_NEW_EPOCH)
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE
    bridge.state = _state(_property("0", 2, 0), epoch=_NEW_EPOCH, sequence=1)
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == "docked"
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE
    bridge.state = _state(_property("2", 3, 99), sequence=3)
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE  # Retired epoch.
    assert not coordinator.device.data
    assert not bridge.posts

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.entry_id not in hass.data[DOMAIN]
    assert not hass.services.has_service(DOMAIN, SERVICE_READ_LOCAL_BATTERY)
    assert not hass.services.has_service(DOMAIN, SERVICE_READ_LOCAL_PROPERTY)
    assert not coordinator.device.data
    for entity_id in (_VACUUM, _BATTERY):
        state = hass.states.get(entity_id)
        assert state.state == STATE_UNAVAILABLE and state.attributes["restored"]
    gets_at_unload = bridge.gets
    await _refresh(hass, coordinator)
    assert bridge.gets == gets_at_unload
    assert not coordinator.device.data
    assert not bridge.posts
    await hass.config_entries.async_remove(entry.entry_id)
    await hass.async_block_till_done()
    assert hass.states.get(_VACUUM) is None
    assert hass.states.get(_BATTERY) is None


async def test_explicit_battery_read_and_mutation_refusal(hass, bridge, hass_admin_user, hass_read_only_user):
    bridge.state = _state(_property("0", 2, 2), _property("2", 3, 0), sequence=1)
    entry, coordinator = await _load_entry(hass, bridge)
    target = {"entity_id": _VACUUM}
    for service in ("start", "pause", "stop", "return_to_base", "locate", "clean_spot"):
        with pytest.raises(HomeAssistantError):
            await hass.services.async_call("vacuum", service, target, blocking=True)
    for service in ("vacuum_request_map", "vacuum_set_property", "vacuum_call_action"):
        assert not hass.services.has_service(DOMAIN, service)

    device = coordinator.device
    for method, args in (
        (device.set_property, (DreameVacuumProperty.BATTERY_LEVEL, 100)),
        (device.set_properties, ({DreameVacuumProperty.BATTERY_LEVEL: 100},)),
        (device.call_action, (DreameVacuumAction.START,)),
        (device.start, ()),
        (device.send_command, ("get_properties", [])),
    ):
        with pytest.raises(InvalidActionException, match="read-only"):
            method(*args)
    device.update(True)
    device.schedule_update(0, True)
    assert device._update_timer is None
    assert device.status.battery_level == 0
    assert not bridge.posts

    async def read(user, *, response=True):
        return await hass.services.async_call(
            DOMAIN,
            SERVICE_READ_LOCAL_BATTERY,
            {CONF_ENTRY_ID: entry.entry_id},
            blocking=True,
            return_response=response,
            context=Context(user_id=user.id),
        )

    with pytest.raises(Unauthorized):
        await read(hass_read_only_user)
    with pytest.raises(HomeAssistantError):
        await read(hass_admin_user, response=False)
    assert not bridge.posts

    assert await read(hass_admin_user) == {"battery_level": 67}
    assert len(bridge.posts) == 1
    request = bridge.posts[0]
    assert len(request["request_id"]) == 32
    assert request == {
        "version": 1,
        "request_id": request["request_id"],
        "epoch": _EPOCH,
        "method": "get_properties",
        "params": [{"did": "2", "siid": 3, "piid": 1}],
        "timeout": 5,
    }
    assert hass.states.get(_BATTERY).state == "0"

    bridge.read_reply = {"request_id": "wrong"}
    with pytest.raises(LocalBridgeError, match="did not match"):
        await read(hass_admin_user)

    bridge.read_reply = {}
    for error, message in (
        ("property_error", "battery property error"),
        ("timeout", "battery request timed out"),
        ("not_armed", "no one-shot battery authorization"),
    ):
        bridge.read_error = error
        with pytest.raises(LocalBridgeError, match=message):
            await read(hass_admin_user)
    bridge.read_error = None
    bridge.read_state = _state(_property("0", 2, 13), epoch=_NEW_EPOCH, sequence=1)
    with pytest.raises(LocalBridgeError, match="session changed"):
        await read(hass_admin_user)
    assert len(bridge.posts) == 6
    assert hass.states.get(_VACUUM).state == "docked"
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE

    bridge.state = _state(epoch=None)
    with pytest.raises(LocalBridgeError, match="no fresh telemetry"):
        await read(hass_admin_user)
    assert len(bridge.posts) == 6


async def test_read_is_never_queued_and_cannot_resume_after_unload(hass, bridge, hass_admin_user):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, coordinator = await _load_entry(hass, bridge)
    bridge.read_release = Event()

    async def read():
        return await hass.services.async_call(
            DOMAIN,
            SERVICE_READ_LOCAL_BATTERY,
            {CONF_ENTRY_ID: entry.entry_id},
            blocking=True,
            return_response=True,
            context=Context(user_id=hass_admin_user.id),
        )

    pending = hass.async_create_task(read())
    try:
        await wait_for(bridge.read_started.wait(), timeout=2)
        assert len(bridge.posts) == 1
        with pytest.raises(LocalBridgeError, match="is busy"):
            await wait_for(read(), timeout=2)
        with pytest.raises(LocalBridgeError, match="is busy"):
            await wait_for(_read_trial(hass, entry, hass_admin_user, "state"), timeout=2)
        assert await hass.config_entries.async_unload(entry.entry_id)
        assert coordinator.device.disconnected
        gets_at_unload = bridge.gets
    finally:
        bridge.read_release.set()
    with pytest.raises(LocalBridgeError, match="session changed"):
        await pending
    assert bridge.gets == gets_at_unload
    assert len(bridge.posts) == 1


async def test_local_read_service_tracks_loaded_entries(hass, bridge_factory):
    first_bridge = await bridge_factory()
    second_bridge = await bridge_factory()
    first, _ = await _load_entry(hass, first_bridge)
    second, _ = await _load_entry(hass, second_bridge)

    duplicate = await _configure(hass, first_bridge, await _start_flow(hass))
    assert duplicate["type"] is FlowResultType.ABORT
    assert duplicate["reason"] == "already_configured"
    assert await hass.config_entries.async_unload(first.entry_id)
    services = {
        SERVICE_READ_LOCAL_BATTERY: {},
        SERVICE_READ_LOCAL_PROPERTY: {CONF_PROPERTY: "state"},
    }
    for service, data in services.items():
        assert hass.services.has_service(DOMAIN, service)
        with pytest.raises(LocalBridgeError, match="Select a loaded"):
            await hass.services.async_call(
                DOMAIN,
                service,
                {CONF_ENTRY_ID: first.entry_id} | data,
                blocking=True,
                return_response=True,
            )
    assert await hass.config_entries.async_unload(second.entry_id)
    assert all(not hass.services.has_service(DOMAIN, service) for service in services)
    assert not first_bridge.posts and not second_bridge.posts


async def test_property_action_authorization_schema_and_controls(hass, bridge, hass_admin_user, hass_read_only_user):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, _ = await _load_entry(hass, bridge)
    assert hass.services.has_service(DOMAIN, SERVICE_READ_LOCAL_PROPERTY)
    assert hass.states.get(_VACUUM).attributes["supported_features"] == 0

    with pytest.raises(Unauthorized):
        await _read_trial(hass, entry, hass_read_only_user, "state")
    with pytest.raises(HomeAssistantError):
        await _read_trial(hass, entry, hass_admin_user, "state", response=False)

    for invalid in (
        {},
        {CONF_PROPERTY: "battery_level"},
        {CONF_PROPERTY: ["state", "status"]},
        {CONF_PROPERTY: {"did": "0", "siid": 2, "piid": 1}},
        {CONF_PROPERTY: "STATE"},
        {CONF_PROPERTY: "state", "continuation": "a" * 32},
    ):
        with pytest.raises(Invalid):
            await hass.services.async_call(
                DOMAIN,
                SERVICE_READ_LOCAL_PROPERTY,
                {CONF_ENTRY_ID: entry.entry_id} | invalid,
                blocking=True,
                return_response=True,
                context=Context(user_id=hass_admin_user.id),
            )
    assert not bridge.posts


async def test_five_explicit_reads_return_raw_values_without_updating_cache(hass, bridge, hass_admin_user):
    bridge.state = _state(_property("0", 2, 2), _property("2", 3, 45), _property("5", 4, 1), sequence=1)
    entry, coordinator = await _load_entry(hass, bridge)
    assert not bridge.posts
    continuation = None
    for step, (name, did, siid, piid, value) in enumerate(
        (
            ("state", "0", 2, 1, (1 << 31) - 1),
            ("status", "5", 4, 1, -(1 << 31)),
            ("charging_status", "3", 3, 2, 0),
            ("task_status", "11", 4, 7, 99),
            ("error", "1", 2, 2, -7),
        ),
        start=1,
    ):
        next_receipt = str(step) * 32 if step < 5 else None
        bridge.read_reply = {
            "result": [_property(did, siid, value, piid=piid)],
            "continuation": next_receipt,
        }
        identity = {"did": did, "siid": siid, "piid": piid}
        assert await _read_trial(hass, entry, hass_admin_user, name) == {
            "property": name, **identity, "value": value
        }
        assert len(bridge.posts) == step
        request = bridge.posts[-1]
        assert len(request["request_id"]) == 32
        assert request == {
            "version": 1,
            "request_id": request["request_id"],
            "epoch": _EPOCH,
            "method": "get_properties",
            "params": [identity],
            "timeout": 5,
            "continuation": continuation,
        }
        continuation = next_receipt
    assert len({request["request_id"] for request in bridge.posts}) == 5
    with pytest.raises(LocalBridgeError, match="trial ended"):
        await _read_trial(hass, entry, hass_admin_user, "state")
    await _refresh(hass, coordinator)
    assert len(bridge.posts) == 5
    assert hass.states.get(_VACUUM).state == "idle"
    assert hass.states.get(_BATTERY).state == "45"
    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    assert diagnostics["properties"] == {"BATTERY_LEVEL": 45, "STATE": 2, "STATUS": 1}
    assert "continuation" not in json.dumps(diagnostics)


@pytest.mark.parametrize(
    "reply",
    [
        {"request_id": "wrong"},
        {"epoch": _NEW_EPOCH},
        {"result": [_property("5", 4, 1)]},  # Valid but different property.
        {"result": [_property("0", 2, 1, piid=2)]},
        {"result": [_property(0, 2, 1)]},
        {"result": [_property("0", 2, True)]},
        {"result": [_property("0", 2, 1 << 31)]},
        {"result": [_property("0", 2, -(1 << 31) - 1)]},
        {"result": [_property("0", 2, 1), _property("0", 2, 2)]},
        {"result": [_property("0", 2, 1) | {"code": 1}]},
        {"continuation": "Z" * 32},
        {"continuation": None},
    ],
)
async def test_reject_uncorrelated_or_malformed_property_reply(hass, bridge, hass_admin_user, reply):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, _ = await _load_entry(hass, bridge)
    bridge.read_reply = {
        "result": [_property("0", 2, 1)],
        "continuation": "1" * 32,
    } | reply
    with pytest.raises(LocalBridgeError):
        await _read_trial(hass, entry, hass_admin_user, "state")
    with pytest.raises(LocalBridgeError, match="trial ended"):
        await _read_trial(hass, entry, hass_admin_user, "status")
    assert len(bridge.posts) == 1


@pytest.mark.parametrize("read_status,read_error", [(503, None), (200, "property_error")])
async def test_failed_middle_read_seals_epoch(hass, bridge, hass_admin_user, read_status, read_error):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, _ = await _load_entry(hass, bridge)
    bridge.read_reply = {"result": [_property("0", 2, 1)], "continuation": "1" * 32}
    await _read_trial(hass, entry, hass_admin_user, "state")

    bridge.read_status, bridge.read_error, bridge.read_reply = read_status, read_error, {}
    with pytest.raises(LocalBridgeError):
        await _read_trial(hass, entry, hass_admin_user, "status")
    assert bridge.posts[-1]["continuation"] == "1" * 32
    with pytest.raises(LocalBridgeError, match="trial ended"):
        await _read_trial(hass, entry, hass_admin_user, "charging_status")
    assert len(bridge.posts) == 2

    bridge.state = _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
    bridge.read_status, bridge.read_error = 200, None
    bridge.read_reply = {"result": [_property("0", 2, 1)], "continuation": "2" * 32}
    await _read_trial(hass, entry, hass_admin_user, "state")
    assert bridge.posts[-1]["epoch"] == _NEW_EPOCH
    assert bridge.posts[-1]["continuation"] is None


async def test_property_receipt_is_discarded_if_postflight_changes_epoch(hass, bridge, hass_admin_user):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, _ = await _load_entry(hass, bridge)
    bridge.read_reply = {"result": [_property("0", 2, 1)], "continuation": "1" * 32}
    bridge.read_state = _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
    with pytest.raises(LocalBridgeError, match="session changed"):
        await _read_trial(hass, entry, hass_admin_user, "state")
    await _read_trial(hass, entry, hass_admin_user, "state")
    assert [request["epoch"] for request in bridge.posts] == [_EPOCH, _NEW_EPOCH]
    assert [request["continuation"] for request in bridge.posts] == [None, None]


async def test_overlapping_property_read_does_not_queue_or_disrupt_trial(hass, bridge, hass_admin_user):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, coordinator = await _load_entry(hass, bridge)
    bridge.read_reply = {"result": [_property("0", 2, 1)], "continuation": "1" * 32}
    bridge.read_release = Event()
    pending = hass.async_create_task(_read_trial(hass, entry, hass_admin_user, "state"))
    try:
        await wait_for(bridge.read_started.wait(), timeout=2)
        with pytest.raises(LocalBridgeError, match="is busy"):
            await wait_for(_read_trial(hass, entry, hass_admin_user, "status"), timeout=2)
        gets = bridge.gets
        await coordinator.async_refresh()
        assert bridge.gets == gets
        assert len(bridge.posts) == 1
    finally:
        bridge.read_release.set()
    assert (await pending)["property"] == "state"
    assert len(bridge.posts) == 1

    bridge.read_reply = {"result": [_property("5", 4, 1)], "continuation": "2" * 32}
    assert (await _read_trial(hass, entry, hass_admin_user, "status"))["property"] == "status"
    assert bridge.posts[-1]["continuation"] == "1" * 32
    assert len(bridge.posts) == 2


@pytest.mark.parametrize("disconnect", [False, True])
async def test_passive_poll_loss_seals_in_progress_trial(hass, bridge, hass_admin_user, disconnect):
    bridge.state = _state(_property("0", 2, 2), sequence=1)
    entry, coordinator = await _load_entry(hass, bridge)
    bridge.read_reply = {"result": [_property("0", 2, 1)], "continuation": "1" * 32}
    await _read_trial(hass, entry, hass_admin_user, "state")

    if disconnect:
        bridge.state = _state(epoch=None)
    else:
        bridge.status = 503
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE

    bridge.status = 200
    bridge.state = _state(_property("0", 2, 2), sequence=2)
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == "idle"
    with pytest.raises(LocalBridgeError, match="trial ended"):
        await _read_trial(hass, entry, hass_admin_user, "status")
    assert len(bridge.posts) == 1


async def test_reconfigure_reloads_and_never_publishes(hass, bridge):
    entry, previous = await _load_entry(hass, bridge)
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    )
    assert result["step_id"] == "local_bridge"
    bridge.token = "B" * 32
    result = await _configure(hass, bridge, result["flow_id"])
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert entry.data[CONF_BRIDGE_TOKEN] == bridge.token
    assert hass.data[DOMAIN][entry.entry_id] is not previous
    assert not bridge.posts


@pytest.mark.parametrize(
    "bad_state",
    [
        '{"version":1,"version":1}',
        '{"version":1,"value":NaN}',
        json.dumps(_state(_property("2", 3, True), sequence=2)),
        json.dumps(_state(_property("2", 3, 101), sequence=2)),
        json.dumps(_state(_property("2", 2, 2), sequence=2)),
        json.dumps(_state(_property("2", 3, 25), _property("2", 3, 30), sequence=2)),
        json.dumps(_state(_property("2", 3, 25), sequence=1 << 53)),
        json.dumps(_state(_property("2", 3, 30), sequence=1)),  # Unchanged sequence, changed state.
        json.dumps(_state(_property("2", 3, 25), sequence=0)),
        json.dumps(_state(sequence=2)),  # Unready state may not carry a sequence.
        " " * 4097,
    ],
)
async def test_reject_invalid_snapshots(hass, bridge, bad_state):
    bridge.state = _state(_property("2", 3, 25), sequence=1)
    _, coordinator = await _load_entry(hass, bridge)
    assert hass.states.get(_BATTERY).state == "25"
    bridge.raw_state = bad_state
    await _refresh(hass, coordinator)
    assert not coordinator.device.data
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE
    assert not bridge.posts


@pytest.mark.parametrize(
    "value",
    [
        "http://10.0.0.1:8443",
        "https://example.test:8443",
        "https://224.0.0.1:8443",
        "https://user:password@10.0.0.1:8443",
        "https://10.0.0.1:8443/path",
        "https://10.0.0.1:8443/?extra=1",
        "https://10.0.0.1:8443/#fragment",
        "https://[fe80::1%25en0]:8443",
    ],
)
async def test_reject_unsafe_origins(value):
    with pytest.raises(ValueError):
        validate_bridge_url(value)
