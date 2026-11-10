"""Exercise the real Home Assistant entry, entity, and service lifecycle."""

from __future__ import annotations

from asyncio import CancelledError, Event, current_task, get_running_loop, sleep, timeout, wait_for
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import socket
import ssl
from unittest.mock import patch

from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from probatio import Invalid
import pytest
import pytest_socket

from homeassistant.config_entries import ConfigEntryState, SOURCE_RECONFIGURE, SOURCE_USER
from homeassistant.const import CONF_NAME, EVENT_HOMEASSISTANT_STOP, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import Context
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import HomeAssistantError, Unauthorized
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.dreame_vacuum.const import CONF_TYPE, DOMAIN
from custom_components.dreame_vacuum import local_bridge
from custom_components.dreame_vacuum.diagnostics import async_get_config_entry_diagnostics
from custom_components.dreame_vacuum.dreame import (
    DreameVacuumAction,
    DreameVacuumProperty,
    InvalidActionException,
)
from custom_components.dreame_vacuum.local_bridge import (
    ACCOUNT_TYPE_LOCAL_BRIDGE,
    CONF_BRIDGE_FINGERPRINT,
    CONF_BRIDGE_READ_MODE,
    CONF_BRIDGE_TOKEN,
    CONF_BRIDGE_URL,
    CONF_ENTRY_ID,
    CONF_PROPERTY,
    READ_MODE_DURABLE_REFRESH,
    READ_MODE_LEGACY,
    READ_MODE_PASSIVE,
    READ_MODE_STATE_REFRESH,
    READ_MODE_STATE_REFRESH_RUN,
    SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN,
    SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN,
    SERVICE_READ_LOCAL_BATTERY,
    SERVICE_READ_LOCAL_PROPERTY,
    SERVICE_REFRESH_LOCAL_STATE,
    SERVICE_START_LOCAL_STATE_REFRESH_RUN,
    LocalBridgeError,
    validate_bridge_url,
)

_TOKEN = "A" * 32
_EPOCH = "a" * 32
_NEW_EPOCH = "b" * 32
_VACUUM = "vacuum.bridge_test"
_BATTERY = "sensor.bridge_test_battery_level"
_REFRESH_SENSOR = "sensor.bridge_test_local_state_refresh"
_RUN_SENSOR = "sensor.bridge_test_local_state_refresh_run"
_PASSIVE_STATE = "sensor.bridge_test_local_passive_state"
_PASSIVE_STATUS = "sensor.bridge_test_local_passive_status"
_REFRESH_ADDRESSES = (
    ("0", 2, 1),
    ("5", 4, 1),
    ("3", 3, 2),
    ("11", 4, 7),
    ("1", 2, 2),
)

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
    actions: list[dict] = field(default_factory=list)
    action_fault: str | None = None
    posts: list[dict] = field(default_factory=list)
    gets: int = 0
    token: str = _TOKEN
    url: str = ""
    status: int = 200
    raw_state: str | None = None
    read_reply: dict = field(default_factory=dict)
    read_replies: list[dict] = field(default_factory=list)
    read_error: str | None = None
    read_status: int = 200
    read_started: Event = field(default_factory=Event)
    read_release: Event | None = None
    hold_read_at: int = 1
    read_state: dict | None = None
    request_times: list[float] = field(default_factory=list)

    async def get_state(self, request):
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            raise web.HTTPUnauthorized()
        self.gets += 1
        if self.raw_state is not None:
            return web.Response(text=self.raw_state, content_type="application/json", status=self.status)
        return web.json_response(self.state, status=self.status)

    async def post_action(self, request):
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            raise web.HTTPUnauthorized()
        payload = await request.json()
        self.actions.append(payload)
        reply = {"version": 1, "request_id": payload["request_id"], "epoch": payload["epoch"],
                 "result": {"action": payload["action"], "code": 0}}
        if self.action_fault == "identity":
            reply["request_id"] = "f" * 32
        elif self.action_fault == "rejected":
            reply["result"]["code"] = -1
        elif self.action_fault == "http":
            raise web.HTTPServiceUnavailable()
        return web.json_response(reply)

    async def post_read(self, request):
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            raise web.HTTPUnauthorized()
        payload = await request.json()
        self.posts.append(payload)
        self.request_times.append(get_running_loop().time())
        index = len(self.posts) - 1
        if index + 1 == self.hold_read_at:
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
        if index < len(self.read_replies):
            reply.update(self.read_replies[index])
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


async def _load_entry(hass, bridge, **changes):
    result = await _configure(
        hass, bridge, await _start_flow(hass), **({CONF_BRIDGE_READ_MODE: READ_MODE_LEGACY} | changes)
    )
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


async def _refresh_trial(hass, entry, user, *, response=True, **fields):
    return await hass.services.async_call(
        DOMAIN,
        SERVICE_REFRESH_LOCAL_STATE,
        {CONF_ENTRY_ID: entry.entry_id} | fields,
        blocking=True,
        return_response=response,
        context=Context(user_id=user.id),
    )


def _prepare_refresh(bridge, values=(2, 14, 2, 0, 0)):
    bridge.state = _state(_property("0", 2, 3), _property("2", 3, 42), sequence=1)
    bridge.read_replies = [
        {
            "result": [_property(did, siid, value, piid=piid)],
            "continuation": f"{index + 1:032x}" if index < 4 else None,
        }
        for index, ((did, siid, piid), value) in enumerate(
            zip(_REFRESH_ADDRESSES, values, strict=True)
        )
    ]


async def _load_refresh_entry(hass, bridge):
    return await _load_entry(hass, bridge, **{CONF_BRIDGE_READ_MODE: READ_MODE_STATE_REFRESH})


def _assert_no_refresh(hass):
    state = hass.states.get(_REFRESH_SENSOR)
    assert state.state == STATE_UNAVAILABLE
    assert {"provenance", "properties", "sample_window"}.isdisjoint(state.attributes)


async def test_refresh_mode_is_explicit_per_entry(hass, bridge_factory, hass_admin_user):
    legacy_bridge, refresh_bridge = await bridge_factory(), await bridge_factory()
    legacy_entry, _ = await _load_entry(hass, legacy_bridge)
    assert legacy_entry.data[CONF_BRIDGE_READ_MODE] == READ_MODE_LEGACY
    refresh_entry, _ = await _load_refresh_entry(hass, refresh_bridge)
    assert refresh_entry.data[CONF_BRIDGE_READ_MODE] == READ_MODE_STATE_REFRESH
    with pytest.raises(HomeAssistantError, match="requires the state refresh bridge mode"):
        await _refresh_trial(hass, legacy_entry, hass_admin_user)
    with pytest.raises(HomeAssistantError, match="require the legacy local bridge mode"):
        await _read_trial(hass, refresh_entry, hass_admin_user, "state")
    with pytest.raises(HomeAssistantError, match="require the legacy local bridge mode"):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_READ_LOCAL_BATTERY,
            {CONF_ENTRY_ID: refresh_entry.entry_id},
            blocking=True,
            return_response=True,
            context=Context(user_id=hass_admin_user.id),
        )
    assert legacy_bridge.gets == refresh_bridge.gets == 2
    assert legacy_bridge.posts == refresh_bridge.posts == []
    assert await hass.config_entries.async_unload(refresh_entry.entry_id)
    assert hass.services.has_service(DOMAIN, SERVICE_REFRESH_LOCAL_STATE)
    assert await hass.config_entries.async_unload(legacy_entry.entry_id)
    assert not hass.services.has_service(DOMAIN, SERVICE_REFRESH_LOCAL_STATE)


async def test_each_refresh_entry_gets_one_attempt(hass, bridge_factory, hass_admin_user):
    first_bridge, second_bridge = await bridge_factory(), await bridge_factory()
    first, _ = await _load_refresh_entry(hass, first_bridge)
    _prepare_refresh(second_bridge)
    second, _ = await _load_refresh_entry(hass, second_bridge)
    with pytest.raises(HomeAssistantError, match="no fresh telemetry"):
        await _refresh_trial(hass, first, hass_admin_user)
    assert not first_bridge.posts
    assert (await _refresh_trial(hass, second, hass_admin_user))["provenance"] == "solicited"
    assert len(second_bridge.posts) == 5


@pytest.mark.parametrize("remove_mode", [False, True])
async def test_unspecified_read_mode_only_allows_passive_telemetry(hass, bridge, hass_admin_user, remove_mode):
    _prepare_refresh(bridge)
    bridge.read_replies.clear()
    result = await _configure(hass, bridge, await _start_flow(hass))
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    await hass.async_block_till_done()
    assert entry.data[CONF_BRIDGE_READ_MODE] == READ_MODE_PASSIVE
    if remove_mode:
        data = dict(entry.data)
        del data[CONF_BRIDGE_READ_MODE]
        hass.config_entries.async_update_entry(entry, data=data)
        assert await hass.config_entries.async_reload(entry.entry_id)
    assert hass.states.get(_BATTERY).state == "42"
    assert hass.states.get(_VACUUM).state == "paused"
    with pytest.raises(HomeAssistantError, match="require the legacy local bridge mode"):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_READ_LOCAL_BATTERY,
            {CONF_ENTRY_ID: entry.entry_id},
            blocking=True,
            return_response=True,
            context=Context(user_id=hass_admin_user.id),
        )
    assert not bridge.posts


async def test_refresh_service_authorization(hass, bridge, hass_admin_user, hass_read_only_user):
    _prepare_refresh(bridge)
    entry, _ = await _load_refresh_entry(hass, bridge)
    with pytest.raises(Unauthorized):
        await _refresh_trial(hass, entry, hass_read_only_user)
    with pytest.raises(HomeAssistantError):
        await _refresh_trial(hass, entry, hass_admin_user, response=False)
    with pytest.raises(Invalid):
        await _refresh_trial(hass, entry, hass_admin_user, property="state")
    assert bridge.gets == 2
    assert not bridge.posts


@pytest.mark.parametrize(
    ("values", "labels"),
    [
        ((2, 14, 2, 0, 0), ("idle", "sleeping", "not_charging", "completed", "no_error")),
        ((0, 14, 2, 0, 0), ("charging_completed", "sleeping", "not_charging", "completed", "no_error")),
        ((19, -(1 << 31), -1, 65536, (1 << 31) - 1), (None, None, None, None, None)),
    ],
)
async def test_refresh_publishes_only_a_complete_solicited_snapshot(
    hass, bridge, hass_admin_user, values, labels
):
    _prepare_refresh(bridge, values)
    entry, coordinator = await _load_refresh_entry(hass, bridge)
    _assert_no_refresh(hass)
    entity = er.async_get(hass).async_get(_REFRESH_SENSOR)
    assert entity.entity_category is er.EntityCategory.DIAGNOSTIC
    assert hass.states.get(_VACUUM).state == "paused"
    assert hass.states.get(_VACUUM).attributes["supported_features"] == 0

    result = await _refresh_trial(hass, entry, hass_admin_user)
    assert bridge.gets == 4
    assert len(bridge.posts) == 5
    assert len({post["request_id"] for post in bridge.posts}) == 5
    for index, (post, (did, siid, piid)) in enumerate(zip(bridge.posts, _REFRESH_ADDRESSES, strict=True)):
        assert post["epoch"] == _EPOCH
        assert post["method"] == "get_properties"
        assert post["timeout"] == 5
        assert post["params"] == [{"did": did, "siid": siid, "piid": piid}]
        assert post["continuation"] == (f"{index:032x}" if index else None)
    assert result["provenance"] == "solicited"
    names = ("state", "status", "charging_status", "task_status", "error")
    assert tuple(result["properties"]) == names
    previous_response = None
    previous_age = float("inf")
    for name, value, label in zip(names, values, labels, strict=True):
        sample = result["properties"][name]
        assert (sample["value"], sample["label"]) == (value, label)
        started = datetime.fromisoformat(sample["requested_at"])
        ended = datetime.fromisoformat(sample["responded_at"])
        assert started.utcoffset() == ended.utcoffset() == timedelta(0)
        assert (previous_response or started) <= started <= ended
        previous_response = ended
        assert 0 <= sample["age_at_publication_seconds"] <= previous_age
        previous_age = sample["age_at_publication_seconds"]
    assert result["sample_window"] == {
        "start": result["properties"]["state"]["requested_at"],
        "end": result["properties"]["error"]["responded_at"],
    }
    sensor = hass.states.get(_REFRESH_SENSOR)
    assert sensor.state == (labels[0] or STATE_UNKNOWN)
    assert {key: sensor.attributes[key] for key in result} == result
    assert hass.states.get(_VACUUM).state == "paused"
    assert hass.states.get(_BATTERY).state == "42"
    result["properties"]["state"]["value"] = 999
    assert coordinator.refresh_snapshot["properties"]["state"]["value"] == values[0]
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _refresh_trial(hass, entry, hass_admin_user)
    assert bridge.gets == 4
    assert len(bridge.posts) == 5


@pytest.mark.parametrize(
    ("step", "reply", "message"),
    [
        (2, {"result": []}, "Invalid bridge property response"),
        (3, {"continuation": "malformed"}, "Invalid bridge continuation receipt"),
        (3, {"continuation": "0" * 31 + "1"}, "reused a continuation receipt"),
        (4, {"result": [_property("11", 4, 1 << 31, piid=7)]}, "Invalid bridge property response"),
        (5, {"continuation": "f" * 32}, "Invalid final bridge continuation receipt"),
        (5, {"request_id": "f" * 32}, "did not match the request"),
    ],
)
async def test_invalid_refresh_response_stops_the_cycle(hass, bridge, hass_admin_user, step, reply, message):
    _prepare_refresh(bridge)
    bridge.read_replies[step - 1].update(reply)
    entry, _ = await _load_refresh_entry(hass, bridge)
    with pytest.raises(HomeAssistantError, match=message):
        await _refresh_trial(hass, entry, hass_admin_user)
    _assert_no_refresh(hass)
    assert bridge.gets == 3
    assert len(bridge.posts) == step
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _refresh_trial(hass, entry, hass_admin_user)
    assert len(bridge.posts) == step


@pytest.mark.parametrize("replacement", [True, False])
async def test_refresh_discards_reads_after_final_epoch_or_readiness_loss(
    hass, bridge, hass_admin_user, replacement
):
    _prepare_refresh(bridge)
    entry, _ = await _load_refresh_entry(hass, bridge)
    bridge.read_state = (
        _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
        if replacement else _state(epoch=None)
    )
    with pytest.raises(HomeAssistantError, match="session changed"):
        await _refresh_trial(hass, entry, hass_admin_user)
    _assert_no_refresh(hass)
    assert bridge.gets == 4
    assert len(bridge.posts) == 5


async def test_failed_preflight_consumes_the_entry_attempt(hass, bridge, hass_admin_user):
    entry, coordinator = await _load_refresh_entry(hass, bridge)
    with pytest.raises(HomeAssistantError, match="no fresh telemetry"):
        await _refresh_trial(hass, entry, hass_admin_user)
    _prepare_refresh(bridge)
    await _refresh(hass, coordinator)
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _refresh_trial(hass, entry, hass_admin_user)
    _assert_no_refresh(hass)
    assert bridge.gets == 4
    assert not bridge.posts


@pytest.mark.parametrize("stop", ["cancel", "unload", "deadline"])
async def test_inflight_refresh_ends_without_a_followup(
    hass, bridge, hass_admin_user, monkeypatch, stop
):
    _prepare_refresh(bridge)
    entry, coordinator = await _load_refresh_entry(hass, bridge)
    bridge.hold_read_at = 2
    bridge.read_release = Event()
    if stop == "deadline":
        monkeypatch.setattr(local_bridge, "_REFRESH_TIMEOUT", 0.2)
    pending = hass.async_create_task(_refresh_trial(hass, entry, hass_admin_user))
    try:
        await wait_for(bridge.read_started.wait(), timeout=2)
        _assert_no_refresh(hass)
        with pytest.raises(HomeAssistantError, match="busy"):
            await _refresh_trial(hass, entry, hass_admin_user)
        await coordinator.async_refresh()
        assert bridge.gets == 3
        if stop == "unload":
            assert await wait_for(hass.config_entries.async_unload(entry.entry_id), timeout=2)
        elif stop == "cancel":
            pending.cancel()
        if stop == "deadline":
            with pytest.raises(HomeAssistantError, match="refresh timed out"):
                await wait_for(pending, timeout=2)
        else:
            with pytest.raises(CancelledError):
                await wait_for(pending, timeout=2)
    finally:
        bridge.read_release.set()
    assert len(bridge.posts) == 2
    assert bridge.gets == 3
    if stop == "unload":
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    _assert_no_refresh(hass)
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _refresh_trial(hass, entry, hass_admin_user)
    assert len(bridge.posts) == 2


@pytest.mark.parametrize("invalidate", ["expiry", "disconnect", "epoch", "malformed", "reload"])
async def test_refresh_lifetime_does_not_follow_passive_telemetry(
    hass, bridge, hass_admin_user, monkeypatch, invalidate
):
    _prepare_refresh(bridge)
    if invalidate == "expiry":
        monkeypatch.setattr(local_bridge, "_REFRESH_TTL", 0.25)
    entry, coordinator = await _load_refresh_entry(hass, bridge)
    before = hass.loop.time()
    await _refresh_trial(hass, entry, hass_admin_user)
    expiry = coordinator._refresh_expiry
    assert expiry is not None
    assert before <= expiry.when() - local_bridge._REFRESH_TTL <= bridge.request_times[0]
    assert hass.states.get(_REFRESH_SENSOR).state == "idle"
    if invalidate == "reload":
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
        assert expiry.cancelled()
    else:
        bridge.state = _state(_property("0", 2, 2), sequence=2)
        if invalidate == "disconnect":
            bridge.state = _state(epoch=None)
        elif invalidate == "epoch":
            bridge.state = _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
        elif invalidate == "malformed":
            bridge.raw_state = "{"
        await _refresh(hass, coordinator)
        if invalidate == "expiry":
            assert coordinator._refresh_expiry is expiry
            assert hass.states.get(_REFRESH_SENSOR).state == "idle"
            await sleep(max(0, expiry.when() - hass.loop.time()) + 0.01)
            assert hass.states.get(_VACUUM).state == "idle"
            assert bridge.gets == 5
        else:
            assert expiry.cancelled()
    _assert_no_refresh(hass)
    assert len(bridge.posts) == 5


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


@pytest.mark.parametrize("mode", [READ_MODE_LEGACY, READ_MODE_STATE_REFRESH])
async def test_reconfigure_reloads_and_never_publishes(hass, bridge, mode):
    entry, previous = await _load_entry(hass, bridge, **{CONF_BRIDGE_READ_MODE: mode})
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
    assert entry.data[CONF_BRIDGE_READ_MODE] == mode
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


@dataclass
class FakeRefreshRunBridge(FakeBridge):
    """Pinned TLS fixture with independently controlled claim and read responses."""

    pin: str = ""
    remaining_ms: int = 599_000
    claims: list[dict] = field(default_factory=list, repr=False)
    aborts: list[dict] = field(default_factory=list, repr=False)
    posts: list[dict] = field(default_factory=list, repr=False)
    claim_started: Event = field(default_factory=Event)
    claim_release: Event | None = None
    abort_status: int = 200
    mutate_response: object = None
    last_read_status: int = 200
    raw_claim: str | None = None
    require_arm: bool = False
    armed: dict | None = field(default=None, repr=False)

    def matches_arm(self, payload):
        return self.armed == {
            "epoch": payload["epoch"],
            "run_id": payload["run_id"],
            "revoke_commitment": sha256(payload["revoke_secret"].encode("ascii")).hexdigest(),
        }

    async def post_run(self, request):
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            raise web.HTTPUnauthorized()
        payload = await request.json()
        action = request.match_info["action"]
        reply = {key: payload[key] for key in ("version", "request_id", "epoch", "run_id")}
        status = 200
        if action == "claim":
            self.claims.append(payload)
            self.claim_started.set()
            if self.claim_release is not None:
                await self.claim_release.wait()
            mode = READ_MODE_DURABLE_REFRESH if self.require_arm else READ_MODE_STATE_REFRESH_RUN
            if payload["mode"] != mode:
                return web.json_response(reply | {"error": "not_armed"})
            if self.require_arm:
                if not self.matches_arm(payload):
                    return web.json_response(reply | {"error": "not_armed"})
                self.armed = None
            reply |= {
                "mode": mode,
                "max_cycles": 3,
                "max_reads": 15,
                "min_cycle_interval_seconds": 120,
                "remaining_grant_ms": self.remaining_ms,
                "cycle": 1,
                "cycle_capability": f"{1:032x}",
            }
        elif action == "abort":
            self.aborts.append(payload)
            if self.require_arm and self.matches_arm(payload):
                self.armed = None
            status = self.abort_status
            reply["terminal"] = True
        else:
            self.posts.append(payload)
            self.request_times.append(get_running_loop().time())
            index = len(self.posts)
            if index == self.hold_read_at:
                self.read_started.set()
                if self.read_release is not None:
                    await self.read_release.wait()
            run_index = sum(item["run_id"] == payload["run_id"] for item in self.posts)
            cycle, property_index = divmod(run_index - 1, 5)
            did, siid, piid = _REFRESH_ADDRESSES[property_index]
            value = (2, -(1 << 31), 2, 0, (1 << 31) - 1)[property_index]
            last = property_index == 4
            reply |= {
                "cycle": cycle + 1,
                "result": [_property(did, siid, value, piid=piid)],
                "continuation": None if last else f"{100 + index:032x}",
                "next_cycle_capability": f"{cycle + 2:032x}" if last and cycle < 2 else None,
            }
            if last:
                status = self.last_read_status
                if self.read_state is not None:
                    self.state = self.read_state
        if self.mutate_response is not None:
            reply = self.mutate_response(action, payload, reply)
        if action == "claim" and self.raw_claim is not None:
            return web.Response(
                text=json.dumps(reply)[:-1] + self.raw_claim, content_type="application/json"
            )
        return web.json_response(reply, status=status)


@pytest.fixture
async def run_bridge(aiohttp_server, socket_enabled, tmp_path):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "loopback bridge fixture")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "test.crt", tmp_path / "test.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    bridge = FakeRefreshRunBridge(pin=cert.fingerprint(hashes.SHA256()).hex())
    _prepare_refresh(bridge)
    app = web.Application()
    app.router.add_get("/v1/state", bridge.get_state)
    app.router.add_post("/v1/read", bridge.post_read)
    app.router.add_post("/v1/action", bridge.post_action)
    app.router.add_post("/v2/refresh/{action:claim|read|abort}", bridge.post_run)
    server = await aiohttp_server(app, ssl=context)
    bridge.url = str(server.make_url("/").origin())
    return bridge


async def _load_run_entry(hass, bridge):
    return await _load_entry(hass, bridge, **{
        CONF_BRIDGE_READ_MODE: READ_MODE_STATE_REFRESH_RUN,
        CONF_BRIDGE_FINGERPRINT: bridge.pin,
    })


async def _load_durable_entry(hass, bridge):
    bridge.require_arm = True
    return await _load_entry(hass, bridge, **{
        CONF_BRIDGE_READ_MODE: READ_MODE_DURABLE_REFRESH,
        CONF_BRIDGE_FINGERPRINT: bridge.pin,
    })


async def _run_action(hass, entry, context, service=SERVICE_START_LOCAL_STATE_REFRESH_RUN, **fields):
    return await hass.services.async_call(
        DOMAIN, service, {CONF_ENTRY_ID: entry.entry_id} | fields,
        blocking=True, return_response=True, context=context,
    )


async def _wait_for_run_state(hass, expected):
    try:
        async with timeout(2):
            while hass.states.get(_RUN_SENSOR).state != expected:
                await sleep(0.002)
    except TimeoutError:
        pytest.fail(f"Expected {expected}; last progress {hass.states.get(_RUN_SENSOR)}")
    return hass.states.get(_RUN_SENSOR)


@pytest.fixture
def advance_loop(hass, monkeypatch):
    """Advance the actual asyncio timer queue without accelerating wall time."""
    loop = hass.loop
    real_time = loop.time
    offset = 0
    monkeypatch.setattr(loop, "time", lambda: real_time() + offset)
    monkeypatch.setattr(loop, "slow_callback_duration", float("inf"))

    async def advance(seconds, *, smooth=False):
        nonlocal offset
        if smooth:
            # Let each real HA poll start near its due time instead of
            # bunching timer handles at arbitrary jumps.
            while seconds > 0.25:
                offset += 0.25
                seconds -= 0.25
                await sleep(0.002)
        offset += seconds
        await sleep(0.01)

    return advance


@pytest.mark.parametrize(("raw_state", "raw_error", "activity"), [
    (2, 0, "idle"),
    (0, 0, "docked"),
    (4, 0, "error"),
    (19, 0, STATE_UNKNOWN),
    (19, 1, "error"),
    (2, -1, STATE_UNKNOWN),
    (2, (1 << 31) - 1, STATE_UNKNOWN),
])
async def test_durable_vacuum_uses_only_fresh_complete_solicited_state(
    hass, run_bridge, hass_admin_user, advance_loop, raw_state, raw_error, activity
):
    run_bridge.state = _state(
        _property("0", 2, 3), _property("5", 4, 14), _property("2", 3, 42), sequence=1
    )
    entry, coordinator = await _load_durable_entry(hass, run_bridge)
    passive = [hass.states.get(entity) for entity in (_PASSIVE_STATE, _PASSIVE_STATUS)]
    assert [state.state for state in passive] == ["paused", "sleeping"]
    assert all(state.attributes["provenance"] == "passive" for state in passive)
    assert hass.states.get(_BATTERY) is None
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
    assert hass.states.get(_VACUUM).attributes["supported_features"] == 0

    def mutate(action, payload, reply):
        if action == "read":
            item = reply["result"][0]
            if item["did"] == "0":
                item["value"] = raw_state
            elif item["did"] == "1":
                item["value"] = raw_error
        return reply

    run_bridge.mutate_response = mutate
    run_bridge.hold_read_at = 5
    run_bridge.read_release = Event()
    context = Context(user_id=hass_admin_user.id)
    prepared = await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
    run_bridge.armed = prepared
    try:
        await _run_action(hass, entry, context)
        await wait_for(run_bridge.read_started.wait(), 2)
        assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
        _assert_no_refresh(hass)
    finally:
        run_bridge.read_release.set()
    await _wait_for_run_state(hass, "waiting")
    vacuum = hass.states.get(_VACUUM)
    assert vacuum.state == activity
    assert vacuum.attributes["provenance"] == "solicited"
    assert vacuum.attributes["sample_window"] == coordinator.refresh_snapshot["sample_window"]
    assert coordinator.refresh_snapshot["properties"]["error"]["value"] == raw_error
    assert coordinator.refresh_snapshot["properties"]["status"]["label"] is None
    assert len(run_bridge.posts) == 5
    for original in passive:
        current = hass.states.get(original.entity_id)
        assert current.state == original.state and current.last_changed == original.last_changed
        assert current.last_updated == original.last_updated

    await advance_loop(60.1)
    expired = hass.states.get(_VACUUM)
    assert expired.state == STATE_UNAVAILABLE
    assert {"provenance", "sample_window"}.isdisjoint(expired.attributes)
    assert [hass.states.get(entity).state for entity in (_PASSIVE_STATE, _PASSIVE_STATUS)] == [
        "paused", "sleeping"
    ]
    assert hass.states.get(_BATTERY) is None
    assert len(run_bridge.posts) == 5


@pytest.mark.parametrize("termination", ["cancel", "epoch", "disconnect"])
async def test_durable_fresh_activity_is_invalidated_without_using_passive_battery(
    hass, run_bridge, hass_admin_user, termination
):
    run_bridge.state = _state(_property("0", 2, 3), sequence=1)
    entry, coordinator = await _load_durable_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)

    def mutate(action, payload, reply):
        if action == "read" and reply["result"][0]["did"] == "1":
            reply["result"][0]["value"] = 0
        return reply

    run_bridge.mutate_response = mutate
    run_bridge.armed = await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
    await _run_action(hass, entry, context)
    await _wait_for_run_state(hass, "waiting")
    assert hass.states.get(_VACUUM).state == "idle"
    assert hass.states.get(_BATTERY) is None
    if termination == "cancel":
        await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
    else:
        run_bridge.state = (
            _state(_property("0", 2, 3), epoch=_NEW_EPOCH, sequence=1)
            if termination == "epoch" else _state(epoch=None)
        )
        await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
    _assert_no_refresh(hass)
    assert hass.states.get(_BATTERY) is None
    assert len(run_bridge.posts) == 5


@pytest.mark.parametrize("termination", ["cancel", "reload", "epoch", "disconnect", "expiry", "stop"])
async def test_durable_unstarted_preparation_is_volatile_and_revocable(
    hass, run_bridge, hass_admin_user, advance_loop, termination
):
    entry, coordinator = await _load_durable_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    with pytest.raises(HomeAssistantError, match="Prepare and arm"):
        await _run_action(hass, entry, context)
    prepared = await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
    assert set(prepared) == {"epoch", "run_id", "revoke_commitment"}
    assert prepared["epoch"] == _EPOCH and len(prepared["run_id"]) == 32
    assert len(prepared["revoke_commitment"]) == 64
    assert hass.states.get(_RUN_SENSOR).state == "prepared"
    with pytest.raises(HomeAssistantError, match="already prepared"):
        await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
    assert not run_bridge.claims and not run_bridge.posts and not run_bridge.aborts
    run_bridge.armed = prepared
    if termination == "cancel":
        await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
    elif termination == "reload":
        assert await hass.config_entries.async_reload(entry.entry_id)
        assert hass.states.get(_RUN_SENSOR).state == "inactive"
    elif termination == "epoch":
        run_bridge.state = _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
        await _refresh(hass, coordinator)
    elif termination == "disconnect":
        run_bridge.state = _state(epoch=None)
        await _refresh(hass, coordinator)
    elif termination == "expiry":
        await advance_loop(600.1)
    else:
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
    await hass.async_block_till_done()
    with pytest.raises(HomeAssistantError, match="Prepare and arm"):
        await _run_action(hass, entry, context)
    assert coordinator._prepared_run is None
    assert len(run_bridge.aborts) == 1 and run_bridge.armed is None
    assert run_bridge.aborts[0]["run_id"] == prepared["run_id"]
    assert not run_bridge.claims and not run_bridge.posts
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE


async def test_durable_unarmed_attempt_cannot_retry_and_fresh_runs_can_repeat(
    hass, run_bridge, hass_admin_user, advance_loop
):
    entry, _ = await _load_durable_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
    with pytest.raises(HomeAssistantError, match="no refresh run authorization"):
        await _run_action(hass, entry, context)
    await hass.async_block_till_done()
    with pytest.raises(HomeAssistantError, match="Prepare and arm"):
        await _run_action(hass, entry, context)
    assert len(run_bridge.claims) == 1 and not run_bridge.posts

    for run_number in (1, 2):
        prepared = await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
        run_bridge.armed = prepared
        await _run_action(hass, entry, context)
        await _wait_for_run_state(hass, "waiting")
        with pytest.raises(HomeAssistantError, match="busy"):
            await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
        await advance_loop(125.3, smooth=True)
        waiting = await _wait_for_run_state(hass, "waiting")
        assert waiting.attributes["completed_cycles"] == 2
        await advance_loop(125.3, smooth=True)
        completed = await _wait_for_run_state(hass, "completed")
        assert completed.attributes["completed_cycles"] == 3
        assert len(run_bridge.posts) == run_number * 15
        assert run_bridge.armed is None
    assert len({item["run_id"] for item in run_bridge.claims}) == 3
    assert len(run_bridge.aborts) == 1


@pytest.mark.parametrize("suppress_cancellation", [False, True])
async def test_durable_cancel_interrupts_preparation_before_readiness_returns(
    hass, run_bridge, hass_admin_user, suppress_cancellation
):
    entry, coordinator = await _load_durable_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    started, resume = Event(), Event()
    ready = await coordinator._bridge.async_get_state()

    async def hold_passive_request():
        started.set()
        try:
            await Event().wait()
        except CancelledError:
            if not suppress_cancellation:
                raise
            current_task().uncancel()
            await resume.wait()
        return ready

    with patch.object(coordinator._bridge, "async_get_state", hold_passive_request):
        pending = hass.async_create_task(
            _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
        )
        await wait_for(started.wait(), 2)
        await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
        resume.set()
        with pytest.raises(CancelledError):
            await wait_for(pending, 2)
    assert coordinator._prepared_run is None
    assert hass.states.get(_RUN_SENSOR).state == "inactive"
    assert not run_bridge.claims and not run_bridge.posts and not run_bridge.aborts


async def test_durable_claim_requires_the_owner_to_echo_the_durable_mode(
    hass, run_bridge, hass_admin_user
):
    entry, _ = await _load_durable_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    run_bridge.armed = await _run_action(hass, entry, context, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)

    def mutate(action, payload, reply):
        if action == "claim":
            reply["mode"] = READ_MODE_STATE_REFRESH_RUN
        return reply

    run_bridge.mutate_response = mutate
    with pytest.raises(HomeAssistantError, match="Invalid bridge refresh run claim response"):
        await _run_action(hass, entry, context)
    await hass.async_block_till_done()
    assert run_bridge.claims[0]["mode"] == READ_MODE_DURABLE_REFRESH
    assert not run_bridge.posts and len(run_bridge.aborts) == 1
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE


async def test_finite_run_three_cycles_keep_passive_poll_phase_and_snapshot_ttl(
    hass, run_bridge, hass_admin_user, advance_loop
):
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    assert hass.states.get(_RUN_SENSOR).state == "inactive"
    assert not run_bridge.posts and not run_bridge.claims
    context = Context(user_id=hass_admin_user.id)
    accepted = await _run_action(hass, entry, context)
    assert accepted["state"] == "starting" and accepted["completed_cycles"] == 0
    assert accepted["max_cycles"] == 3 and accepted["expires_at"]
    first_wait = await _wait_for_run_state(hass, "waiting")
    assert first_wait.attributes["completed_cycles"] == 1
    assert first_wait.attributes["next_due_at"]
    assert hass.states.get(_REFRESH_SENSOR).state == "idle"
    assert hass.states.get(_VACUUM).state == "paused"
    assert hass.states.get(_BATTERY).state == "42"
    initial_snapshot = coordinator.refresh_snapshot
    properties = initial_snapshot["properties"]
    assert (properties["status"]["value"], properties["status"]["label"]) == (-(1 << 31), None)
    assert (properties["error"]["value"], properties["error"]["label"]) == ((1 << 31) - 1, None)
    assert properties["task_status"]["value"] == 0

    for cycle in (1, 2):
        gets = run_bridge.gets
        for _ in range(6):
            await advance_loop(10.1, smooth=True)
        _assert_no_refresh(hass)
        progress = hass.states.get(_RUN_SENSOR)
        assert progress.state == "waiting", progress
        assert len(run_bridge.posts) == cycle * 5
        assert run_bridge.gets - gets >= 5
        for _ in range(6):
            await advance_loop(10.1, smooth=True)
        assert len(run_bridge.posts) == cycle * 5
        await advance_loop(4.1, smooth=True)
        expected = "waiting" if cycle == 1 else "completed"
        state = await _wait_for_run_state(hass, expected)
        assert state.attributes["completed_cycles"] == cycle + 1
        assert hass.states.get(_REFRESH_SENSOR).state == "idle"
        assert run_bridge.request_times[cycle * 5] - run_bridge.request_times[cycle * 5 - 1] >= 125
        coordinator._expire_refresh_snapshot(initial_snapshot)
        assert hass.states.get(_REFRESH_SENSOR).state == "idle"

    for _ in range(13):
        await advance_loop(10.1)
    _assert_no_refresh(hass)
    assert hass.states.get(_RUN_SENSOR).state == "completed"
    assert len(run_bridge.claims) == 1 and len(run_bridge.posts) == 15
    assert not run_bridge.aborts
    assert len({item["request_id"] for item in run_bridge.claims + run_bridge.posts}) == 16
    for index, payload in enumerate(run_bridge.posts):
        cycle, prop = divmod(index, 5)
        did, siid, piid = _REFRESH_ADDRESSES[prop]
        assert payload["params"] == [{"did": did, "siid": siid, "piid": piid}]
        assert payload["version"] == 2 and payload["method"] == "get_properties"
        assert payload["run_id"] == run_bridge.claims[0]["run_id"]
        assert payload["epoch"] == _EPOCH and payload["cycle"] == cycle + 1
        assert payload["timeout"] == 5
        assert payload["cycle_capability"] == (f"{cycle + 1:032x}" if prop == 0 else None)
        assert payload["continuation"] == (f"{100 + index:032x}" if prop > 0 else None)
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _run_action(hass, entry, context)


@pytest.mark.parametrize("transport", ["rest", "websocket"])
async def test_finite_run_accepts_pinned_ha_direct_api_and_ui_calls(
    hass, run_bridge, hass_client, hass_ws_client, transport
):
    entry, _ = await _load_run_entry(hass, run_bridge)
    assert hass.states.get(_RUN_SENSOR).state == "inactive"
    data = {CONF_ENTRY_ID: entry.entry_id}
    if transport == "rest":
        assert await async_setup_component(hass, "api", {})
        client = await hass_client()
        response = await client.post(f"/api/services/{DOMAIN}/{SERVICE_START_LOCAL_STATE_REFRESH_RUN}", json=data)
        assert response.status == 200, await response.text()
    else:
        client = await hass_ws_client()
        await client.send_json({
            "id": 1, "type": "call_service", "domain": DOMAIN,
            "service": SERVICE_START_LOCAL_STATE_REFRESH_RUN, "service_data": data,
            "return_response": True,
        })
        result = await client.receive_json()
        assert result["success"], result
        assert result["result"]["response"]["state"] == "starting"
    await _wait_for_run_state(hass, "waiting")
    assert len(run_bridge.claims) == 1 and len(run_bridge.posts) == 5


async def test_finite_run_requires_direct_admin_exact_schema_and_distinct_mode(
    hass, run_bridge, hass_admin_user, hass_read_only_user
):
    entry, _ = await _load_run_entry(hass, run_bridge)
    admin_context = Context(user_id=hass_admin_user.id)
    for service in (
        SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN,
        SERVICE_START_LOCAL_STATE_REFRESH_RUN,
        SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN,
    ):
        with pytest.raises(HomeAssistantError, match="direct authenticated administrator"):
            await _run_action(hass, entry, Context(), service)
        with pytest.raises(HomeAssistantError, match="direct authenticated administrator"):
            await _run_action(hass, entry, Context(user_id=hass_admin_user.id, parent_id=Context().id), service)
        with pytest.raises(Unauthorized):
            await _run_action(hass, entry, Context(user_id=hass_read_only_user.id), service)
        with pytest.raises(Invalid):
            await _run_action(hass, entry, admin_context, service, cycle=2)
    with pytest.raises(HomeAssistantError, match="requires the state refresh bridge mode"):
        await _refresh_trial(hass, entry, hass_admin_user)
    with pytest.raises(HomeAssistantError, match="require the legacy local bridge mode"):
        await _read_trial(hass, entry, hass_admin_user, "state")
    assert (await _run_action(hass, entry, admin_context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN))["state"] == "inactive"
    assert not run_bridge.claims and not run_bridge.posts and not run_bridge.aborts


@pytest.mark.parametrize("pin", ["", "00" * 32])
async def test_finite_run_requires_the_pinned_https_certificate(hass, run_bridge, pin):
    result = await _configure(hass, run_bridge, await _start_flow(hass), **{
        CONF_BRIDGE_READ_MODE: READ_MODE_STATE_REFRESH_RUN,
        CONF_BRIDGE_FINGERPRINT: pin,
    })
    assert result["type"] is FlowResultType.FORM
    assert result["errors"]["base"] == ("cannot_connect_bridge" if pin else "invalid_bridge_settings")
    assert not run_bridge.claims


@pytest.mark.parametrize("interrupt", ["cancel", "start_task_cancelled", "failed_unload", "core_stop"])
async def test_finite_run_interrupts_pending_claim_and_never_adopts_late_capability(
    hass, run_bridge, hass_admin_user, interrupt
):
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    run_bridge.claim_release = Event()
    run_bridge.abort_status = 503
    context = Context(user_id=hass_admin_user.id)
    start_task = hass.async_create_task(_run_action(hass, entry, context))
    try:
        await wait_for(run_bridge.claim_started.wait(), 2)
        with pytest.raises(HomeAssistantError, match="busy"):
            await _run_action(hass, entry, context)
        assert hass.states.get(_RUN_SENSOR).state == "starting"
        assert not run_bridge.posts
        if interrupt == "cancel":
            stopped = await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
            assert stopped["state"] == "cancelled" and stopped["reason"] == "requested"
            await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
        elif interrupt == "start_task_cancelled":
            start_task.cancel()
        elif interrupt == "failed_unload":
            with patch.object(hass.config_entries, "async_unload_platforms", return_value=False):
                assert not await hass.config_entries.async_unload(entry.entry_id)
        else:
            hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
        with pytest.raises(CancelledError):
            await wait_for(start_task, 2)
        await hass.async_block_till_done()
        assert coordinator._run is None
        assert hass.states.get(_RUN_SENSOR).state == "cancelled"
        _assert_no_refresh(hass)
        assert not run_bridge.posts and len(run_bridge.aborts) == 1
        claim, abort = run_bridge.claims[0], run_bridge.aborts[0]
        assert claim["run_id"] == abort["run_id"] and claim["revoke_secret"] == abort["revoke_secret"]
        assert claim["request_id"] != abort["request_id"]
    finally:
        run_bridge.claim_release.set()


@pytest.mark.parametrize("fault", [
    "lost_fifth", "final_epoch", "read_type", "wrong_address", "reuse_capability", "wrong_identity",
    "wrong_version", "early_cycle_capability", "bad_limits", "duplicate_keys", "short_grant", "oversize",
])
async def test_finite_run_rejects_bad_responses_and_aborts_without_publishing(
    hass, run_bridge, hass_admin_user, fault
):
    entry, _ = await _load_run_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)

    def mutate(action, payload, reply):
        if action == "read":
            if fault == "read_type":
                reply["result"][0]["value"] = True
            elif fault == "wrong_address":
                reply["result"][0]["piid"] = 7
            elif fault == "reuse_capability":
                reply["continuation"] = payload["cycle_capability"]
            elif fault == "wrong_identity":
                reply["run_id"] = "c" * 32
            elif fault == "wrong_version":
                reply["version"] = 1
            elif fault == "early_cycle_capability":
                reply["next_cycle_capability"] = "c" * 32
        elif action == "claim" and fault == "bad_limits":
            reply["cycle"] = True
        return reply

    run_bridge.mutate_response = mutate
    if fault == "lost_fifth":
        run_bridge.last_read_status = 503
    elif fault == "final_epoch":
        run_bridge.read_state = _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
    elif fault == "short_grant":
        run_bridge.remaining_ms = 44_999
    elif fault == "duplicate_keys":
        run_bridge.raw_claim = ', "cycle": 1}'
    elif fault == "oversize":
        run_bridge.raw_claim = "}" + " " * 2048
    if fault in ("bad_limits", "short_grant", "duplicate_keys", "oversize"):
        with pytest.raises(HomeAssistantError):
            await _run_action(hass, entry, context)
    else:
        await _run_action(hass, entry, context)
    failed = await _wait_for_run_state(hass, "failed")
    assert failed.attributes["completed_cycles"] == 0
    await hass.async_block_till_done()
    _assert_no_refresh(hass)
    assert len(run_bridge.aborts) == 1
    assert len(run_bridge.posts) == (
        5 if fault in ("lost_fifth", "final_epoch")
        else 0 if fault in ("bad_limits", "short_grant", "duplicate_keys", "oversize")
        else 1
    )
    if run_bridge.claims:
        exported = str(failed.attributes) + str(await async_get_config_entry_diagnostics(hass, entry))
        for secret in ("run_id", "request_id", "revoke_secret"):
            assert run_bridge.claims[0][secret] not in exported


@pytest.mark.parametrize("stop", ["cancel", "epoch", "late", "busy", "insufficient", "reload", "grant_expired"])
async def test_finite_run_wait_ends_without_catch_up_or_new_claim(
    hass, run_bridge, hass_admin_user, advance_loop, stop
):
    if stop == "insufficient":
        run_bridge.remaining_ms = 169_999
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    await _run_action(hass, entry, context)
    if stop != "insufficient":
        await _wait_for_run_state(hass, "waiting")
    expected, reason = "failed", None
    if stop == "cancel":
        await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
        expected, reason = "cancelled", "requested"
    elif stop == "epoch":
        run_bridge.state = _state(_property("0", 2, 2), epoch=_NEW_EPOCH, sequence=1)
        await _refresh(hass, coordinator)
        reason = "epoch_changed"
    elif stop == "late":
        await advance_loop(131)
        reason = "wake_late"
    elif stop == "busy":
        coordinator._busy = True
        await advance_loop(126)
        coordinator._busy = False
        reason = "bridge_busy"
    elif stop == "insufficient":
        reason = "insufficient_grant"
    elif stop == "grant_expired":
        await advance_loop(601)
        reason = "grant_expired"
    else:
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
        expected = "inactive"
    progress = await _wait_for_run_state(hass, expected)
    assert progress.attributes["reason"] == reason
    _assert_no_refresh(hass)
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _run_action(hass, entry, context)
    await hass.async_block_till_done()
    assert len(run_bridge.claims) == 1 and len(run_bridge.posts) == 5
    assert len(run_bridge.aborts) == 1


async def test_finite_run_handles_cancellation_before_background_task_entry(
    hass, run_bridge, hass_admin_user
):
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    create_background_task = type(entry).async_create_background_task

    def cancel_before_start(self, hass, target, name, eager_start=True):
        task = create_background_task(self, hass, target, name, eager_start)
        if name == "local state refresh run":
            assert not eager_start
            task.cancel()
        return task

    with patch.object(type(entry), "async_create_background_task", cancel_before_start):
        accepted = await _run_action(hass, entry, context)
    assert accepted["state"] == "starting"
    await _wait_for_run_state(hass, "cancelled")
    await hass.async_block_till_done()
    assert coordinator._run is None
    assert len(run_bridge.claims) == len(run_bridge.aborts) == 1
    assert not run_bridge.posts


async def test_finite_run_cancels_a_pending_read_with_one_abort(
    hass, run_bridge, hass_admin_user
):
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    run_bridge.hold_read_at = 3
    run_bridge.read_release = Event()
    context = Context(user_id=hass_admin_user.id)
    await _run_action(hass, entry, context)
    try:
        await wait_for(run_bridge.read_started.wait(), 2)
        assert hass.states.get(_RUN_SENSOR).state == "sampling"
        assert coordinator._run is not None
        assert (await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN))["state"] == "cancelled"
        assert coordinator._run is None
        assert not run_bridge.read_release.is_set()
        assert len(run_bridge.aborts) == 1
    finally:
        run_bridge.read_release.set()
    await hass.async_block_till_done()
    _assert_no_refresh(hass)
    assert len(run_bridge.posts) == 3


async def test_finite_run_wake_tolerance_is_not_a_read_deadline(
    hass, run_bridge, hass_admin_user, advance_loop
):
    entry, _ = await _load_run_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    await _run_action(hass, entry, context)
    await _wait_for_run_state(hass, "waiting")
    expected_due = run_bridge.request_times[-1] + 125
    await advance_loop(120, smooth=True)
    while hass.loop.time() < expected_due - 3:
        await advance_loop(0.25)
    run_bridge.hold_read_at = 6
    run_bridge.read_started.clear()
    run_bridge.read_release = Event()
    try:
        await advance_loop(expected_due + 2 - hass.loop.time())
        await wait_for(run_bridge.read_started.wait(), 2)
        await advance_loop(4)
        run_bridge.read_release.set()
        await _wait_for_run_state(hass, "waiting")
        assert hass.states.get(_RUN_SENSOR).attributes["completed_cycles"] == 2
        assert run_bridge.request_times[6] > expected_due + 5
        assert len(run_bridge.posts) == 10
        assert not run_bridge.aborts
    finally:
        run_bridge.read_release.set()


async def test_finite_run_cancel_before_claim_dispatch_sends_no_control_post(
    hass, run_bridge, hass_admin_user
):
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    started = Event()

    async def hold_passive_request():
        started.set()
        await Event().wait()

    context = Context(user_id=hass_admin_user.id)
    with patch.object(coordinator._bridge, "async_get_state", hold_passive_request):
        pending = hass.async_create_task(_run_action(hass, entry, context))
        await wait_for(started.wait(), 2)
        progress = await _run_action(hass, entry, context, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
        assert progress["state"] == "cancelled"
        with pytest.raises(CancelledError):
            await wait_for(pending, 2)
    assert coordinator._run is None
    assert not run_bridge.claims and not run_bridge.posts and not run_bridge.aborts
    with pytest.raises(HomeAssistantError, match="already been used"):
        await _run_action(hass, entry, context)


@pytest.mark.parametrize("at_state", ["starting", "sampling"])
async def test_finite_run_releases_busy_if_progress_notification_raises(
    hass, run_bridge, hass_admin_user, at_state
):
    entry, coordinator = await _load_run_entry(hass, run_bridge)
    context = Context(user_id=hass_admin_user.id)
    original = coordinator.async_update_listeners
    failed = False

    def notify():
        nonlocal failed
        if not failed and coordinator.refresh_run_progress["state"] == at_state:
            failed = True
            raise RuntimeError("Progress notification failed")
        original()

    with patch.object(coordinator, "async_update_listeners", notify):
        if at_state == "starting":
            with pytest.raises(HomeAssistantError, match="claim failed"):
                await _run_action(hass, entry, context)
        else:
            await _run_action(hass, entry, context)
        await _wait_for_run_state(hass, "failed")
    await hass.async_block_till_done()
    assert failed and not coordinator._busy and coordinator._run is None
    gets = run_bridge.gets
    await _refresh(hass, coordinator)
    assert run_bridge.gets == gets + 1
    assert not run_bridge.posts
    assert len(run_bridge.aborts) == (1 if at_state == "sampling" else 0)


async def _load_control_entry(hass, bridge):
    return await _load_entry(hass, bridge, **{
        CONF_BRIDGE_READ_MODE: local_bridge.READ_MODE_ON_DEMAND,
        CONF_BRIDGE_FINGERPRINT: bridge.pin,
    })


async def test_local_control_buttons_work_without_a_solicited_snapshot(hass, run_bridge, hass_admin_user):
    entry, coordinator = await _load_control_entry(hass, run_bridge)
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
    context = Context(user_id=hass_admin_user.id)
    for action, entity in (("start", "button.bridge_test_start_cleaning"), ("pause", "button.bridge_test_pause"), ("dock", "button.bridge_test_return_to_dock")):
        assert hass.states.get(entity).state != STATE_UNAVAILABLE
        await hass.services.async_call("button", "press", {"entity_id": entity}, blocking=True, context=context)
        assert run_bridge.actions[-1]["action"] == action
    assert len(run_bridge.actions) == 3
    assert not run_bridge.posts and not run_bridge.claims
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert len(run_bridge.actions) == 3  # Reload does not initiate movement.


@pytest.mark.parametrize("fault", ["identity", "rejected", "http"])
@pytest.mark.parametrize("action", ["start", "dock"])
async def test_local_action_failure_does_not_retry(hass, run_bridge, hass_admin_user, fault, action):
    _, coordinator = await _load_control_entry(hass, run_bridge)
    run_bridge.action_fault = fault
    with pytest.raises(HomeAssistantError):
        await coordinator.async_control(action, Context(user_id=hass_admin_user.id))
    assert len(run_bridge.actions) == 1
    assert coordinator._action_busy is False
    run_bridge.action_fault = None
    await coordinator.async_control("pause", Context(user_id=hass_admin_user.id))
    assert [p["action"] for p in run_bridge.actions] == [action, "pause"]


async def test_local_controls_reject_internal_automated_and_non_admin_calls(hass, run_bridge, hass_admin_user):
    _, coordinator = await _load_control_entry(hass, run_bridge)
    user = await hass.auth.async_create_user("Non admin")
    for context in (Context(), Context(user_id=user.id), Context(user_id=hass_admin_user.id, parent_id="automation")):
        with pytest.raises(HomeAssistantError):
            await coordinator.async_control("start", context)
    assert not run_bridge.actions
    assert not run_bridge.posts


@dataclass
class FakeCandidateBridge(FakeBridge):
    """Serve the independently shaped candidate API on loopback only."""

    reported: dict = field(default_factory=dict)
    epoch: str | None = _EPOCH
    rpc_error: str | None = None
    fail_reads: bool = False
    rpc_out: list[dict] = field(default_factory=list)
    sequence: int = 0
    push: list[dict] = field(default_factory=list)
    map_reports: list[dict] = field(default_factory=list)
    history: list[dict] = field(default_factory=list)

    async def candidate_state(self, request):
        assert request.headers["Authorization"] == f"Bearer {self.token}"
        return web.json_response({
            "version": 1, "epoch": self.epoch, "connected": self.epoch is not None,
            "ready": self.epoch is not None,
            "sequence": self.sequence, "properties": self.push,
            "history": self.history, "map_reports": self.map_reports,
        })

    async def rpc(self, request):
        assert request.headers["Authorization"] == f"Bearer {self.token}"
        data = await request.json()
        self.posts.append(data)
        identity = {key: data[key] for key in ("version", "epoch", "request_id")}
        assert data["epoch"] == self.epoch
        if data["method"] == "get_properties":
            if self.fail_reads:
                return web.json_response(identity | {"error": "timeout"})
            assert 0 < len(data["params"]) <= 15
            return web.json_response(identity | {"result": [
                {**item, "code": 0, "value": self.reported[item["did"]]}
                for item in data["params"] if item["did"] in self.reported
            ]})
        if self.rpc_error is not None:
            return web.json_response(identity | {"error": self.rpc_error})
        if data["method"] == "set_properties":
            return web.json_response(identity | {"result": [
                {key: item[key] for key in ("did", "siid", "piid")} | {"code": 0}
                for item in data["params"]
            ]})
        if data["method"] == "action":
            params = data["params"]
            return web.json_response(identity | {"result": {
                key: params[key] for key in ("did", "siid", "aiid")
            } | {"code": 0, "out": self.rpc_out}})
        raise AssertionError(f"Unexpected method {data['method']}")


@pytest.fixture
async def candidate_bridge(aiohttp_server, socket_enabled):
    bridge = FakeCandidateBridge(
        state=_state(_property("0", 2, 2), _property("2", 3, 42), sequence=1),
        reported={
            "0": 2, "1": 0, "2": 42, "3": 2, "5": 14, "8": 1, "9": 1, "10": 0,
            "11": 0, "27": 0, "31": False, "72": True, "73": "22:00", "74": "08:00",
            "91": 40, "114": "1-1-12:30-1111111-1-0-1-1-0", "120": 82,
        },
    )
    app = web.Application()
    app.router.add_get("/v1/state", bridge.get_state)
    app.router.add_get("/v1/candidate/state", bridge.candidate_state)
    app.router.add_post("/v1/rpc", bridge.rpc)
    server = await aiohttp_server(app)
    bridge.url = str(server.make_url("/").origin())
    return bridge


async def test_candidate_reconfigure_preserves_registry_and_uses_native_entities(hass, candidate_bridge):
    entry, _ = await _load_entry(hass, candidate_bridge)
    registry = er.async_get(hass)
    vacuum_id = registry.async_get(_VACUUM).id
    battery_id = registry.async_get(_BATTERY).id
    retired = registry.async_get_or_create("button", DOMAIN, f"{entry.entry_id}_local_start", config_entry=entry)
    user_disabled = registry.async_get_or_create(
        "button", DOMAIN, f"{entry.entry_id}_local_dock", config_entry=entry,
        disabled_by=er.RegistryEntryDisabler.USER,
    )
    passive = registry.async_get_or_create("sensor", DOMAIN, f"{entry.entry_id}_local_passive_state", config_entry=entry)
    flow = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    )
    result = await _configure(hass, candidate_bridge, flow["flow_id"], bridge_read_mode="local_candidate")
    assert result["type"] is FlowResultType.ABORT
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert registry.async_get(_VACUUM).id == vacuum_id
    assert registry.async_get(_BATTERY).id == battery_id
    assert registry.async_get(retired.entity_id).disabled_by is er.RegistryEntryDisabler.INTEGRATION
    assert registry.async_get(user_disabled.entity_id).disabled_by is er.RegistryEntryDisabler.USER
    assert registry.async_get(passive.entity_id).id == passive.id
    assert hass.states.get(passive.entity_id).attributes["provenance"] == "candidate"
    assert hass.states.get(_BATTERY).state == "42"
    assert hass.states.get("number.bridge_test_volume").state == "40"
    assert hass.states.get("switch.bridge_test_child_lock").state == "off"
    assert hass.states.get("time.bridge_test_dnd_start").state == "22:00:00"
    assert hass.states.get(_VACUUM).attributes["schedule"][0]["id"] == 1
    assert not hass.services.has_service(DOMAIN, SERVICE_READ_LOCAL_BATTERY)
    assert {post["method"] for post in candidate_bridge.posts} == {"get_properties"}

    await hass.services.async_call("vacuum", "start", {"entity_id": _VACUUM}, blocking=True)
    await hass.services.async_call("number", "set_value", {"entity_id": "number.bridge_test_volume", "value": 55}, blocking=True)
    assert [(post["method"], post["params"]) for post in candidate_bridge.posts[-3:]] == [
        ("action", {"did": "2.1", "siid": 2, "aiid": 1, "in": []}),
        ("set_properties", [{"did": "7.1", "siid": 7, "piid": 1, "value": 55}]),
        ("action", {"did": "7.2", "siid": 7, "aiid": 2, "in": []}),
    ]
    assert hass.states.get("number.bridge_test_volume").state == "40"
    candidate_bridge.reported["91"] = 55
    coordinator = hass.data[DOMAIN][entry.entry_id]
    for _ in range(5):
        await _refresh(hass, coordinator)
    assert hass.states.get("number.bridge_test_volume").state == "55"

    flow = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_RECONFIGURE, "entry_id": entry.entry_id}
    )
    await _configure(hass, candidate_bridge, flow["flow_id"], bridge_read_mode=READ_MODE_LEGACY)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert registry.async_get(_VACUUM).id == vacuum_id
    assert registry.async_get(_BATTERY).id == battery_id
    assert registry.async_get(retired.entity_id).disabled_by is None
    assert registry.async_get(user_disabled.entity_id).disabled_by is er.RegistryEntryDisabler.USER
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_candidate_reconnect_field_loss_and_ambiguous_mutation(hass, candidate_bridge):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    candidate_bridge.rpc_error = "ambiguous"
    with pytest.raises(HomeAssistantError, match="outcome unknown"):
        await hass.services.async_call("vacuum", "return_to_base", {"entity_id": _VACUUM}, blocking=True)
    assert len([post for post in candidate_bridge.posts if post["method"] == "action"]) == 1

    candidate_bridge.epoch = None
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == STATE_UNAVAILABLE
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE

    candidate_bridge.epoch = _NEW_EPOCH
    candidate_bridge.reported.pop("91")
    candidate_bridge.reported["2"] = 0
    candidate_bridge.rpc_error = None
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == "0"
    assert hass.states.get("number.bridge_test_volume").state == STATE_UNAVAILABLE
    assert len([post for post in candidate_bridge.posts if post["method"] == "action"]) == 1
    assert all(post["method"] == "get_properties" for post in candidate_bridge.posts
               if post["epoch"] == _NEW_EPOCH)
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize(
    ("service", "fields", "mode", "target_key", "rows"),
    [
        ("vacuum_clean_segment", {"segments": [1, 3], "repeats": [1, 2]}, 18, "selects",
         [[1, 1, 1, 1, 1], [3, 2, 1, 1, 2]]),
        ("vacuum_clean_zone", {"zone": [0, 0, 500, 500], "repeats": 2}, 19, "areas",
         [[0, 0, 500, 500, 2, 1, 1]]),
        ("vacuum_clean_spot", {"points": [[150, 250]], "repeats": 1}, 20, "points",
         [[150, 250, 1, 1, 1]]),
    ],
)
async def test_candidate_native_parameterized_cleaning(hass, candidate_bridge, service, fields, mode, target_key, rows):
    entry, _ = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    await hass.services.async_call(DOMAIN, service, {"entity_id": _VACUUM, **fields}, blocking=True)
    post = candidate_bridge.posts[-1]
    assert post["method"] == "action"
    assert post["params"] == {
        "did": "4.1", "siid": 4, "aiid": 1,
        "in": [{"piid": 1, "value": mode}, {"piid": 10, "value": json.dumps({target_key: rows}, separators=(",", ":"))}],
    }
    assert hass.states.get(_VACUUM).state == "idle"
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_candidate_dnd_and_schedules_use_native_controls(hass, candidate_bridge):
    entry, _ = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    time_id = "time.bridge_test_dnd_start"
    await hass.services.async_call("time", "set_value", {"entity_id": time_id, "time": "21:30:00"}, blocking=True)
    assert candidate_bridge.posts[-1]["params"] == [{"did": "5.2", "siid": 5, "piid": 2, "value": "21:30"}]
    await hass.services.async_call(DOMAIN, "vacuum_delete_scheduled_task", {
        "entity_id": _VACUUM, "schedule_id": 1,
    }, blocking=True)
    assert candidate_bridge.posts[-1]["params"] == {
        "did": "8.1", "siid": 8, "aiid": 1, "in": [{"piid": 3, "value": 1}],
    }
    schedule = "1-0-08:30-1111111-1-0-1-1-0"
    await hass.services.async_call(DOMAIN, "vacuum_set_schedule", {
        "entity_id": _VACUUM, "schedule": schedule,
    }, blocking=True)
    assert candidate_bridge.posts[-1]["params"] == [{"did": "8.2", "siid": 8, "piid": 2, "value": schedule}]
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_candidate_push_and_ordered_map_reports_reach_native_entities(hass, candidate_bridge):
    from test_local_maps import frame

    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    map_entity = "camera.bridge_test_map"
    assert hass.states.get(map_entity).state == STATE_UNAVAILABLE

    candidate_bridge.sequence = 1
    candidate_bridge.push = [_property("2", 3, 39)]
    candidate_bridge.map_reports = [{
        "sequence": 1,
        "properties": [_property("87", 6, f"0,{frame()}", piid=13)],
    }]
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == "39"
    assert coordinator.device.status.current_map.map_id == 7
    assert hass.states.get(map_entity).state != STATE_UNAVAILABLE
    assert {post["method"] for post in candidate_bridge.posts} == {"get_properties"}
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_candidate_push_cache_does_not_refresh_unrelated_stale_fields(hass, candidate_bridge):
    candidate_bridge.sequence = 1
    candidate_bridge.push = [_property("2", 3, 42)]
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    assert hass.states.get(_BATTERY).state == "42"

    # Keep the session alive while reads fail and only an unrelated property changes.
    coordinator._seen["2"] = hass.loop.time() - 26
    candidate_bridge.fail_reads = True
    candidate_bridge.sequence = 2
    candidate_bridge.push.append(_property("0", 2, 2))
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == STATE_UNAVAILABLE
    assert hass.states.get(_VACUUM).state == "idle"
    assert {post["method"] for post in candidate_bridge.posts} == {"get_properties"}
    assert await hass.config_entries.async_unload(entry.entry_id)


def _candidate_map(bridge, sequence, position):
    from test_local_maps import frame, NOW_MS

    bridge.sequence = sequence
    bridge.map_reports = [{
        "sequence": sequence,
        "properties": [_property(
            "87", 6, f"0,{frame(sequence, timestamp=NOW_MS + sequence * 1000, robot=position)}", piid=13,
        )],
    }]


async def test_candidate_goto_stops_on_new_target_frame_and_restores_owned_setting(hass, candidate_bridge):
    _candidate_map(candidate_bridge, 1, (50, 50))
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    await hass.services.async_call(
        DOMAIN, "vacuum_goto", {"entity_id": _VACUUM, "x": 250, "y": 250}, blocking=True,
    )
    assert [post["params"] for post in candidate_bridge.posts if post["method"] == "action"] == [{
        "did": "4.1", "siid": 4, "aiid": 1, "in": [
            {"piid": 1, "value": 19}, {"piid": 10, "value": '{"areas":[[200,200,300,300,1,0,1]]}'},
        ],
    }]
    assert coordinator.device.status.go_to_zone is None
    candidate_bridge.reported.update({"11": 2, "5": 19, "0": 1, "27": 1})
    _candidate_map(candidate_bridge, 2, (250, 250))
    await _refresh(hass, coordinator)
    async with timeout(2):
        while coordinator._goto is not None:
            await sleep(0)
    mutations = [post["params"] for post in candidate_bridge.posts if post["method"] != "get_properties"]
    assert mutations[-2:] == [
        {"did": "4.2", "siid": 4, "aiid": 2, "in": []},
        [{"did": "4.23", "siid": 4, "piid": 23, "value": 0}],
    ]
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize("interrupt", ["pause", "reconnect"])
async def test_candidate_goto_continuation_cancels_on_new_motion_or_epoch(hass, candidate_bridge, interrupt):
    _candidate_map(candidate_bridge, 1, (50, 50))
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    await hass.services.async_call(
        DOMAIN, "vacuum_goto", {"entity_id": _VACUUM, "x": 250, "y": 250}, blocking=True,
    )
    if interrupt == "pause":
        await hass.services.async_call("vacuum", "pause", {"entity_id": _VACUUM}, blocking=True)
    else:
        candidate_bridge.epoch = _NEW_EPOCH
    candidate_bridge.reported.update({"11": 2, "5": 19, "0": 1, "27": 1})
    _candidate_map(candidate_bridge, 2, (250, 250))
    await _refresh(hass, coordinator)
    assert coordinator._goto is None
    assert not any(post["method"] == "action" and post["params"]["did"] == "4.2"
                   for post in candidate_bridge.posts)
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_candidate_goto_respects_later_explicit_setting_attempt(hass, candidate_bridge):
    _candidate_map(candidate_bridge, 1, (50, 50))
    candidate_bridge.reported["9"] = 2
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    await hass.services.async_call(
        DOMAIN, "vacuum_goto", {"entity_id": _VACUUM, "x": 250, "y": 250}, blocking=True,
    )
    assert coordinator._goto.restore[DreameVacuumProperty.WATER_VOLUME] == (2, 1)
    candidate_bridge.rpc_error = "ambiguous"
    with pytest.raises(HomeAssistantError, match="outcome unknown"):
        await coordinator.async_execute_command(coordinator.device.set_property, DreameVacuumProperty.WATER_VOLUME, 1)
    assert DreameVacuumProperty.WATER_VOLUME not in coordinator._goto.restore
    candidate_bridge.rpc_error = None
    candidate_bridge.reported.update({"11": 2, "5": 19, "0": 1, "27": 1, "9": 1})
    _candidate_map(candidate_bridge, 2, (250, 250))
    await _refresh(hass, coordinator)
    async with timeout(2):
        while coordinator._goto is not None:
            await sleep(0)
    assert candidate_bridge.posts[-1]["params"] == [{"did": "4.23", "siid": 4, "piid": 23, "value": 0}]
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize(
    ("reported", "enabled", "observed"),
    [
        (2, 34, 34),
        ('{"obstacle_detect_switch":true,"obstacle_app_display_switch":false}',
         '{"obstacle_app_display_switch":true}',
         '{"obstacle_detect_switch":true,"obstacle_app_display_switch":true}'),
    ],
)
async def test_candidate_obstacle_upload_switch_is_explicit_and_uses_reported_format(
    hass, candidate_bridge, reported, enabled, observed,
):
    candidate_bridge.reported["26"] = reported
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    picture = "switch.bridge_test_ai_obstacle_picture"
    upload = "switch.bridge_test_ai_obstacle_image_upload"
    detection = "switch.bridge_test_ai_obstacle_detection"
    assert hass.states.get(detection).state == "on"
    assert hass.states.get(upload).state == "off"
    assert (hass.states.get(picture) is not None) == (type(reported) is int)
    assert not any(post["method"] != "get_properties" for post in candidate_bridge.posts)

    await hass.services.async_call("switch", "turn_on", {"entity_id": upload}, blocking=True)
    assert hass.states.get(upload).state == "off"
    assert candidate_bridge.posts[-1]["params"] == [{"did": "4.22", "siid": 4, "piid": 22, "value": enabled}]

    candidate_bridge.reported["26"] = observed
    await _refresh(hass, coordinator)
    assert hass.states.get(upload).state == "on"
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_candidate_picture_bit_and_reported_ai_eligibility(hass, candidate_bridge):
    candidate_bridge.reported["26"] = '{"obstacle_detect_switch":true}'
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    picture = "switch.bridge_test_ai_obstacle_picture"
    upload = "switch.bridge_test_ai_obstacle_image_upload"
    assert hass.states.get(picture) is None
    assert hass.states.get(upload) is None

    candidate_bridge.sequence = 1
    candidate_bridge.push = [_property("26", 4, 2, piid=22)]
    await _refresh(hass, coordinator)
    assert hass.states.get(picture).state == "off"
    await hass.services.async_call("switch", "turn_on", {"entity_id": picture}, blocking=True)
    assert candidate_bridge.posts[-1]["params"] == [{"did": "4.22", "siid": 4, "piid": 22, "value": 6}]
    assert await hass.config_entries.async_unload(entry.entry_id)
