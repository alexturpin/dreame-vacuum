"""Exercise the candidate update path using a loopback bridge and real HA coordinator."""

from collections import deque

from aiohttp import web
import pytest

from homeassistant.const import CONF_NAME
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.dreame_vacuum.const import DOMAIN
from custom_components.dreame_vacuum.local_candidate import LocalCandidateCoordinator


pytestmark = pytest.mark.asyncio
TOKEN = "t" * 32
EPOCH = "a" * 32


def state(epoch=None, value=86):
    return web.json_response({
        "version": 1, "epoch": epoch, "connected": bool(epoch), "ready": bool(epoch),
        "sequence": int(bool(epoch)),
        "properties": [{"did": "2", "siid": 3, "piid": 1, "code": 0, "value": value}] if epoch else [],
        "history": [], "map_reports": [],
    })


@pytest.fixture
async def bridge_coordinator(hass, aiohttp_server, socket_enabled):
    snapshots = deque()
    reads = []

    async def get_state(request):
        assert request.headers.get("Authorization") == f"Bearer {TOKEN}"
        return snapshots.popleft()

    async def read(request):
        assert request.headers.get("Authorization") == f"Bearer {TOKEN}"
        command = await request.json()
        assert command["method"] == "get_properties"
        reads.append(command)
        return web.json_response({
            "version": 1, "epoch": command["epoch"], "request_id": command["request_id"],
            "result": [{**row, "code": -4004} for row in command["params"] if row["did"] != "2"],
        })

    app = web.Application()
    app.router.add_get("/v1/candidate/state", get_state)
    app.router.add_post("/v1/rpc", read)
    server = await aiohttp_server(app)
    entry = MockConfigEntry(domain=DOMAIN, data={
        CONF_NAME: "Synthetic vacuum", "bridge_url": str(server.make_url("/").origin()), "bridge_token": TOKEN,
    })
    entry.add_to_hass(hass)
    coordinator = LocalCandidateCoordinator(hass, entry)
    try:
        yield coordinator, snapshots, reads
    finally:
        await coordinator.async_close()


@pytest.mark.parametrize("fail_after_read", [False, True])
async def test_relay_failure_clears_stale_state_and_reports_update_failure(bridge_coordinator, caplog, fail_after_read):
    coordinator, snapshots, reads = bridge_coordinator
    snapshots.extend([state(EPOCH), state(EPOCH)])
    await coordinator.async_refresh()
    assert coordinator.last_update_success
    assert coordinator.device.available
    assert coordinator.device.data[2] == 86
    assert coordinator.protocol.epoch == EPOCH

    reads.clear()
    if fail_after_read:
        coordinator._last_read.clear()
        snapshots.append(state(EPOCH, value=99))
    snapshots.append(web.json_response({"version": 1, "error": "owner_unavailable"}, status=503))
    await coordinator.async_refresh()
    assert bool(reads) == fail_after_read
    assert not snapshots
    assert not coordinator.last_update_success
    assert isinstance(coordinator.last_exception, UpdateFailed)
    assert "The owner relay is unavailable" in str(coordinator.last_exception)
    assert "The owner relay is unavailable" in caplog.text
    assert coordinator.protocol.epoch is None
    assert not coordinator.device.available
    assert coordinator.device.data == {}

    reads.clear()
    snapshots.append(state())
    await coordinator.async_refresh()
    assert coordinator.last_update_success
    assert not coordinator.device.available
    assert coordinator.device.data == {}
    assert reads == []


@pytest.mark.parametrize(("error", "message"), [
    ("owner_timeout", "The owner relay timed out"),
    ("invalid_owner_response", "The owner relay returned an invalid state reply"),
    ("busy", "A bridge state request is already in progress"),
    ("private diagnostic", "Invalid bridge service error"),
    ("private diagnostic" * 30, "Bridge response too large"),
])
async def test_state_error_diagnostics_are_bounded_and_allowlisted(bridge_coordinator, caplog, error, message):
    coordinator, snapshots, reads = bridge_coordinator
    snapshots.append(web.json_response({"version": 1, "error": error}, status=503))
    await coordinator.async_refresh()
    assert not coordinator.last_update_success
    assert isinstance(coordinator.last_exception, UpdateFailed)
    assert str(coordinator.last_exception) == f"Local bridge state unavailable. {message}"
    assert "private diagnostic" not in caplog.text
    assert reads == []
