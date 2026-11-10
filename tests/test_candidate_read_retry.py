"""Verify candidate read recovery through the real HA entities and a loopback peer."""

from aiohttp import web
import pytest

from homeassistant.const import STATE_UNAVAILABLE

from custom_components.dreame_vacuum.dreame import DreameVacuumProperty as P

from test_local_bridge import (
    FakeCandidateBridge,
    _BATTERY,
    _VACUUM,
    _load_entry,
    _refresh,
    advance_loop,
    candidate_bridge,
    forbid_upstream_transports,
)


pytestmark = [pytest.mark.usefixtures("enable_custom_integrations"), pytest.mark.asyncio]


@pytest.mark.parametrize("outage_persists", [False, True])
async def test_candidate_read_retry_preserves_freshness_boundary(
    hass, candidate_bridge, advance_loop, outage_persists
):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    assert hass.states.get(_VACUUM).state == "idle"
    assert hass.states.get(_BATTERY).state == "42"

    # Drive explicit poll times so a delayed due poll and RPC can be reproduced
    # without sleeping through the actual 25-second freshness window.
    coordinator.update_interval = None
    coordinator._async_unsub_refresh()
    await advance_loop(15)
    candidate_bridge.fail_reads = True
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == "42"

    candidate_bridge.reported["2"] = 43
    candidate_bridge.fail_reads = outage_persists
    await advance_loop(5)
    await _refresh(hass, coordinator)
    assert hass.states.get(_BATTERY).state == ("42" if outage_persists else "43")
    for _ in range(2):
        await advance_loop(3)
        await _refresh(hass, coordinator)

    assert coordinator.protocol.epoch == candidate_bridge.epoch
    assert coordinator.last_update_success
    assert hass.states.get(_VACUUM).state == (STATE_UNAVAILABLE if outage_persists else "idle")
    assert hass.states.get(_BATTERY).state == (STATE_UNAVAILABLE if outage_persists else "43")

    candidate_bridge.fail_reads = False
    await advance_loop(3)
    await _refresh(hass, coordinator)
    assert hass.states.get(_VACUUM).state == "idle"
    assert hass.states.get(_BATTERY).state == "43"
    assert {post["method"] for post in candidate_bridge.posts} == {"get_properties"}
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.fixture
def reject_mop_consumable_batches(monkeypatch):
    rpc = FakeCandidateBridge.rpc

    async def rpc_with_firmware_rejection(self, request):
        data = await request.json()
        if data["method"] == "get_properties" and any(
            (item["siid"], item["piid"]) in {(18, 1), (18, 2)} for item in data["params"]
        ):
            # The r2250 returns a failure row without a piid for either mop
            # consumable. The owner rejects the uncorrelatable read reply.
            assert request.headers["Authorization"] == f"Bearer {self.token}"
            self.posts.append(data)
            identity = {key: data[key] for key in ("version", "epoch", "request_id")}
            return web.json_response(identity | {"error": "invalid_reply"})
        return await rpc(self, request)

    monkeypatch.setattr(FakeCandidateBridge, "rpc", rpc_with_firmware_rejection)


@pytest.mark.usefixtures("reject_mop_consumable_batches")
async def test_cleaning_totals_reach_native_entities(hass, candidate_bridge):
    candidate_bridge.reported.update({"125": 1710000000, "126": 76, "127": 2, "128": 61, "129": 0})
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")

    assert hass.states.get("sensor.bridge_test_total_cleaning_time").state == "76"
    assert hass.states.get("sensor.bridge_test_cleaning_count").state == "2"
    assert hass.states.get("sensor.bridge_test_total_cleaned_area").state == "61"
    assert hass.states.get("sensor.bridge_test_first_cleaning_date").state != STATE_UNAVAILABLE
    assert coordinator.device.get_property(P.TOTAL_RUNTIME) == 0
    assert await hass.config_entries.async_unload(entry.entry_id)
