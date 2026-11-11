"""Verify passive device metadata through a loopback bridge and HA entities."""

import pytest

from homeassistant.const import STATE_UNKNOWN, STATE_UNAVAILABLE
from homeassistant.helpers import device_registry, entity_registry

from custom_components.dreame_vacuum.local_bridge import LocalBridgeError

from test_local_bridge import (
    _VACUUM,
    _load_entry,
    _refresh,
    candidate_bridge,
    forbid_upstream_transports,
)


pytestmark = [pytest.mark.usefixtures("enable_custom_integrations"), pytest.mark.asyncio]
FIRMWARE_SENSOR = "sensor.bridge_test_firmware_version"
METADATA = {
    "model": "dreame.vacuum.r2250",
    "fw_ver": "4.3.3_1021",
    "hw_ver": "Linux",
    "ap": {"ssid": "Test network", "rssi": -62},
    "netif": {"localIp": "192.0.2.7"},
}


def vacuum_registry_entry(hass):
    vacuum = entity_registry.async_get(hass).async_get(_VACUUM)
    return device_registry.async_get(hass).async_get(vacuum.device_id)


async def test_late_metadata_updates_vacuum_firmware_sensor_and_registry(hass, candidate_bridge):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    assert hass.states.get(FIRMWARE_SENSOR).state == STATE_UNKNOWN
    assert hass.states.get(_VACUUM).attributes["firmware_version"] is None
    assert hass.states.get(_VACUUM).attributes["ap"] is None
    registry_entry = vacuum_registry_entry(hass)
    assert registry_entry.sw_version is None
    assert registry_entry.hw_version is None
    assert coordinator.device.info.version == 0
    previous_capabilities = set(coordinator.device.capability.list)

    candidate_bridge.posts.clear()
    candidate_bridge.device_info = METADATA
    await _refresh(hass, coordinator)

    assert candidate_bridge.sequence == 0
    assert candidate_bridge.posts == []
    assert hass.states.get(FIRMWARE_SENSOR).state == "4.3.3_1021"
    vacuum_attrs = hass.states.get(_VACUUM).attributes
    assert vacuum_attrs["firmware_version"] == "4.3.3_1021"
    assert vacuum_attrs["ap"] == {"ssid": "Test network", "bssid": None, "rssi": -62, "ip": "192.0.2.7"}
    assert coordinator.device.info.version == 1021
    assert set(coordinator.device.capability.list) == previous_capabilities
    updated = vacuum_registry_entry(hass)
    assert updated.id == registry_entry.id
    assert updated.sw_version == "4.3.3_1021"
    assert updated.hw_version == "Linux"

    candidate_bridge.epoch = None
    candidate_bridge.device_info = {}
    await _refresh(hass, coordinator)

    assert hass.states.get(FIRMWARE_SENSOR).state == STATE_UNAVAILABLE
    assert coordinator.device.info.ap is None
    assert vacuum_registry_entry(hass).sw_version == "4.3.3_1021"
    assert candidate_bridge.posts == []
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize(("firmware", "build"), [("4.3.3_1021", 1021), ("4.3.3_preview", 0)])
async def test_metadata_present_at_setup(hass, candidate_bridge, firmware, build):
    candidate_bridge.device_info = {**METADATA, "fw_ver": firmware}
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    assert hass.states.get(FIRMWARE_SENSOR).state == firmware
    assert vacuum_registry_entry(hass).sw_version == firmware
    assert vacuum_registry_entry(hass).hw_version == "Linux"
    assert coordinator.device.info.version == build
    assert {"auto_rename_segment", "obstacles", "washless_base", "video_during_cleaning", "floor_material_edit"} <= set(
        coordinator.device.capability.list
    )
    assert {post["method"] for post in candidate_bridge.posts} == {"get_properties"}
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_partial_metadata_uses_configured_model_and_exposes_known_ip(hass, candidate_bridge):
    candidate_bridge.device_info = {"netif": {"localIp": "192.0.2.7"}}
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    assert coordinator.device.info.model == "dreame.vacuum.r2250"
    assert hass.states.get(FIRMWARE_SENSOR).state == STATE_UNKNOWN
    assert vacuum_registry_entry(hass).hw_version is None
    assert hass.states.get(_VACUUM).attributes["ap"] == {"ip": "192.0.2.7"}
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize("metadata", [
    [],
    {"model": "dreame.vacuum.other"},
    {"fw_ver": "x" * 65},
    {"fw_ver": "4.3.3_1021\n"},
    {"hw_ver": ""},
    {"token": "private"},
    {"ap": {"ssid": "é" * 17}},
    {"ap": {"rssi": True}},
    {"ap": {"rssi": -128}},
    {"ap": {"bssid": "private"}},
    {"netif": {"localIp": "::1"}},
    {"netif": {"localIp": "192.0.2.01"}},
])
async def test_rejects_invalid_device_info(hass, candidate_bridge, metadata):
    entry, coordinator = await _load_entry(hass, candidate_bridge, bridge_read_mode="local_candidate")
    candidate_bridge.device_info = metadata
    with pytest.raises(LocalBridgeError, match="^Invalid local device information$"):
        await coordinator.bridge.async_candidate_state()
    assert await hass.config_entries.async_unload(entry.entry_id)
