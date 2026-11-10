"""The Dreame Vacuum component."""

from __future__ import annotations
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.typing import ConfigType
import logging
import warnings
from importlib import import_module
from .const import DOMAIN

## Dynamically load frontend.py so that it can be easily stripped
try:
    frontend = import_module(f"{__package__}.frontend")
except ModuleNotFoundError as ex:
    if ex.name != f"{__package__}.frontend":
        raise
    frontend = None

warnings.filterwarnings("ignore", category=Warning, module="miio")

## Info logs during init are needed for debugging, suppress the HA warning instead of logging less
logging.getLogger("homeassistant.util.logging").addFilter(
    lambda record: not (
        isinstance(record.args, tuple)
        and record.args
        and "logging too frequently" in str(record.msg)
        and str(record.args[0]).startswith(__package__)
    )
)

from .coordinator import DreameVacuumDataUpdateCoordinator
from .local_bridge import (
    SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN,
    SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN,
    SERVICE_READ_LOCAL_BATTERY,
    SERVICE_READ_LOCAL_PROPERTY,
    SERVICE_REFRESH_LOCAL_STATE,
    SERVICE_START_LOCAL_STATE_REFRESH_RUN,
    LocalBridgeCoordinator,
    async_register_local_read,
    is_local_bridge,
    is_local_candidate,
)

PLATFORMS = (
    Platform.VACUUM,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.SWITCH,
    Platform.BUTTON,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.CAMERA,
    Platform.TIME,
)


LOCAL_BRIDGE_PLATFORMS = (Platform.VACUUM, Platform.SENSOR, Platform.BUTTON)


CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the Dreame Vacuum integration."""
    if frontend is not None and any(
        not is_local_bridge(entry.data) for entry in hass.config_entries.async_entries(DOMAIN)
    ):
        await frontend.setup(hass)
    return True


def _archive_poc_entities(hass: HomeAssistant, entry: ConfigEntry, candidate: bool) -> None:
    """Retire PoC controls and snapshots while preserving their recorder IDs."""
    registry = er.async_get(hass)
    archived_keys = {"local_start", "local_pause", "local_dock", "local_state_refresh", "local_state_refresh_run"}
    unique_ids = {f"{entry.entry_id}_{key}" for key in archived_keys}
    for entity in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity.unique_id not in unique_ids:
            continue
        if candidate and entity.disabled_by is None:
            registry.async_update_entity(entity.entity_id, disabled_by=er.RegistryEntryDisabler.INTEGRATION)
        elif not candidate and entity.disabled_by is er.RegistryEntryDisabler.INTEGRATION:
            registry.async_update_entity(entity.entity_id, disabled_by=None)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Dreame Vacuum from a config entry."""

    if is_local_candidate(entry.data):
        from .local_candidate import LocalCandidateCoordinator

        coordinator = LocalCandidateCoordinator(hass, entry)
        try:
            await coordinator.async_config_entry_first_refresh()
            hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
            await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        except BaseException:
            if coordinator.device is not None:
                await coordinator.async_close()
            hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
            raise
        _archive_poc_entities(hass, entry, candidate=True)
        return True

    if is_local_bridge(entry.data):
        _archive_poc_entities(hass, entry, candidate=False)
        coordinator = LocalBridgeCoordinator(hass, entry)
        try:
            await coordinator.async_config_entry_first_refresh()
            hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
            await hass.config_entries.async_forward_entry_setups(entry, LOCAL_BRIDGE_PLATFORMS)
        except BaseException:
            await coordinator.async_close()
            hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
            raise
        async_register_local_read(hass)
        return True

    coordinator = DreameVacuumDataUpdateCoordinator(hass, entry=entry)

    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    # Set up all platforms for this device/entry.
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(update_listener))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload Dreame Vacuum config entry."""
    coordinators = hass.data[DOMAIN]
    coordinator = coordinators[entry.entry_id]
    if coordinator.device._local_candidate:
        if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
            return False
        coordinators.pop(entry.entry_id)
        await coordinator.async_close()
        return True

    if isinstance(coordinator, LocalBridgeCoordinator):
        coordinator.async_cancel_refresh()
        await coordinator.async_cancel_refresh_run("entry_unloaded")
        if not await hass.config_entries.async_unload_platforms(entry, LOCAL_BRIDGE_PLATFORMS):
            return False
        coordinators.pop(entry.entry_id)
        if not any(isinstance(item, LocalBridgeCoordinator) for item in coordinators.values()):
            hass.services.async_remove(DOMAIN, SERVICE_READ_LOCAL_BATTERY)
            hass.services.async_remove(DOMAIN, SERVICE_READ_LOCAL_PROPERTY)
            hass.services.async_remove(DOMAIN, SERVICE_REFRESH_LOCAL_STATE)
            hass.services.async_remove(DOMAIN, SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN)
            hass.services.async_remove(DOMAIN, SERVICE_START_LOCAL_STATE_REFRESH_RUN)
            hass.services.async_remove(DOMAIN, SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN)
        await coordinator.async_close()
        return True

    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        coordinator: DreameVacuumDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
        if coordinator._unsub_dispatcher:
            coordinator._unsub_dispatcher()
            coordinator._unsub_dispatcher = None
        coordinator._device.listen(None)
        coordinator._device.listen_error(None)
        try:
            await hass.async_add_executor_job(coordinator._device.disconnect)
        finally:
            coordinator._device = None
            hass.data[DOMAIN].pop(entry.entry_id, None)

    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle removal of a Dreame Vacuum config entry."""
    if frontend is not None and not is_local_bridge(entry.data):
        await frontend.remove(hass, entry)


async def update_listener(hass: HomeAssistant, config_entry: ConfigEntry) -> None:
    """Handle options update."""
    await hass.config_entries.async_reload(config_entry.entry_id)
