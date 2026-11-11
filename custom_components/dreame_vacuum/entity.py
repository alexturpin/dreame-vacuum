from __future__ import annotations

from typing import Any, Dict
from dataclasses import dataclass
from collections.abc import Callable
from functools import partial

from homeassistant.core import callback
from homeassistant.const import EVENT_CORE_CONFIG_UPDATE
from homeassistant.helpers.device_registry import CONNECTION_NETWORK_MAC
from homeassistant.helpers import entity_registry
from homeassistant.helpers.entity import DeviceInfo, Entity
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.entity import async_generate_entity_id

from .coordinator import DreameVacuumDataUpdateCoordinator
from .const import DOMAIN, LOGGER
from .dreame import (
    ATTR_VALUE,
    SEGMENT_TYPE_CODE_TO_NAME,
    DeviceUnavailableException,
    DreameVacuumDevice,
    DreameVacuumProperty,
    DreameVacuumAutoSwitchProperty,
    DreameVacuumStrAIProperty,
    DreameVacuumAIProperty,
    DreameVacuumAction,
    DeviceException,
    InvalidActionException,
    PROPERTY_AVAILABILITY,
    ACTION_AVAILABILITY,
)


def setup_native_entities(hass, entry, coordinator, domain, descriptions, entity_type, async_add_entities):
    """Discover supported candidate entities without removing stale registry IDs."""
    if not coordinator.device._local_candidate:
        remove_entities(hass, entry, coordinator, domain, descriptions)
        async_add_entities(
            entity_type(coordinator, description)
            for description in descriptions
            if description.exists_fn(description, coordinator.device)
        )
        return

    added = set()

    @callback
    def discover():
        device = coordinator.device
        if not device.capability.loaded:
            return
        entities = []
        for index, description in enumerate(descriptions):
            if (
                index in added
                or not device._protocol.supports_entity(domain, description)
                or not description.exists_fn(description, device)
                or not candidate_field_present(device, description)
            ):
                continue
            entities.append(entity_type(coordinator, description))
            added.add(index)
        if entities:
            async_add_entities(entities)

    entry.async_on_unload(coordinator.async_add_listener(discover))
    discover()


_CANDIDATE_ACTION_FIELDS = {
    DreameVacuumAction.RESET_MAIN_BRUSH: DreameVacuumProperty.MAIN_BRUSH_LEFT,
    DreameVacuumAction.RESET_SIDE_BRUSH: DreameVacuumProperty.SIDE_BRUSH_LEFT,
    DreameVacuumAction.RESET_FILTER: DreameVacuumProperty.FILTER_LEFT,
    DreameVacuumAction.RESET_SENSOR: DreameVacuumProperty.SENSOR_DIRTY_LEFT,
    DreameVacuumAction.RESET_MOP_PAD: DreameVacuumProperty.MOP_PAD_LEFT,
    DreameVacuumAction.CLEAR_WARNING: DreameVacuumProperty.ERROR,
}


def candidate_field_present(device, description) -> bool:
    """Return whether the current snapshot backs this entity's value."""
    key = description.key
    if key in ("dnd", "dnd_start", "dnd_end") and device.capability.dnd_task:
        prop = DreameVacuumProperty.DND_TASK
    else:
        prop = description.property_key
    if prop is None and description.action_key is not None:
        prop = _CANDIDATE_ACTION_FIELDS.get(description.action_key)
    if prop is None:
        prop = {
            "dnd": DreameVacuumProperty.DND,
            "dnd_start": DreameVacuumProperty.DND_START,
            "dnd_end": DreameVacuumProperty.DND_END,
            "off_peak_charging": DreameVacuumProperty.OFF_PEAK_CHARGING,
            "off_peak_charging_start": DreameVacuumProperty.OFF_PEAK_CHARGING,
            "off_peak_charging_end": DreameVacuumProperty.OFF_PEAK_CHARGING,
            "charging_state": DreameVacuumProperty.CHARGING_STATUS,
            "problem": DreameVacuumProperty.ERROR,
        }.get(key)
    if key == "current_room":
        return device.status.current_room is not None
    if key == "maintenance":
        return any(device.get_property(item) is not None for item in (
            DreameVacuumProperty.MAIN_BRUSH_LEFT, DreameVacuumProperty.SIDE_BRUSH_LEFT,
            DreameVacuumProperty.FILTER_LEFT, DreameVacuumProperty.SENSOR_DIRTY_LEFT, DreameVacuumProperty.MOP_PAD_LEFT,
        ))
    return prop is None or device.get_property(prop) is not None


def remove_entities(hass, entry, coordinator, domain, descriptions):
    """Remove entities from registry that are no longer provided by the integration"""

    registry = entity_registry.async_get(hass)
    for entry in entity_registry.async_entries_for_config_entry(registry, entry.entry_id):
        entity_id = entry.entity_id
        if (
            entity_id.startswith(f"{domain}.")
            and ("_map_" not in entity_id or domain == "button")
            and "_room_" not in entity_id
            and "_shortcut_" not in entity_id
        ):
            if entry.translation_key is not None:
                found = False
                for description in descriptions:
                    key = description.key
                    if key is None and description.property_key is not None:
                        key = description.property_key.name.lower()
                    if key is None and description.action_key is not None:
                        key = description.action_key.name.lower()
                    if key is None and description.name is not None:
                        key = description.name.lower().replace(" ", "_")

                    if key == entry.translation_key:
                        found = description.exists_fn(description, coordinator.device)
                        break

                if not found:
                    registry.async_remove(entity_id)


@dataclass
class DreameVacuumEntityDescription:
    key: str = None
    name: str = None
    entity_category: str = None
    property_key: DreameVacuumProperty = None
    action_key: DreameVacuumAction = None
    exists_fn: Callable[[object, object], bool] = lambda description, device: bool(
        (description.action_key is not None and description.action_key in device.action_mapping)
        or description.property_key is None
        or (
            isinstance(description.property_key, DreameVacuumProperty)
            and description.property_key.value in device.data
        )
        or (
            isinstance(description.property_key, DreameVacuumAutoSwitchProperty)
            and device.auto_switch_data
            and description.property_key.name in device.auto_switch_data
        )
        or (
            (
                isinstance(description.property_key, DreameVacuumStrAIProperty)
                or isinstance(description.property_key, DreameVacuumAIProperty)
            )
            and device.ai_data
            and description.property_key.name in device.ai_data
        )
    )
    value_fn: Callable[[object, object], Any] = None
    value_int_fn: Callable[[object, str], int] = None
    format_fn: Callable[[str, object], Any] = None
    available_fn: Callable[[object], bool] = None
    icon_fn: Callable[[str, object], str] = None
    attrs_fn: Callable[[object, Dict]] = None


class DreameVacuumEntity(CoordinatorEntity[DreameVacuumDataUpdateCoordinator]):
    """Defines a base Dreame Vacuum entity."""

    _attr_has_entity_name = True
    _name_placeholder = hasattr(Entity, "_attr_translation_placeholders")

    def __init__(
        self,
        coordinator: DreameVacuumDataUpdateCoordinator,
        description: DreameVacuumEntityDescription = None,
    ) -> None:
        if description is not None:
            if description.key is None:
                if description.property_key is not None:
                    description.key = description.property_key.name.lower()
                elif description.action_key is not None:
                    description.key = description.action_key.name.lower()

            if description.name is None and description.key is not None:
                description.name = description.key.replace("_", " ").title()
            elif description.key is None and description.name is not None:
                description.key = description.name.lower().replace(" ", "_").replace("-", "_")

            if description.value_fn is None and (description.property_key is not None or description.key is not None):
                if description.property_key is not None:
                    prop = description.property_key.name.lower()
                else:
                    prop = description.key.lower()
                if hasattr(coordinator.device.status, prop):
                    description.value_fn = lambda value, entity: getattr(entity.device.status, prop)

            if description.available_fn is None:
                if description.property_key is not None:
                    description.available_fn = PROPERTY_AVAILABILITY.get(description.property_key.name)
                elif description.action_key is not None:
                    description.available_fn = ACTION_AVAILABILITY.get(description.action_key.name)
                elif description.key is not None:
                    if description.key in PROPERTY_AVAILABILITY:
                        description.available_fn = PROPERTY_AVAILABILITY[description.key]
                    elif description.key in ACTION_AVAILABILITY:
                        description.available_fn = ACTION_AVAILABILITY[description.key]

        super().__init__(coordinator=coordinator)
        if description:
            if description.key is not None:
                self._attr_translation_key = description.key
            self.entity_description = description
            self._set_id()
            self._attr_unique_id = f"{self.device.mac}_{self.entity_description.key}"

    def _set_id(self) -> None:
        if self.entity_description:
            if self.entity_description.icon_fn is not None:
                self._attr_icon = self.entity_description.icon_fn(self.native_value, self.device)

    def _generate_entity_id(self, format) -> None:
        if self.entity_description.key:
            self.entity_id = async_generate_entity_id(
                format, f"{self.device.name}_{self.entity_description.key}", hass=self.coordinator.hass
            )

    @callback
    def _handle_coordinator_update(self) -> None:
        self._set_id()
        self.async_write_ha_state()

    def _localize_entity(self, domain: str, name: str, default: str = None) -> str:
        return self._localize(f"entity.{domain}.{name}.name", default)

    def _localized_name(self, domain: str, key: str, default: str, **placeholders) -> str:
        name = self._localize_entity(domain, key, default)
        for item, value in placeholders.items():
            name = name.replace("{" + item + "}", str(value))
        return name

    def _localize_entity_state(self, domain: str, name: str, key: str, default: str = None) -> str:
        return self._localize(f"entity.{domain}.{name}.state.{key}", default)

    def _localize_entity_component(self, name: str, default: str = None) -> str:
        return self._localize(f"entity_component.frontend_title.state.{name}", default)

    def _localize(self, path: str, default: str = None) -> str:
        return self.coordinator.localize(path, default)

    def _localize_segment_name(self, segment, segment_id) -> str:
        if segment:
            if segment.custom_name:
                return segment.custom_name
            type_name = SEGMENT_TYPE_CODE_TO_NAME.get(segment.type) if segment.type else None
            if type_name:
                key = type_name.lower().replace(" ", "_")
                name = self._localize_entity_state("select", "segment_name", key, type_name).title()
                if segment.index:
                    name = f"{name} {segment.index + 1}"
                return name
            template = self._localize_entity_component("segment_name_placeholder", "Room %index%")
            return template.replace("%index%", str(segment_id))
        template = self._localize_entity_component("segment_unavailable", "Room %index% Unavailable")
        return template.replace("%index%", str(segment_id))

    def _localize_map_name(self, map_data) -> str:
        if map_data is None:
            return None
        if map_data.custom_name:
            return map_data.custom_name
        template = self._localize_entity_component("map_name_placeholder", "Map %index%")
        return template.replace("%index%", str(map_data.map_index))

    def _raise_error(self, exc: Exception) -> None:
        self.coordinator.raise_error(exc)

    async def _try_command(self, mask_error, func, *args, **kwargs) -> bool:
        """Call a vacuum command handling error messages."""
        if not self.device.device_connected:
            self._raise_error(DeviceUnavailableException())

        try:
            if self.device._local_candidate:
                await self.coordinator.async_execute_command(func, *args, **kwargs)
            else:
                await self.hass.async_add_executor_job(partial(func, *args, **kwargs))
            return True
        except InvalidActionException as exc:
            LOGGER.error(mask_error, exc)
            self._raise_error(exc)
        except DeviceException as exc:
            if self.device.available:
                self._raise_error(exc)
            return False

    def _on_locale_changed(self):
        self._set_id()
        self.__dict__.pop("name", None)
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()

        async def on_config_update(event) -> None:
            if "language" not in event.data:
                return
            await self.coordinator.async_load_locale(self.hass)
            self._on_locale_changed()

        self.async_on_remove(self.hass.bus.async_listen(EVENT_CORE_CONFIG_UPDATE, on_config_update))

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information about this Dreame Vacuum device."""
        if self.device._local_candidate:
            info = self.device.info
            versions = {}
            if info.firmware_version is not None:
                versions["sw_version"] = info.firmware_version
            if info.hardware_version is not None:
                versions["hw_version"] = info.hardware_version
            return DeviceInfo(
                identifiers={(DOMAIN, self.device.mac)},
                name=self.device.name,
                manufacturer="Dreame",
                model=info.model,
                **versions,
            )
        if self.device.info:
            return DeviceInfo(
                connections={(CONNECTION_NETWORK_MAC, self.device.info.mac_address or self.device.mac)},
                identifiers={(DOMAIN, self.device.mac)},
                name=self.device.name,
                serial_number=self.device.status.serial_number if self.device.status else None,
                manufacturer=self.device.info.manufacturer,
                model=self.device.info.model,
                sw_version=self.device.info.firmware_version,
                hw_version=self.device.info.hardware_version,
            )

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        if not self.device.device_connected:
            return False

        if self.device._local_candidate and not candidate_field_present(self.device, self.entity_description):
            return False
        if self.entity_description.available_fn is not None:
            return self.entity_description.available_fn(self.device)
        return self._attr_available

    @property
    def native_value(self) -> Any:
        """Return the native value of the entity."""
        value = None
        if self.device._local_candidate and not candidate_field_present(self.device, self.entity_description):
            return None
        if self.entity_description.property_key is not None:
            value = self.device.get_property(self.entity_description.property_key)
        if self.entity_description.value_fn is not None:
            return self.entity_description.value_fn(value, self)
        return value

    @property
    def extra_state_attributes(self) -> dict[str, str] | None:
        """Return the extra state attributes of the entity."""
        attrs = None
        if self.device._local_candidate and not candidate_field_present(self.device, self.entity_description):
            return None
        if self.entity_description.attrs_fn is not None:
            attrs = self.entity_description.attrs_fn(self)
        elif self.entity_description.value_fn is not None or self.entity_description.value_int_fn is not None:
            if self.entity_description.property_key is not None:
                attrs = {ATTR_VALUE: self.device.get_property(self.entity_description.property_key)}
            elif self.entity_description.value_int_fn is not None:
                attrs = {ATTR_VALUE: self.entity_description.value_int_fn(self.native_value, self)}
        return attrs

    @property
    def device(self) -> DreameVacuumDevice:
        return self.coordinator.device
