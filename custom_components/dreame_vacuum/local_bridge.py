"""Read-only Home Assistant client for the local Dreame MQTT bridge."""

from __future__ import annotations

from asyncio import CancelledError, IncompleteReadError
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from ipaddress import ip_address, ip_network
import json
import re
from typing import Any, cast
from uuid import uuid4

from aiohttp import ClientError, ClientSession, ClientTimeout, Fingerprint
import voluptuous as vol
from yarl import URL

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.service import async_register_admin_service
from homeassistant.helpers.update_coordinator import CoordinatorEntity, DataUpdateCoordinator

from .const import CONF_ACCOUNT_TYPE, DOMAIN, LOGGER
from .dreame import DreameVacuumDevice, DreameVacuumProperty

ACCOUNT_TYPE_LOCAL_BRIDGE = "local_bridge"
CONF_BRIDGE_URL = "bridge_url"
CONF_BRIDGE_TOKEN = "bridge_token"
CONF_BRIDGE_FINGERPRINT = "bridge_fingerprint"
SERVICE_READ_LOCAL_BATTERY = "read_local_battery"
SERVICE_READ_LOCAL_PROPERTY = "read_local_property"
CONF_ENTRY_ID = "config_entry_id"
CONF_PROPERTY = "property"

_VERSION = 1
_STATE_TIMEOUT = 3
_READ_TIMEOUT = 5
_MAX_RESPONSE_BYTES = 4096
_MAX_SEQUENCE = (1 << 53) - 1
_HEX_ID = re.compile(r"[0-9a-f]{32}")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{32,128}")
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_PRIVATE_NETWORKS = tuple(
    ip_network(network) for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7")
)
_BATTERY = DreameVacuumProperty.BATTERY_LEVEL
_TELEMETRY = {
    (2, 1): DreameVacuumProperty.STATE,
    (3, 1): _BATTERY,
    (4, 1): DreameVacuumProperty.STATUS,
}
_BATTERY_ADDRESS = ("2", 3, 1)
_READ_PROPERTIES = {
    "state": ("0", 2, 1),
    "status": ("5", 4, 1),
    "charging_status": ("3", 3, 2),
    "task_status": ("11", 4, 7),
    "error": ("1", 2, 2),
}

_READ_ERRORS = {
    "busy": "A bridge read is already in progress",
    "invalid_request": "The bridge rejected the {label} request",
    "not_armed": "The bridge has no one-shot {label} authorization",
    "not_ready": "The bridge has no fresh telemetry",
    "property_error": "The robot reported a {label} error",
    "replay": "The bridge refused a duplicate request",
    "stale_epoch": "The bridge session changed before the {label} request",
    "timeout": "The robot {label} request timed out",
    "unavailable": "The robot {label} request is unavailable",
    "unsupported": "The bridge does not support this {label} request",
}


class LocalBridgeError(HomeAssistantError):
    """A failed or refused local bridge operation."""


def is_local_bridge(data: Mapping[str, Any]) -> bool:
    """Identify local bridge entries before touching legacy configuration."""
    return data.get(CONF_ACCOUNT_TYPE) == ACCOUNT_TYPE_LOCAL_BRIDGE


def validate_bridge_url(value: str) -> URL:
    """Accept only a private or loopback literal address and a bare HTTP origin."""
    url = URL(value.strip())
    try:
        address = ip_address(url.host or "")
        port = url.port
    except ValueError:
        raise ValueError("Invalid local bridge address") from None
    if (
        url.user is not None
        or url.password is not None
        or url.raw_path != "/"
        or url.query_string
        or url.fragment
        or port is None
        or not 0 < port <= 65535
        or (address.version == 6 and address.scope_id is not None)
    ):
        raise ValueError("Invalid local bridge origin")
    if address.is_loopback and url.scheme in ("http", "https"):
        return url.origin()
    if url.scheme == "https" and any(address in network for network in _PRIVATE_NETWORKS):
        return url.origin()
    raise ValueError("The bridge must use HTTPS on a private address or HTTP on loopback")


def _parse_property(item: Any) -> dict:
    if (
        not isinstance(item, dict)
        or item.keys() != {"did", "siid", "piid", "code", "value"}
        or any(type(item[field]) is not int for field in ("siid", "piid", "code", "value"))
        or item["code"] != 0
    ):
        raise LocalBridgeError("Invalid bridge property")
    return item


def _parse_properties(value: Any) -> dict[int, int]:
    if not isinstance(value, list) or len(value) > len(_TELEMETRY):
        raise LocalBridgeError("Invalid bridge properties")
    properties = {}
    for raw_item in value:
        item = _parse_property(raw_item)
        prop = _TELEMETRY.get((item["siid"], item["piid"]))
        bounds = (0, 100) if prop == _BATTERY else (-1, 65535)
        if (
            prop is None
            or item["did"] != str(prop.value)
            or prop.value in properties
            or not bounds[0] <= item["value"] <= bounds[1]
        ):
            raise LocalBridgeError("Invalid bridge property")
        properties[prop.value] = item["value"]
    return properties


def _unique_json_object(items: list[tuple[str, Any]]) -> dict:
    value = dict(items)
    if len(items) != len(value):
        raise ValueError("Duplicate bridge JSON key")
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError("Invalid bridge JSON number")


@dataclass(frozen=True)
class BridgeSnapshot:
    """Passive telemetry for the bridge's current robot session."""

    epoch: str | None
    sequence: int
    properties: dict[int, int]


class LocalBridgeClient:
    """Strict, bounded version 1 HTTP client."""

    def __init__(self, session: ClientSession, data: Mapping[str, Any]) -> None:
        if session.trust_env:
            raise ValueError("Local bridge connections cannot use environment proxies")
        self._session = session
        self._url = validate_bridge_url(data[CONF_BRIDGE_URL])
        token = data[CONF_BRIDGE_TOKEN]
        pin = data.get(CONF_BRIDGE_FINGERPRINT, "").strip().replace(":", "").lower()
        if not _TOKEN.fullmatch(token):
            raise ValueError("Invalid local bridge bearer token")
        if pin and (self._url.scheme != "https" or not _FINGERPRINT.fullmatch(pin)):
            raise ValueError("Invalid local bridge certificate fingerprint")
        self._ssl = Fingerprint(bytes.fromhex(pin)) if pin else True
        self._headers = {"Authorization": f"Bearer {token}", "Cache-Control": "no-store"}

    async def _request(
        self, method: str, path: str, timeout: int, payload: dict | None = None
    ) -> dict:
        try:
            async with self._session.request(
                method,
                self._url.with_path(path),
                headers=self._headers,
                ssl=self._ssl,
                allow_redirects=False,
                timeout=ClientTimeout(total=timeout),
                json=payload,
            ) as response:
                if response.status == 401:
                    raise LocalBridgeError("Bridge authentication failed")
                if response.status != 200 or response.content_type != "application/json":
                    raise LocalBridgeError("Invalid bridge HTTP response")
                try:
                    await response.content.readexactly(_MAX_RESPONSE_BYTES + 1)
                except IncompleteReadError as exc:
                    value = json.loads(
                        exc.partial, object_pairs_hook=_unique_json_object, parse_constant=_reject_json_constant
                    )
                else:
                    raise LocalBridgeError("Bridge response too large")
        except (ClientError, TimeoutError, ValueError, RecursionError):
            raise LocalBridgeError("Bridge request failed") from None
        if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != _VERSION:
            raise LocalBridgeError("Invalid bridge protocol version")
        return value

    async def async_get_state(self) -> BridgeSnapshot:
        """Read an authenticated snapshot without sending robot traffic."""
        data = await self._request("GET", "/v1/state", _STATE_TIMEOUT)
        if data.keys() != {"version", "epoch", "connected", "ready", "sequence", "properties"}:
            raise LocalBridgeError("Invalid bridge state envelope")
        epoch, connected, ready, sequence = (
            data["epoch"],
            data["connected"],
            data["ready"],
            data["sequence"],
        )
        properties = _parse_properties(data["properties"])
        if (
            type(connected) is not bool
            or type(ready) is not bool
            or type(sequence) is not int
            or not 0 <= sequence <= _MAX_SEQUENCE
        ):
            raise LocalBridgeError("Invalid bridge session state")
        if not connected:
            if epoch is not None or ready or sequence or properties:
                raise LocalBridgeError("Invalid disconnected bridge state")
        elif not isinstance(epoch, str) or not _HEX_ID.fullmatch(epoch):
            raise LocalBridgeError("Invalid bridge session epoch")
        if ready != bool(properties) or ready != bool(sequence):
            raise LocalBridgeError("Invalid bridge telemetry readiness")
        return BridgeSnapshot(epoch, sequence, properties)

    async def async_read_battery(self, epoch: str) -> int:
        """Make one explicitly requested battery read using the original contract."""
        result, _ = await self.async_read_property(epoch, _BATTERY_ADDRESS, None)
        return cast(int, result["value"])

    async def async_read_property(
        self, epoch: str, address: tuple[str, int, int], continuation: str | None
    ) -> tuple[dict[str, str | int], str | None]:
        """Make one explicitly requested, correlated property read."""
        did, siid, piid = address
        identity = {"did": did, "siid": siid, "piid": piid}
        battery = address == _BATTERY_ADDRESS
        label = "battery" if battery else "property"
        request_id = uuid4().hex
        payload = {
            "version": _VERSION,
            "request_id": request_id,
            "epoch": epoch,
            "method": "get_properties",
            "params": [identity],
            "timeout": _READ_TIMEOUT,
        }
        if not battery:
            payload["continuation"] = continuation
        response = await self._request("POST", "/v1/read", _READ_TIMEOUT + 1, payload)
        if response.get("request_id") != request_id or response.get("epoch") != epoch:
            raise LocalBridgeError("Bridge read response did not match the request")
        if response.keys() == {"version", "request_id", "epoch", "error"}:
            code = response["error"]
            if isinstance(code, str) and code in _READ_ERRORS:
                if code == "property_error" and battery:
                    label = "battery property"
                raise LocalBridgeError(_READ_ERRORS[code].format(label=label))
            raise LocalBridgeError("Unknown bridge read failure")
        expected_keys = {"version", "request_id", "epoch", "result"}
        if not battery:
            expected_keys.add("continuation")
        if response.keys() != expected_keys:
            raise LocalBridgeError("Invalid bridge read response")
        result = response["result"]
        if not isinstance(result, list) or len(result) != 1:
            raise LocalBridgeError(f"Invalid bridge {label} response")
        item = _parse_property(result[0])
        minimum, maximum = (0, 100) if battery else (-(1 << 31), (1 << 31) - 1)
        if (
            any(item[key] != value for key, value in identity.items())
            or not minimum <= item["value"] <= maximum
        ):
            raise LocalBridgeError(f"Invalid bridge {label} response")
        next_receipt = response.get("continuation")
        if not battery:
            if address == _READ_PROPERTIES["error"]:
                if next_receipt is not None:
                    raise LocalBridgeError("Invalid final bridge continuation receipt")
            elif (
                not isinstance(next_receipt, str)
                or not _HEX_ID.fullmatch(next_receipt)
                or next_receipt == continuation
            ):
                raise LocalBridgeError("Invalid bridge continuation receipt")
        return {**identity, "value": item["value"]}, next_receipt


class LocalBridgeCoordinator(DataUpdateCoordinator[DreameVacuumDevice]):
    """Apply only validated snapshots from the active bridge epoch."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self._bridge = LocalBridgeClient(async_get_clientsession(hass), entry.data)
        super().__init__(
            hass, LOGGER, config_entry=entry, name=DOMAIN, update_interval=timedelta(seconds=10)
        )
        self.device = DreameVacuumDevice(entry.data[CONF_NAME], None, None, local_bridge=True)
        self._busy = False
        self._epoch: str | None = None
        self._sequence = 0
        self._retired_epochs: deque[str] = deque(maxlen=32)
        self._read_continuation: str | None = None
        self._read_sealed = False
        self._closed = False

    def _seal_read_trial(self) -> None:
        self._read_continuation = None
        self._read_sealed = True

    def _invalidate_snapshot(self) -> None:
        self.device.apply_local_bridge_properties({})
        if self._read_continuation is not None:
            self._seal_read_trial()

    async def _async_apply_snapshot(self) -> None:
        try:
            state = await self._bridge.async_get_state()
            if self._closed or state.epoch is None:
                self._invalidate_snapshot()
                return
            if state.epoch in self._retired_epochs:
                raise LocalBridgeError("Bridge returned a retired session epoch")
            if state.epoch != self._epoch:
                if self._epoch is not None:
                    self._retired_epochs.append(self._epoch)
                self._epoch = state.epoch
                self._sequence = 0
                self._read_continuation = None
                self._read_sealed = False
                self.device.apply_local_bridge_properties({})
            if not state.properties:
                self._invalidate_snapshot()
                return
            if state.sequence < self._sequence:
                raise LocalBridgeError("Bridge telemetry sequence moved backwards")
            if state.sequence == self._sequence:
                if self.device.available and self.device.data != state.properties:
                    raise LocalBridgeError("Bridge telemetry changed without a new sequence")
                return
            self._sequence = state.sequence
            self.device.apply_local_bridge_properties(state.properties)
        except (CancelledError, LocalBridgeError):
            self._invalidate_snapshot()
            raise

    async def _async_update_data(self) -> DreameVacuumDevice:
        if self._busy or self._closed:
            return self.device
        self._busy = True
        try:
            await self._async_apply_snapshot()
        except LocalBridgeError as err:
            LOGGER.debug("Local bridge snapshot rejected: %s", err)
        finally:
            self._busy = False
        return self.device

    async def async_read_property(
        self, address: tuple[str, int, int]
    ) -> dict[str, str | int]:
        """Check current readiness and verify the session after the explicit read."""
        if self._closed:
            raise LocalBridgeError("The local bridge entry is unloaded")
        if self._busy:
            raise LocalBridgeError("The local bridge is busy")
        self._busy = True
        trial = address != _BATTERY_ADDRESS
        started = False
        try:
            await self._async_apply_snapshot()
            epoch = self._epoch
            if not self.device.available or epoch is None:
                raise LocalBridgeError("The local bridge has no fresh telemetry")
            if trial and self._read_sealed:
                raise LocalBridgeError("The property read trial ended for this bridge session")
            started = True
            try:
                result, continuation = await self._bridge.async_read_property(
                    epoch, address, self._read_continuation
                )
            except LocalBridgeError:
                if not self._closed:
                    try:
                        await self._async_apply_snapshot()
                    except LocalBridgeError:
                        pass  # The original read failure describes the action outcome.
                raise
            if not self._closed:
                await self._async_apply_snapshot()
            if self._closed or self._epoch != epoch or not self.device.available:
                label = "battery" if address == _BATTERY_ADDRESS else "property"
                raise LocalBridgeError(f"The bridge session changed during the {label} read")
            if trial:
                self._read_continuation = continuation
                self._read_sealed = continuation is None
            return result
        except BaseException:
            if trial and started and self._epoch == epoch:
                # A cancellation can arrive after the bridge has sent the read.
                self._seal_read_trial()
            raise
        finally:
            self._busy = False
            if not self._closed:
                self.async_set_updated_data(self.device)

    async def async_close(self) -> None:
        """Clear the device state and stop coordinator scheduling."""
        self._closed = True
        self._read_continuation = None
        self.device.disconnect()
        await self.async_shutdown()


class LocalBridgeEntity(CoordinatorEntity[LocalBridgeCoordinator]):
    """Shared identity and availability for passive bridge entities."""

    _attr_has_entity_name = True

    def __init__(self, coordinator: LocalBridgeCoordinator, key: str) -> None:
        super().__init__(coordinator)
        entry = coordinator.config_entry
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=coordinator.device.name,
            manufacturer="Dreame",
        )

    @property
    def available(self) -> bool:
        return self.coordinator.device.available


@callback
def async_register_local_read(hass: HomeAssistant) -> None:
    """Register opt-in reads for loaded local bridge entries."""
    if hass.services.has_service(DOMAIN, SERVICE_READ_LOCAL_BATTERY):
        return

    def get_coordinator(call: ServiceCall) -> LocalBridgeCoordinator:
        coordinator = hass.data[DOMAIN].get(call.data[CONF_ENTRY_ID])
        if not isinstance(coordinator, LocalBridgeCoordinator):
            raise LocalBridgeError("Select a loaded Local MQTT Bridge entry")
        return coordinator

    async def async_read_battery(call: ServiceCall) -> dict[str, int]:
        result = await get_coordinator(call).async_read_property(_BATTERY_ADDRESS)
        return {"battery_level": cast(int, result["value"])}

    async def async_read_property(call: ServiceCall) -> dict[str, str | int]:
        property_name = call.data[CONF_PROPERTY]
        result = await get_coordinator(call).async_read_property(_READ_PROPERTIES[property_name])
        return {"property": property_name, **result}

    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_READ_LOCAL_BATTERY,
        async_read_battery,
        schema=vol.Schema({vol.Required(CONF_ENTRY_ID): str}),
        supports_response=SupportsResponse.ONLY,
    )
    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_READ_LOCAL_PROPERTY,
        async_read_property,
        schema=vol.Schema(
            {
                vol.Required(CONF_ENTRY_ID): str,
                vol.Required(CONF_PROPERTY): vol.In(_READ_PROPERTIES),
            }
        ),
        supports_response=SupportsResponse.ONLY,
    )
