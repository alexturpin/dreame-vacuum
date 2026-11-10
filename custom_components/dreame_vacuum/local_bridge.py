"""Read-only Home Assistant client for the local Dreame MQTT bridge."""

from __future__ import annotations

from asyncio import (
    CancelledError,
    IncompleteReadError,
    Task,
    TimerHandle,
    current_task,
    gather,
    shield,
    sleep,
    timeout_at,
)
from collections import deque
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import timedelta
from hashlib import sha256
from ipaddress import ip_address, ip_network
import json
import re
from secrets import token_hex
from typing import Any, cast
from uuid import uuid4

from aiohttp import ClientError, ClientSession, ClientTimeout, Fingerprint
import voluptuous as vol
from yarl import URL

from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import CONF_NAME, EVENT_HOMEASSISTANT_STOP, STATE_UNKNOWN
from homeassistant.core import Event, HomeAssistant, ServiceCall, SupportsResponse, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.service import async_register_admin_service
from homeassistant.helpers.update_coordinator import CoordinatorEntity, DataUpdateCoordinator
from homeassistant.util import dt as dt_util
from homeassistant.util.hass_dict import HassKey

from .const import CONF_ACCOUNT_TYPE, DOMAIN, LOGGER
from .dreame import DreameVacuumDevice, DreameVacuumProperty
from .dreame.const import (
    CHARGING_STATUS_CODE_TO_NAME,
    ERROR_CODE_TO_ERROR_NAME,
    STATE_CODE_TO_STATE,
    STATUS_CODE_TO_NAME,
    TASK_STATUS_CODE_TO_NAME,
)
from .dreame.types import DreameVacuumState

ACCOUNT_TYPE_LOCAL_BRIDGE = "local_bridge"
CONF_BRIDGE_URL = "bridge_url"
CONF_BRIDGE_TOKEN = "bridge_token"
CONF_BRIDGE_FINGERPRINT = "bridge_fingerprint"
CONF_BRIDGE_READ_MODE = "bridge_read_mode"
READ_MODE_PASSIVE = "passive"
READ_MODE_LEGACY = "legacy"
READ_MODE_STATE_REFRESH = "state_refresh"
READ_MODE_STATE_REFRESH_RUN = "state_refresh_run"
READ_MODE_DURABLE_REFRESH = "durable_refresh"
READ_MODE_ON_DEMAND = "on_demand_refresh"
READ_MODE_CANDIDATE = "local_candidate"
_RUN_MODES = (READ_MODE_STATE_REFRESH_RUN, READ_MODE_DURABLE_REFRESH, READ_MODE_ON_DEMAND)
BRIDGE_READ_MODES = (
    READ_MODE_PASSIVE,
    READ_MODE_ON_DEMAND,
    READ_MODE_CANDIDATE,
    READ_MODE_DURABLE_REFRESH,
    READ_MODE_LEGACY,
    READ_MODE_STATE_REFRESH,
    READ_MODE_STATE_REFRESH_RUN,
)
SERVICE_READ_LOCAL_BATTERY = "read_local_battery"
SERVICE_READ_LOCAL_PROPERTY = "read_local_property"
SERVICE_REFRESH_LOCAL_STATE = "refresh_local_state"
SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN = "prepare_local_state_refresh_run"
SERVICE_START_LOCAL_STATE_REFRESH_RUN = "start_local_state_refresh_run"
SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN = "cancel_local_state_refresh_run"
CONF_ENTRY_ID = "config_entry_id"
CONF_PROPERTY = "property"

_VERSION = 1
_STATE_TIMEOUT = 3
_READ_TIMEOUT = 5
_REFRESH_TIMEOUT = 45
_REFRESH_TTL = 60
_REFRESH_ATTEMPTS: HassKey[set[str]] = HassKey(f"{DOMAIN}.local_refresh_attempts")
_RUN_ATTEMPTS: HassKey[set[str]] = HassKey(f"{DOMAIN}.local_refresh_run_attempts")
_RUN_VERSION = 2
_RUN_MAX_CYCLES = 3
_RUN_MAX_READS = 15
_RUN_MIN_INTERVAL = 120
_RUN_MAX_SECONDS = 600
_RUN_CONTROL_TIMEOUT = 3
_RUN_INTERVAL = 125
_RUN_WAKE_TOLERANCE = 5
_RUN_MAX_RESPONSE_BYTES = 2048
_MAX_RESPONSE_BYTES = 4096
_MAX_SERVICE_ERROR_BYTES = 256
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
_READ_LABELS = {
    "state": STATE_CODE_TO_STATE,
    "status": STATUS_CODE_TO_NAME,
    "charging_status": CHARGING_STATUS_CODE_TO_NAME,
    "task_status": TASK_STATUS_CODE_TO_NAME,
    "error": ERROR_CODE_TO_ERROR_NAME,
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


def is_local_candidate(data: Mapping[str, Any]) -> bool:
    """Identify entries explicitly opted into native local operation."""
    return is_local_bridge(data) and data.get(CONF_BRIDGE_READ_MODE) == READ_MODE_CANDIDATE


def bridge_state(value: int) -> DreameVacuumState | None:
    """Decode the local bridge's known state range."""
    if value == 0:
        return DreameVacuumState.CHARGING_COMPLETED
    if 1 <= value <= 18:
        return DreameVacuumState(value)
    return None


def bridge_property_label(property_name: str, value: int) -> str | None:
    """Translate a reported value without applying upstream derived status."""
    if property_name == "state":
        state = bridge_state(value)
        if state is None:
            return None
        value = state
    label = _READ_LABELS[property_name].get(value)
    return None if label == STATE_UNKNOWN else label


def _is_hex_id(value: Any) -> bool:
    return isinstance(value, str) and _HEX_ID.fullmatch(value) is not None


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
    """Strict, bounded HTTP client for the two bridge protocol versions."""

    def __init__(self, session: ClientSession, data: Mapping[str, Any]) -> None:
        if session.trust_env:
            raise ValueError("Local bridge connections cannot use environment proxies")
        self._session = session
        self._url = validate_bridge_url(data[CONF_BRIDGE_URL])
        self._read_mode = data.get(CONF_BRIDGE_READ_MODE, READ_MODE_PASSIVE)
        token = data[CONF_BRIDGE_TOKEN]
        pin = data.get(CONF_BRIDGE_FINGERPRINT, "").strip().replace(":", "").lower()
        if not _TOKEN.fullmatch(token):
            raise ValueError("Invalid local bridge bearer token")
        if pin and (self._url.scheme != "https" or not _FINGERPRINT.fullmatch(pin)):
            raise ValueError("Invalid local bridge certificate fingerprint")
        if self._read_mode in _RUN_MODES and not pin:
            raise ValueError("Local refresh runs require a pinned HTTPS bridge")
        self._ssl = Fingerprint(bytes.fromhex(pin)) if pin else True
        self._headers = {"Authorization": f"Bearer {token}", "Cache-Control": "no-store"}

    async def _request(
        self,
        method: str,
        path: str,
        timeout: float,
        payload: dict | None = None,
        *,
        version: int = _VERSION,
        max_bytes: int | None = None,
        query: dict | None = None,
        service_errors: Mapping[str, str] | None = None,
    ) -> dict:
        if max_bytes is None:
            max_bytes = _RUN_MAX_RESPONSE_BYTES if version == _RUN_VERSION else _MAX_RESPONSE_BYTES
        try:
            async with self._session.request(
                method,
                self._url.with_path(path).with_query(query),
                headers=self._headers,
                ssl=self._ssl,
                allow_redirects=False,
                timeout=ClientTimeout(total=timeout, ceil_threshold=float("inf")),
                json=payload,
                auto_decompress=False if version == _RUN_VERSION or service_errors is not None else None,
            ) as response:
                if response.status == 401:
                    raise LocalBridgeError("Bridge authentication failed")
                service_error = response.status == 503 and service_errors is not None
                if (response.status != 200 and not service_error) or response.content_type != "application/json":
                    raise LocalBridgeError("Invalid bridge HTTP response")
                limit = _MAX_SERVICE_ERROR_BYTES if service_error else max_bytes
                try:
                    await response.content.readexactly(limit + 1)
                except IncompleteReadError as exc:
                    value = json.loads(
                        exc.partial, object_pairs_hook=_unique_json_object, parse_constant=_reject_json_constant
                    )
                else:
                    raise LocalBridgeError("Bridge response too large")
        except (ClientError, TimeoutError, ValueError, RecursionError):
            raise LocalBridgeError("Bridge request failed") from None
        if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != version:
            raise LocalBridgeError("Invalid bridge protocol version")
        if service_error and service_errors is not None:
            error = value.get("error")
            if value.keys() == {"version", "error"} and type(error) is str and error in service_errors:
                raise LocalBridgeError(service_errors[error])
            raise LocalBridgeError("Invalid bridge service error")
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

    async def async_action(self, epoch: str, action: str) -> None:
        """One fixed movement command, with no retry or ancillary robot requests."""
        if action not in ("start", "pause", "dock") or not _is_hex_id(epoch):
            raise LocalBridgeError("Invalid local action")
        request = {"version": 1, "request_id": token_hex(16), "epoch": epoch, "action": action}
        reply = await self._request("POST", "/v1/action", 6, request)
        if reply.get("request_id") != request["request_id"] or reply.get("epoch") != epoch:
            raise LocalBridgeError("Action outcome unknown: response identity mismatch; do not retry automatically")
        if reply.keys() == {"version", "request_id", "epoch", "error"}:
            raise LocalBridgeError("Local action failed or outcome unknown; check the robot before another command")
        result = reply.get("result")
        if (reply.keys() != {"version", "request_id", "epoch", "result"} or type(result) is not dict
                or result.keys() != {"action", "code"} or result["action"] != action
                or type(result["code"]) is not int):
            raise LocalBridgeError("Action outcome unknown: invalid response")
        if result["code"] != 0:
            raise LocalBridgeError(f"Robot rejected {action} (code {result['code']})")

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

    async def _async_run_request(
        self, action: str, epoch: str, run_id: str, fields: dict, timeout: float
    ) -> dict:
        identity = {
            "version": _RUN_VERSION,
            "request_id": token_hex(16),
            "epoch": epoch,
            "run_id": run_id,
        }
        response = await self._request(
            "POST", f"/v2/refresh/{action}", timeout, identity | fields, version=_RUN_VERSION
        )
        if any(response.get(key) != value for key, value in identity.items()):
            raise LocalBridgeError("Bridge refresh run response did not match the request")
        if response.keys() == identity.keys() | {"error"}:
            code = response["error"]
            if isinstance(code, str) and code in _READ_ERRORS:
                if code == "not_armed":
                    raise LocalBridgeError("The bridge has no refresh run authorization")
                raise LocalBridgeError(_READ_ERRORS[code].format(label="refresh run"))
            raise LocalBridgeError("Unknown bridge refresh run failure")
        return {key: value for key, value in response.items() if key not in identity}

    async def async_claim_refresh_run(
        self, epoch: str, run_id: str, revoke_secret: str, timeout: float
    ) -> tuple[int, str]:
        """Exchange one claim for its initial cycle capability and grant lifetime."""
        mode = self._read_mode
        if mode not in _RUN_MODES:
            raise LocalBridgeError("The local bridge is not configured for a refresh run")
        response = await self._async_run_request(
            "claim", epoch, run_id, {"mode": mode, "revoke_secret": revoke_secret}, timeout
        )
        limits = {
            "max_cycles": 1 if self._read_mode == READ_MODE_ON_DEMAND else _RUN_MAX_CYCLES,
            "max_reads": 5 if self._read_mode == READ_MODE_ON_DEMAND else _RUN_MAX_READS,
            "min_cycle_interval_seconds": _RUN_MIN_INTERVAL,
            "cycle": 1,
        }
        if (
            response.keys() != limits.keys() | {"mode", "remaining_grant_ms", "cycle_capability"}
            or response["mode"] != mode
            or any(type(response[key]) is not int or response[key] != value for key, value in limits.items())
            or type(response["remaining_grant_ms"]) is not int
            or not 0 < response["remaining_grant_ms"] <= (60 if mode == READ_MODE_ON_DEMAND else _RUN_MAX_SECONDS) * 1000
            or not _is_hex_id(response["cycle_capability"])
        ):
            raise LocalBridgeError("Invalid bridge refresh run claim response")
        return response["remaining_grant_ms"], response["cycle_capability"]

    async def async_read_refresh_run_property(
        self,
        epoch: str,
        run_id: str,
        cycle: int,
        address: tuple[str, int, int],
        cycle_capability: str | None,
        continuation: str | None,
        timeout: float,
    ) -> tuple[int, str | None, str | None]:
        """Send one read and verify its exact identity, value and capability placement."""
        did, siid, piid = address
        identity = {"did": did, "siid": siid, "piid": piid}
        response = await self._async_run_request(
            "read",
            epoch,
            run_id,
            {
                "cycle": cycle,
                "method": "get_properties",
                "params": [identity],
                "timeout": min(_READ_TIMEOUT, timeout),
                "cycle_capability": cycle_capability,
                "continuation": continuation,
            },
            min(_READ_TIMEOUT + 1, timeout),
        )
        if (
            response.keys() != {"cycle", "result", "continuation", "next_cycle_capability"}
            or type(response["cycle"]) is not int
            or response["cycle"] != cycle
        ):
            raise LocalBridgeError("Invalid bridge refresh run read response")
        result = response["result"]
        if not isinstance(result, list) or len(result) != 1:
            raise LocalBridgeError("Invalid bridge refresh run property response")
        item = _parse_property(result[0])
        if any(item[key] != value for key, value in identity.items()) or not -(1 << 31) <= item["value"] < 1 << 31:
            raise LocalBridgeError("Invalid bridge refresh run property response")
        last = address == _READ_PROPERTIES["error"]
        receipt = response["continuation"]
        next_cycle = response["next_cycle_capability"]
        max_cycles = 1 if self._read_mode == READ_MODE_ON_DEMAND else _RUN_MAX_CYCLES
        if (receipt is not None if last else not _is_hex_id(receipt)) or (
            not _is_hex_id(next_cycle) if last and cycle < max_cycles else next_cycle is not None
        ):
            raise LocalBridgeError("Invalid bridge refresh run capability placement")
        return item["value"], receipt, next_cycle

    async def async_abort_refresh_run(
        self, epoch: str, run_id: str, revoke_secret: str, timeout: float
    ) -> None:
        """Attempt one revocation with a proof that cannot authorize a read."""
        response = await self._async_run_request("abort", epoch, run_id, {"revoke_secret": revoke_secret}, timeout)
        if response.keys() != {"terminal"} or response["terminal"] is not True:
            raise LocalBridgeError("Invalid bridge refresh run abort response")


@dataclass(repr=False)
class _RefreshRun:
    """Volatile identities and permissions, intentionally absent from diagnostics."""

    epoch: str
    run_id: str
    revoke_secret: str | None
    task: Task | None
    may_be_armed: bool = False
    deadline: float = 0
    capability: str | None = None
    continuation: str | None = None
    seen_capabilities: set[str] = field(default_factory=set)

    def remember_capability(self, capability: str | None) -> None:
        if capability is not None:
            if capability in self.seen_capabilities or capability in (self.run_id, self.revoke_secret):
                raise LocalBridgeError("Bridge reused a refresh run capability")
            self.seen_capabilities.add(capability)


class LocalBridgeCoordinator(DataUpdateCoordinator[DreameVacuumDevice]):
    """Apply only validated snapshots from the active bridge epoch."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self._bridge = LocalBridgeClient(async_get_clientsession(hass), entry.data)
        self._read_mode = entry.data.get(CONF_BRIDGE_READ_MODE, READ_MODE_PASSIVE)
        super().__init__(
            hass, LOGGER, config_entry=entry, name=DOMAIN, update_interval=timedelta(seconds=10)
        )
        self.device = DreameVacuumDevice(entry.data[CONF_NAME], None, None, local_bridge=True)
        self._action_busy = False
        self._busy = False
        self._epoch: str | None = None
        self._sequence = 0
        self._retired_epochs: deque[str] = deque(maxlen=32)
        self._read_continuation: str | None = None
        self._read_sealed = False
        self._closed = False
        self._refresh_task: Task | None = None
        self._refresh_expiry: TimerHandle | None = None
        self._refresh_snapshot: dict[str, Any] | None = None
        self._prepare_task: Task | None = None
        self._prepared_run: _RefreshRun | None = None
        self._prepare_expiry: TimerHandle | None = None
        self._run: _RefreshRun | None = None
        self._run_abort_task: Task | None = None
        self._run_progress = {
            "state": "inactive",
            "completed_cycles": 0,
            "max_cycles": 1 if self._read_mode == READ_MODE_ON_DEMAND else _RUN_MAX_CYCLES,
            "next_due_at": None,
            "expires_at": None,
            "reason": None,
        }
        if self.state_refresh_run_enabled:
            entry.async_on_unload(
                hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self._async_stop_refresh_run)
            )

    async def async_control(self, action: str, context) -> None:
        """Require a direct admin press; actions are independent of snapshot expiry."""
        if not context.user_id or context.parent_id is not None:
            raise LocalBridgeError("Movement requires a direct authenticated administrator press")
        user = await self.hass.auth.async_get_user(context.user_id)
        if user is None or not user.is_admin:
            raise LocalBridgeError("Movement requires an administrator")
        if self._read_mode != READ_MODE_ON_DEMAND or action not in ("start", "pause", "dock"):
            raise LocalBridgeError("Local control is unavailable")
        if self._closed or self._epoch is None or not self.device.available:
            raise LocalBridgeError("Robot is unavailable")
        if self._action_busy:
            raise LocalBridgeError("A local action is already in progress")
        self._action_busy = True
        try:
            # Cancel a pending snapshot locally. The owner independently revokes
            # reads before sending an action; do not wait for an abort round trip.
            if self._run is not None:
                self._end_refresh_run(self._run, "cancelled", "movement_requested")
            self._clear_refresh_snapshot()
            self.async_update_listeners()
            await self._bridge.async_action(self._epoch, action)
        finally:
            self._action_busy = False

    @property
    def state_refresh_enabled(self) -> bool:
        return self._read_mode == READ_MODE_STATE_REFRESH

    @property
    def state_refresh_run_enabled(self) -> bool:
        return self._read_mode in _RUN_MODES

    @property
    def durable_refresh_enabled(self) -> bool:
        return self._read_mode in (READ_MODE_DURABLE_REFRESH, READ_MODE_ON_DEMAND)

    @property
    def refresh_run_progress(self) -> dict[str, Any]:
        """Return only display state, never the active run record."""
        return self._run_progress.copy()

    @property
    def refresh_snapshot(self) -> dict[str, Any] | None:
        """Return the solicited state while its oldest sample remains fresh."""
        if self._refresh_expiry is None or self.hass.loop.time() >= self._refresh_expiry.when():
            return None
        return self._refresh_snapshot

    @callback
    def _clear_refresh_snapshot(self) -> None:
        if self._refresh_expiry is not None:
            self._refresh_expiry.cancel()
            self._refresh_expiry = None
        self._refresh_snapshot = None

    @callback
    def _expire_refresh_snapshot(self, snapshot: dict[str, Any]) -> None:
        if snapshot is self._refresh_snapshot:
            self._clear_refresh_snapshot()
            self.async_update_listeners()

    def _publish_refresh_snapshot(
        self, samples: dict[str, tuple[float, dict]], published_at: float
    ) -> dict[str, Any]:
        snapshot = {
            "provenance": "solicited",
            "sample_window": {
                "start": samples["state"][1]["requested_at"],
                "end": samples["error"][1]["responded_at"],
            },
            "properties": {
                name: sample | {"age_at_publication_seconds": published_at - started}
                for name, (started, sample) in samples.items()
            },
        }
        self._clear_refresh_snapshot()
        self._refresh_snapshot = snapshot
        self._refresh_expiry = self.hass.loop.call_at(
            samples["state"][0] + _REFRESH_TTL, self._expire_refresh_snapshot, snapshot
        )
        return snapshot

    @callback
    def async_cancel_refresh(self) -> None:
        """Stop an in-flight cycle and discard any snapshot as unload begins."""
        if self._refresh_task is not None and not self._refresh_task.cancelling():
            self._refresh_task.cancel()
        if self._refresh_snapshot is not None:
            self._clear_refresh_snapshot()
            self.async_update_listeners()

    def _seal_read_trial(self) -> None:
        self._read_continuation = None
        self._read_sealed = True

    def _invalidate_snapshot(self) -> None:
        self._clear_refresh_snapshot()
        self.device.apply_local_bridge_properties({})
        if self._read_continuation is not None:
            self._seal_read_trial()

    def _fail_refresh_run(self, reason: str) -> None:
        if self._prepared_run is not None:
            self._end_prepared_run("failed", reason)
        if self._run is not None:
            self._end_refresh_run(self._run, "failed", reason)

    async def _async_apply_snapshot(self) -> None:
        try:
            state = await self._bridge.async_get_state()
            if self._closed or state.epoch is None:
                self._invalidate_snapshot()
                self._fail_refresh_run("bridge_not_ready")
                return
            if state.epoch in self._retired_epochs:
                raise LocalBridgeError("Bridge returned a retired session epoch")
            if state.epoch != self._epoch:
                self._clear_refresh_snapshot()
                self._fail_refresh_run("epoch_changed")
                if self._epoch is not None:
                    self._retired_epochs.append(self._epoch)
                self._epoch = state.epoch
                self._sequence = 0
                self._read_continuation = None
                self._read_sealed = False
                self.device.apply_local_bridge_properties({})
            if not state.properties:
                self._invalidate_snapshot()
                self._fail_refresh_run("bridge_not_ready")
                return
            if state.sequence < self._sequence:
                raise LocalBridgeError("Bridge telemetry sequence moved backwards")
            if state.sequence == self._sequence:
                if self.device.available and self.device.data != state.properties:
                    raise LocalBridgeError("Bridge telemetry changed without a new sequence")
                return
            self._sequence = state.sequence
            self.device.apply_local_bridge_properties(state.properties)
        except CancelledError:
            self._invalidate_snapshot()
            if self._prepared_run is not None or (
                self._run is not None and self._run.task is not current_task()
            ):
                self._fail_refresh_run("passive_check_cancelled")
            raise
        except LocalBridgeError:
            self._invalidate_snapshot()
            self._fail_refresh_run("bridge_not_ready")
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
        if self._read_mode != READ_MODE_LEGACY:
            raise LocalBridgeError("Individual reads require the legacy local bridge mode")
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

    async def async_refresh_local_state(self) -> dict[str, Any]:
        """Take one complete, same-epoch solicited snapshot per trial entry."""
        if not self.state_refresh_enabled:
            raise LocalBridgeError("Local state refresh requires the state refresh bridge mode")
        entry = self.config_entry
        if self._closed or entry.state is not ConfigEntryState.LOADED:
            raise LocalBridgeError("The local bridge entry is unloaded")
        if self._busy:
            raise LocalBridgeError("The local bridge is busy")
        attempts = self.hass.data.setdefault(_REFRESH_ATTEMPTS, set())
        if entry.entry_id in attempts:
            raise LocalBridgeError("The local state refresh attempt has already been used")

        attempts.add(entry.entry_id)
        self._busy = True
        self._refresh_task = current_task()
        clock = self.hass.loop.time
        deadline = clock() + _REFRESH_TIMEOUT
        try:
            async with timeout_at(deadline):
                await self._async_apply_snapshot()
                epoch = self._epoch
                if not self.device.available or epoch is None:
                    raise LocalBridgeError("The local bridge has no fresh telemetry")

                samples = {}
                continuation = None
                seen_receipts = set()
                for name, address in _READ_PROPERTIES.items():
                    started = clock()
                    if started >= deadline:
                        raise TimeoutError
                    requested_at = dt_util.utcnow().isoformat()
                    result, continuation = await self._bridge.async_read_property(
                        epoch, address, continuation
                    )
                    responded_at = dt_util.utcnow().isoformat()
                    if continuation is not None:
                        if continuation in seen_receipts:
                            raise LocalBridgeError("Bridge reused a continuation receipt")
                        seen_receipts.add(continuation)
                    value = cast(int, result["value"])
                    samples[name] = (
                        started,
                        {
                            "value": value,
                            "label": bridge_property_label(name, value),
                            "requested_at": requested_at,
                            "responded_at": responded_at,
                        },
                    )

                if clock() >= deadline:
                    raise TimeoutError
                await self._async_apply_snapshot()
                published_at = clock()
                if published_at >= deadline:
                    raise TimeoutError
                if self._closed or self._epoch != epoch or not self.device.available:
                    raise LocalBridgeError("The bridge session changed during the local state refresh")

                return deepcopy(self._publish_refresh_snapshot(samples, published_at))
        except TimeoutError:
            self._clear_refresh_snapshot()
            raise LocalBridgeError("The local state refresh timed out") from None
        except (CancelledError, LocalBridgeError):
            self._clear_refresh_snapshot()
            raise
        finally:
            self._refresh_task = None
            self._busy = False
            if not self._closed and entry.state is ConfigEntryState.LOADED:
                self.async_set_updated_data(self.device)

    @callback
    def _set_run_progress(self, state: str, **changes: Any) -> None:
        self._run_progress.update(state=state, next_due_at=None, reason=None)
        self._run_progress.update(changes)
        self.async_update_listeners()

    def _run_display_time(self, deadline: float) -> str:
        return (dt_util.utcnow() + timedelta(seconds=deadline - self.hass.loop.time())).isoformat()

    def _require_refresh_run(self, run: _RefreshRun, deadline: float) -> float:
        if (
            run is not self._run
            or self._closed
            or self.hass.is_stopping
            or self.config_entry.state is not ConfigEntryState.LOADED
            or self._epoch != run.epoch
            or not self.device.available
        ):
            raise LocalBridgeError("The local refresh run is no longer active")
        if run.task is not None and run.task.cancelling():
            raise CancelledError
        now = self.hass.loop.time()
        if now >= min(deadline, run.deadline):
            raise TimeoutError
        return now

    @callback
    def _end_refresh_run(self, run: _RefreshRun, state: str, reason: str | None = None) -> None:
        if run is not self._run:
            return
        self._run = None
        run.capability = run.continuation = None
        run.seen_capabilities.clear()
        revoke_secret, run.revoke_secret = run.revoke_secret, None
        task, run.task = run.task, None
        if task is not None and task is not current_task() and not task.done() and not task.cancelling():
            task.cancel()
        if state != "completed":
            self._clear_refresh_snapshot()
            if run.may_be_armed and revoke_secret is not None:
                self._schedule_refresh_run_abort(run, revoke_secret)
        self._set_run_progress(state, reason=reason)

    def _schedule_refresh_run_abort(self, run: _RefreshRun, revoke_secret: str) -> None:
        deadline = self.hass.loop.time() + _RUN_CONTROL_TIMEOUT
        self._run_abort_task = self.config_entry.async_create_task(
            self.hass,
            self._async_abort_refresh_run(run.epoch, run.run_id, revoke_secret, deadline),
            "abort local state refresh run",
            eager_start=False,
        )

    async def _async_abort_refresh_run(
        self, epoch: str, run_id: str, revoke_secret: str, deadline: float
    ) -> None:
        try:
            remaining = deadline - self.hass.loop.time()
            if remaining <= 0:
                raise TimeoutError
            async with timeout_at(deadline):
                await self._bridge.async_abort_refresh_run(epoch, run_id, revoke_secret, remaining)
        except (LocalBridgeError, TimeoutError):
            LOGGER.debug("Local refresh run abort was not acknowledged")
        finally:
            self._run_abort_task = None

    @callback
    def _cancel_prepare_task(self) -> None:
        task, self._prepare_task = self._prepare_task, None
        if task is not None and not task.cancelling():
            task.cancel()

    @callback
    def _async_stop_refresh_run(self, _: Event) -> None:
        self._cancel_prepare_task()
        if self._prepared_run is not None:
            self._end_prepared_run("cancelled", "home_assistant_stopped")
        elif self._run is not None:
            self._end_refresh_run(self._run, "cancelled", "home_assistant_stopped")
        elif self._refresh_snapshot is not None:
            self._clear_refresh_snapshot()
            self.async_update_listeners()

    async def async_cancel_refresh_run(self, reason: str = "requested") -> dict[str, Any]:
        """Invalidate the run immediately and wait for at most one bounded abort."""
        self._cancel_prepare_task()
        if self._prepared_run is not None:
            self._end_prepared_run("cancelled", reason)
        elif self._run is not None:
            self._end_refresh_run(self._run, "cancelled", reason)
        elif self.durable_refresh_enabled and self._refresh_snapshot is not None:
            self._clear_refresh_snapshot()
            self.async_update_listeners()
        if self._run_abort_task is not None:
            await shield(self._run_abort_task)
        return self.refresh_run_progress

    def _take_prepared_run(self) -> _RefreshRun:
        run, self._prepared_run = self._prepared_run, None
        if self._prepare_expiry is not None:
            self._prepare_expiry.cancel()
            self._prepare_expiry = None
        return cast(_RefreshRun, run)

    @callback
    def _end_prepared_run(self, state: str, reason: str) -> None:
        run = self._take_prepared_run()
        revoke_secret, run.revoke_secret = run.revoke_secret, None
        self._clear_refresh_snapshot()
        self._schedule_refresh_run_abort(run, cast(str, revoke_secret))
        self._set_run_progress(state, reason=reason)

    async def async_prepare_refresh_run(self) -> dict[str, str]:
        """Create a volatile run binding for an independent owner-side arm."""
        if self._read_mode != READ_MODE_DURABLE_REFRESH:
            raise LocalBridgeError("Preparing a refresh run requires the durable refresh bridge mode")
        entry = self.config_entry
        if self._closed or self.hass.is_stopping or entry.state is not ConfigEntryState.LOADED:
            raise LocalBridgeError("The local bridge entry is unloaded or Home Assistant is stopping")
        if self._busy or self._run is not None or self._run_abort_task is not None:
            raise LocalBridgeError("The local bridge is busy")
        if self._prepared_run is not None:
            raise LocalBridgeError("A local refresh run is already prepared")
        self._busy = True
        task = self._prepare_task = current_task()
        try:
            await self._async_apply_snapshot()
            if self._prepare_task is not task:
                raise CancelledError
            if self._closed or self.hass.is_stopping or entry.state is not ConfigEntryState.LOADED:
                raise LocalBridgeError("The local bridge entry is unloaded or Home Assistant is stopping")
            if self._epoch is None or not self.device.available:
                raise LocalBridgeError("The local bridge has no fresh telemetry")
            run = _RefreshRun(self._epoch, token_hex(16), token_hex(16), None, may_be_armed=True)
            run.deadline = self.hass.loop.time() + _RUN_MAX_SECONDS
            self._prepared_run = run
            self._prepare_expiry = self.hass.loop.call_at(
                run.deadline, self._end_prepared_run, "failed", "preparation_expired"
            )
            try:
                self._set_run_progress(
                    "prepared", completed_cycles=0, expires_at=self._run_display_time(run.deadline)
                )
            except Exception:
                self._end_prepared_run("failed", "prepare_failed")
                raise LocalBridgeError("The local refresh run preparation failed") from None
            return {
                "epoch": run.epoch,
                "run_id": run.run_id,
                "revoke_commitment": sha256(cast(str, run.revoke_secret).encode("ascii")).hexdigest(),
            }
        finally:
            self._prepare_task = None
            self._busy = False
            if not self._closed and entry.state is ConfigEntryState.LOADED:
                self.async_set_updated_data(self.device)

    async def async_start_refresh_run(self) -> dict[str, Any]:
        """Claim once, then hand the finite run to the loaded config entry."""
        if not self.state_refresh_run_enabled:
            raise LocalBridgeError("Local refresh runs require the state refresh run bridge mode")
        entry = self.config_entry
        if self._closed or self.hass.is_stopping or entry.state is not ConfigEntryState.LOADED:
            raise LocalBridgeError("The local bridge entry is unloaded or Home Assistant is stopping")
        if self._busy:
            raise LocalBridgeError("The local bridge is busy")

        clock = self.hass.loop.time
        if self._read_mode == READ_MODE_ON_DEMAND:
            if self._run is not None or self._run_abort_task is not None:
                raise LocalBridgeError("The local bridge is busy")
            epoch = self._epoch
            if epoch is None or not self.device.available:
                raise LocalBridgeError("The local bridge has no fresh telemetry")
            run = _RefreshRun(epoch, token_hex(16), token_hex(16), current_task())
            self._clear_refresh_snapshot()
        elif self.durable_refresh_enabled:
            if self._run is not None or self._run_abort_task is not None:
                raise LocalBridgeError("The local bridge is busy")
            if self._prepared_run is None:
                raise LocalBridgeError("Prepare and arm a new local refresh run first")
            if clock() >= self._prepared_run.deadline:
                self._end_prepared_run("failed", "preparation_expired")
                raise LocalBridgeError("The local refresh run preparation expired")
            run = self._take_prepared_run()
            run.task = current_task()
            self._clear_refresh_snapshot()
        else:
            attempts = self.hass.data.setdefault(_RUN_ATTEMPTS, set())
            if entry.entry_id in attempts:
                raise LocalBridgeError("The local refresh run attempt has already been used")
            attempts.add(entry.entry_id)
            epoch = self._epoch
            if epoch is None or not self.device.available:
                self._set_run_progress("failed", reason="bridge_not_ready")
                raise LocalBridgeError("The local bridge has no fresh telemetry")
            run = _RefreshRun(epoch, token_hex(16), token_hex(16), current_task())

        started = clock()
        control_deadline = started + _RUN_CONTROL_TIMEOUT
        run.deadline = started + _RUN_MAX_SECONDS
        self._run = run
        self._busy = True
        try:
            self._set_run_progress("starting", completed_cycles=0, expires_at=None)
            async with timeout_at(control_deadline):
                await self._async_apply_snapshot()
                claim_started = self._require_refresh_run(run, control_deadline)
                run.may_be_armed = True
                remaining_ms, capability = await self._bridge.async_claim_refresh_run(
                    run.epoch,
                    run.run_id,
                    cast(str, run.revoke_secret),
                    control_deadline - claim_started,
                )
                self._require_refresh_run(run, control_deadline)
                run.deadline = min(run.deadline, claim_started + remaining_ms / 1000)
                if clock() + _REFRESH_TIMEOUT > run.deadline:
                    self._end_refresh_run(run, "failed", "insufficient_grant")
                    raise LocalBridgeError("The refresh run grant is too short for a complete cycle")
                run.remember_capability(capability)
                run.capability = capability
                self._set_run_progress("starting", expires_at=self._run_display_time(run.deadline))
                run.task = entry.async_create_background_task(
                    self.hass, self._async_run_refresh_cycles(run), "local state refresh run", eager_start=False
                )
                run.task.add_done_callback(self._refresh_run_task_done)
                return self.refresh_run_progress
        except CancelledError:
            self._end_refresh_run(run, "cancelled", "start_interrupted")
            raise
        except TimeoutError:
            self._end_refresh_run(run, "failed", "claim_timeout")
            raise LocalBridgeError("The local refresh run claim timed out") from None
        except LocalBridgeError:
            self._end_refresh_run(run, "failed", "claim_failed")
            raise
        except Exception:
            self._end_refresh_run(run, "failed", "claim_failed")
            raise LocalBridgeError("The local refresh run claim failed") from None
        finally:
            self._busy = False
            if not self._closed and entry.state is ConfigEntryState.LOADED:
                self.async_set_updated_data(self.device)

    @callback
    def _refresh_run_task_done(self, task: Task) -> None:
        # A task cancelled before its first instruction cannot run its finally block.
        if self._run is not None and self._run.task is task:
            self._end_refresh_run(self._run, "cancelled", "run_interrupted")

    async def _async_run_refresh_cycles(self, run: _RefreshRun) -> None:
        clock = self.hass.loop.time
        due = None
        try:
            async with timeout_at(run.deadline):
                max_cycles = 1 if self._read_mode == READ_MODE_ON_DEMAND else _RUN_MAX_CYCLES
                for cycle in range(1, max_cycles + 1):
                    if due is not None:
                        await sleep(max(0, due - clock()))
                    now = self._require_refresh_run(run, run.deadline)
                    if due is not None and now > due + _RUN_WAKE_TOLERANCE:
                        self._end_refresh_run(run, "failed", "wake_late")
                        return
                    deadline = now + _REFRESH_TIMEOUT
                    if deadline > run.deadline:
                        self._end_refresh_run(run, "failed", "insufficient_grant")
                        return
                    if self._busy:
                        self._end_refresh_run(run, "failed", "bridge_busy")
                        return
                    self._busy = True
                    try:
                        self._set_run_progress("sampling")
                        async with timeout_at(deadline):
                            completed_at = await self._async_refresh_run_cycle(run, cycle, deadline)
                    finally:
                        self._busy = False
                        if not self._closed and self.config_entry.state is ConfigEntryState.LOADED:
                            self.async_set_updated_data(self.device)
                    self._run_progress["completed_cycles"] = cycle
                    if cycle == max_cycles:
                        self._end_refresh_run(run, "completed")
                        return
                    due = completed_at + _RUN_INTERVAL
                    if due + _REFRESH_TIMEOUT > run.deadline:
                        self._end_refresh_run(run, "failed", "insufficient_grant")
                        return
                    self._set_run_progress("waiting", next_due_at=self._run_display_time(due))
        except CancelledError:
            self._end_refresh_run(run, "cancelled", "run_interrupted")
            raise
        except TimeoutError:
            reason = "grant_expired" if clock() >= run.deadline else "cycle_timeout"
            self._end_refresh_run(run, "failed", reason)
        except Exception:
            self._end_refresh_run(run, "failed", "cycle_failed")

    async def _async_refresh_run_cycle(self, run: _RefreshRun, cycle: int, deadline: float) -> float:
        await self._async_apply_snapshot()
        samples = {}
        next_capability = None
        for name, address in _READ_PROPERTIES.items():
            started = self._require_refresh_run(run, deadline)
            requested_at = dt_util.utcnow().isoformat()
            value, receipt, next_capability = await self._bridge.async_read_refresh_run_property(
                run.epoch, run.run_id, cycle, address, run.capability, run.continuation, deadline - started
            )
            self._require_refresh_run(run, deadline)
            run.remember_capability(receipt)
            run.remember_capability(next_capability)
            run.capability = None
            run.continuation = receipt
            samples[name] = (
                started,
                {
                    "value": value,
                    "label": bridge_property_label(name, value),
                    "requested_at": requested_at,
                    "responded_at": dt_util.utcnow().isoformat(),
                },
            )

        await self._async_apply_snapshot()
        completed_at = self._require_refresh_run(run, deadline)
        self._publish_refresh_snapshot(samples, completed_at)
        run.capability = next_capability
        return completed_at

    async def async_close(self) -> None:
        """Clear the device state and stop coordinator scheduling."""
        self._closed = True
        self._read_continuation = None
        tasks = [task for task in (self._prepare_task, self._refresh_task) if task is not None]
        self.async_cancel_refresh()
        await self.async_cancel_refresh_run("entry_unloaded")
        if tasks:
            await gather(*tasks, return_exceptions=True)
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
        if (
            not isinstance(coordinator, LocalBridgeCoordinator)
            or coordinator.config_entry.state is not ConfigEntryState.LOADED
        ):
            raise LocalBridgeError("Select a loaded Local MQTT Bridge entry")
        return coordinator

    async def async_read_battery(call: ServiceCall) -> dict[str, int]:
        result = await get_coordinator(call).async_read_property(_BATTERY_ADDRESS)
        return {"battery_level": cast(int, result["value"])}

    async def async_read_property(call: ServiceCall) -> dict[str, str | int]:
        property_name = call.data[CONF_PROPERTY]
        result = await get_coordinator(call).async_read_property(_READ_PROPERTIES[property_name])
        return {"property": property_name, **result}

    async def async_refresh_local_state(call: ServiceCall) -> dict[str, Any]:
        coordinator = get_coordinator(call)
        if coordinator._read_mode == READ_MODE_ON_DEMAND:
            return await get_run_coordinator(call).async_start_refresh_run()
        return await coordinator.async_refresh_local_state()

    def get_run_coordinator(call: ServiceCall) -> LocalBridgeCoordinator:
        # The admin service helper also admits internal contexts without a user.
        if not call.context.user_id or call.context.parent_id is not None:
            raise LocalBridgeError("This action requires a direct authenticated administrator call")
        coordinator = get_coordinator(call)
        if not coordinator.state_refresh_run_enabled:
            raise LocalBridgeError("Local refresh runs require the state refresh run bridge mode")
        return coordinator

    async def async_start_refresh_run(call: ServiceCall) -> dict[str, Any]:
        return await get_run_coordinator(call).async_start_refresh_run()

    async def async_cancel_refresh_run(call: ServiceCall) -> dict[str, Any]:
        return await get_run_coordinator(call).async_cancel_refresh_run()

    async def async_prepare_refresh_run(call: ServiceCall) -> dict[str, str]:
        return await get_run_coordinator(call).async_prepare_refresh_run()

    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_PREPARE_LOCAL_STATE_REFRESH_RUN,
        async_prepare_refresh_run,
        schema=vol.Schema({vol.Required(CONF_ENTRY_ID): str}),
        supports_response=SupportsResponse.ONLY,
    )

    for service, handler in (
        (SERVICE_START_LOCAL_STATE_REFRESH_RUN, async_start_refresh_run),
        (SERVICE_CANCEL_LOCAL_STATE_REFRESH_RUN, async_cancel_refresh_run),
    ):
        async_register_admin_service(
            hass,
            DOMAIN,
            service,
            handler,
            schema=vol.Schema({vol.Required(CONF_ENTRY_ID): str}),
            supports_response=SupportsResponse.OPTIONAL,
        )

    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_REFRESH_LOCAL_STATE,
        async_refresh_local_state,
        schema=vol.Schema({vol.Required(CONF_ENTRY_ID): str}),
        supports_response=SupportsResponse.ONLY,
    )
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
