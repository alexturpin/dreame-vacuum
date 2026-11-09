# Local bridge implementation boundary

The experimental config entry uses the upstream `dreame_vacuum` domain and
real Home Assistant entry, coordinator, vacuum, and sensor lifecycle. The
device uses Tasshack's property IDs, property cache and battery status
accessor. The local vacuum maps reported state using Tasshack's activity map.

The read-only mode deliberately creates no `DreameVacuumProtocol`. The
upstream constructor and its startup path assume cloud or miIO discovery,
followed by default property requests and recurring update timers. The local
coordinator owns HTTP polling and applies a complete, validated snapshot to
the shared device cache. Applying the snapshot avoids the upstream
`_message_callback` and `_handle_properties` paths, whose property callbacks
and timers assume a connected upstream transport. Local vacuum and sensor
platform setup stop before legacy service registration or device probing.
When both entry types coexist, legacy custom vacuum services require the
send-command feature that only legacy vacuum entities advertise.

The explicit administrator actions use the bridge client's singleton read.
Upstream `_request_properties` batches results through the legacy update
path. Each bridge action checks passive readiness, validates a correlated
reply for the exact address, checks the session again, and returns the raw
value to the caller. The passive cache is populated by snapshots. A local
coordinator refuses overlapping reads while it awaits the snapshot or reply.

## Read modes

The Local MQTT Bridge configuration and reconfiguration form offers two modes
under **Read mode**. **Legacy individual reads** is the default and preserves
the battery action and the separate five-property trial actions. The battery
action uses its own one-shot bridge authorization. The legacy property trial
requires one action per property, in order. It stores the bridge's continuation
receipt in memory and loses it on reload. A read failure or loss of passive
readiness clears the receipt and seals that trial for the current epoch.

**Bounded state refresh** requires a bridge owner configured for a single
`state_refresh` grant. The entry accepts only the refresh action. Home
Assistant administrators can request it with a response variable.

```yaml
action: dreame_vacuum.refresh_local_state
data:
  config_entry_id: YOUR_LOCAL_BRIDGE_ENTRY_ID
response_variable: local_state
```

The action uses one passive readiness check, five sequential singleton reads
(`state`, `status`, `charging_status`, `task_status`, `error`), and a final
passive readiness check for the same session. It passes each opaque
continuation receipt directly to the next request. A 45-second monotonic
deadline covers the entire cycle. The entry gets one attempt per Home
Assistant process, including across entry reloads. Cancellation, deadline,
invalid or lost replies, and detected session changes end the cycle without
publishing a partial result. The owner independently enforces its grant.

On success the action response and the **Local state refresh** diagnostic
sensor include `provenance: solicited`, a `sample_window` from the first
request to the last response, and a `properties` map. Each property has a
raw `value`, interpreted `label`, UTC `requested_at` and `responded_at`,
and monotonic `age_at_publication_seconds`. The readings describe a window of
observations. The sensor state uses the `state` label. Unknown codes preserve
their integers with a null label. State code zero is the charging-completed
alias, and state codes 1–18 use the pinned mapping.

The diagnostic snapshot expires 60 seconds after the first read request
started. Detected disconnect, epoch change, invalid readiness data, or
unload clears it. It starts unavailable after Home Assistant starts or the
entry reloads. Setup, ordinary updates, and the expiry timer send zero
property read requests. Passive vacuum and battery entities use their own
telemetry and the local vacuum exposes zero control features.

## Upstream protocol seam

For any later expansion, the device protocol-construction seam is the
assignment to `DreameVacuumDevice._protocol`. The upstream device places
solicited traffic through `_request_properties` and receives unsolicited
messages through `_message_callback`. Using an injected transport there
would require explicit allowlists and validation before entering those
methods, and distinct lifecycle rules before enabling discovery, timers, or
legacy services. The current checkpoint covers the live HA lifecycle and
bounded read-only contract, with that expansion pending separate evidence
and authorization.
