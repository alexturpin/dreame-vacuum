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

The battery action retains its separate one-shot bridge authorization. For
the five-property trial, the bridge authorizes the sequence and returns an
opaque receipt on each successful intermediate reply. The coordinator keeps
it in memory after confirming the reply and epoch. The operator invokes
each subsequent property separately. A read failure or loss of passive
readiness clears the receipt and seals the trial for the current epoch.
Reloading the entry loses the receipt. The bridge requires the previous
receipt to continue a partially completed trial.

For any later expansion, the device protocol-construction seam is the
assignment to `DreameVacuumDevice._protocol`. The upstream device places
solicited traffic through `_request_properties` and receives unsolicited
messages through `_message_callback`. Using an injected transport there
would require explicit allowlists and validation before entering those
methods, and distinct lifecycle rules before enabling discovery, timers, or
legacy services. The current checkpoint covers the live HA lifecycle and
bounded read-only contract, with that expansion pending separate evidence
and authorization.
