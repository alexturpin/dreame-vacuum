# Local bridge implementation boundary

The experimental config entry uses the upstream `dreame_vacuum` domain and
real Home Assistant entry, coordinator, vacuum, and sensor lifecycle. The
device uses Tasshack's property IDs, passive property cache and battery
status accessor. The local vacuum maps state using Tasshack's activity map.

The read-only mode deliberately creates no `DreameVacuumProtocol`. The
upstream constructor and its startup path assume cloud or miIO discovery,
followed by default property requests and recurring update timers. The local
coordinator owns HTTP polling and applies validated passive GET results to
the shared device cache. Applying those results avoids the upstream
`_message_callback` and `_handle_properties` paths, whose property callbacks
and timers assume a connected upstream transport. Local vacuum and sensor
platform setup stop before legacy service registration or device probing.
When both entry types coexist, legacy custom vacuum services require the
send-command feature that only legacy vacuum entities advertise.

The explicit administrator actions use the bridge client's singleton read.
Upstream `_request_properties` batches results through the legacy update
path. Each bridge action checks passive readiness, validates a correlated
reply for the exact address, checks the session again, and returns the raw
value to the caller. A local coordinator refuses overlapping reads while it
awaits the snapshot or reply.

## Read modes

**Passive telemetry only** is the default for Local MQTT Bridge, including
existing entries without a saved **Read mode**. The vacuum and battery use
passive reports and the entry refuses all read actions.

**Legacy individual reads** preserves the battery action and the separate
five-property trial actions for entries explicitly configured in that mode. The battery
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

**Finite state refresh run** requires a pinned HTTPS bridge and an owner
configured with a startup `state_refresh_run` grant. A direct authenticated
administrator can call `dreame_vacuum.start_local_state_refresh_run` with
only `config_entry_id`. The grant allows up to three ordered five-property
cycles and expires within ten minutes of owner startup. The integration
starts each later cycle 125 seconds after the previous cycle's final
validation, provided a full 45-second cycle still fits and the wake is no
more than five seconds late. Ordinary passive polling continues between
cycles. Each successful cycle updates **Local state refresh**, with the
same 60-second freshness limit.

**Local state refresh run** shows progress independently of the snapshot.
The start action can return the current progress, and the sensor shows
completed cycles, the next due time, grant expiry and any terminal reason.
If the start response is lost, inspect the progress sensor. Home Assistant
can finish an accepted service call after its caller disconnects.
A direct administrator can call `dreame_vacuum.cancel_local_state_refresh_run`
with the same entry ID. Cancellation clears active state immediately and
attempts one bounded revocation request. The owner independently enforces
grant expiry and request limits if that request cannot be delivered.

The entry accepts one start per Home Assistant process, including across
reloads. It starts inactive after setup, and the grant capabilities stay in
memory. Reload, shutdown, session change, invalid replies and timing
failures end the active run. The integration does not recover a lost
capability or start another run automatically.

### On-demand state refresh (recommended for persistent local use)

Select **On-demand state refresh** with pinned HTTPS and an owner explicitly
installed with the matching `on_demand_refresh` policy. This is a different
policy from the temporary trial grants: an authenticated direct HA administrator
action can request one snapshot without a separate root arm.

Call `dreame_vacuum.refresh_local_state` with `config_entry_id` and response
handling from Developer Tools. The action returns acceptance/progress; watch
**Local state refresh run** for completion and **Local state refresh** for the
result. Do not retry an ambiguous action response. The existing start-run action
is an alias for the same single cycle in this mode. Internal/automation contexts
and non-administrator users are refused.

Each action allows exactly the five ordered state properties, at most five
seconds per read and 45 seconds per cycle. The owner issues a volatile claim
valid for at most 60 seconds and permits no second cycle. Failed/lost replies
end the cycle with no retry or partial publication. Allow at least 120 seconds
between refresh starts in the same owner epoch. The owner also bounds consumed
claim identities to 1,024 per epoch; reaching that limit requires an operator to
review and restart the owner, not an automatic reset.

The vacuum uses only the complete solicited snapshot. It expires 60 seconds
from the first request, becoming unavailable until the next explicit refresh.
The native passive feed remains independent; no battery read is included.
Setup, passive GET polling, reconnects and process recovery never initiate
property reads. Restart/reload discards in-flight work and snapshot state.
All local vacuum controls remain disabled, including generic send-command.

The persistent policy is deployment configuration; it survives restart. A local
root disarm disables further claims for the lifetime of the owner process,
including reconnects. To keep access disabled across restarts, stop/disable
command ingress and restore the passive owner configuration. Deployment and
robot testing require their own explicit scope approval.

### Durable state refresh

Select **Durable state refresh** with pinned HTTPS for a matching
durable bridge owner. A loaded entry starts with passive polling and an
unavailable vacuum. Each finite run takes three distinct steps.

1. A direct authenticated administrator calls
   `dreame_vacuum.prepare_local_state_refresh_run` with only
   `config_entry_id` and response handling. It checks passive readiness and
   returns the current `epoch`, random `run_id` and `revoke_commitment`.
   The commitment is the lowercase SHA-256 digest of the ASCII revoke
   secret. The secret stays in the running Home Assistant entry.
2. An independent root operator authorizes the exact returned tuple on
   the MQTT owner. Follow the owner's local procedure. HTTP and relay
   credentials provide no arm operation. If a previous run spent a read in
   the current epoch, wait at least 120 seconds from its last cycle start
   before arming a new preparation.
3. A direct authenticated administrator calls
   `dreame_vacuum.start_local_state_refresh_run` with only
   `config_entry_id`. The action consumes the preparation and claims the
   owner grant using the in-memory proof. It uses the finite run's ordered
   reads, pacing, budgets and version-2 endpoints. Its claim requires the
   owner to accept and echo `mode: durable_refresh`.

A preparation is usable for at most ten minutes and is discarded on
detected readiness loss, epoch change, cancellation, unload or shutdown.
A restarted entry has no preparation or grant proof. A failed or ambiguous
start consumes the preparation. Use the progress sensor to inspect the
result. Any later run needs a fresh preparation and independent owner
authorization. Cancel via `dreame_vacuum.cancel_local_state_refresh_run`
with the same entry ID. Cancellation clears local state and attempts one
bounded abort, including for a preparation that the owner may already
have armed. If the abort cannot reach the owner, its separate expiry
still applies. The owner can also disarm locally. If cancellation and
root arming race, verify the owner is disarmed before preparing again.

The read-only vacuum is available only while the complete solicited
snapshot is fresh. It exposes `provenance: solicited` and its
`sample_window`. Recognized nonzero error codes take precedence.
Unrecognized error codes produce unknown activity. With observed error
zero, Tasshack maps the supported raw state codes, including the
zero alias for charging completed. Unrecognized state codes also
produce unknown activity. Status, charging status and task status
remain observations in **Local state refresh**. They do not supply
derived activity when the state or error is unknown.

The snapshot clears 60 seconds after its first request and on detected
disconnect, session replacement or cancellation. The vacuum becomes
unavailable between snapshots. **Local passive state** and **Local passive
status** keep separate history from the bridge's per-session report cache.
That cache has no per-property timestamps. This mode creates no battery
entity. An existing passive receiver can still display its last observed
battery value for the current session. Battery freshness requires its own
policy. This profile exposes zero control features and does not request
battery, maps or writes.

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


## Manual start/pause buttons (local.7)

On-demand entries expose **Start cleaning** and **Pause** buttons on the existing
device. Availability follows the passive connection, independently of the
60-second solicited vacuum snapshot. A direct authenticated administrator press
is required; internal/automation contexts and non-admin users are rejected.
Each press sends one `POST /v1/action` with version, fresh request ID, current
epoch and an action name (`start` or `pause`). No arbitrary parameters, retries,
optimistic activity changes or additional robot reads are performed.

The owner must explicitly enable its HA-control policy. The installed bridge
still enforces its pinned TLS/credentials, action allowlist and session/replay
checks. Pause revokes unfinished reads before attempting its single action.
A missing response is an unknown outcome: check the robot rather than assuming
it did not move. These buttons are not an emergency stop. Startup, reconnect,
reload and passive polling never send actions. The vacuum entity itself retains
zero control features; use these buttons, not legacy upstream vacuum services.

### Return to dock (local.8)

On-demand entries also expose **Return to dock**. It uses the same direct-admin
button path with the fixed action name `dock`; the bridge maps that to MIoT
CHARGE (`siid=3`, `aiid=1`, no input parameters). Like Pause, it cancels an
unfinished state refresh. It does not call upstream `return_to_base()`, issue
ancillary writes, or retry. Docking acceptance must be verified separately on
the target robot; a successful action reply alone does not establish arrival.
