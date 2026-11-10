# Local inline map ingestion review

November 10, 2026. This review used saved structural evidence, source and
guarded local test peers.

## Observed action response

A docked D10s Pro acknowledged MIoT action `6/1` with code `0` after receiving
property `2` with `{"req_type":1,"frame_type":"I","force_type":1}`. Its reply
contained a 2,200-character string at property `1`, an empty string at
property `3` and an integer at property `5`. The smoke helper called the
bridge client directly and retained field types and lengths. The live bytes,
map contents and Home Assistant rendering are unverified.

## Native Home Assistant path

In candidate mode, [`dreame_vacuum.vacuum_request_map`](../custom_components/dreame_vacuum/services.yaml)
targets the vacuum entity and sends a map-frame request. The native command
scope validates the action reply, then the [local manager](../custom_components/dreame_vacuum/dreame/local_map.py)
interprets `1` as the inline frame, `3` as an optional object reference and
`5` as the timestamp used when frame metadata is missing a wall-clock time.
Tasshack decodes the compressed frame and applies the I/P stream rules. The
candidate coordinator publishes an accepted map revision after the command
succeeds, and the native camera serves the renderer output. Opening the
camera in candidate mode uses the map already held by the integration.

Previously the manager advanced its latest timestamp and map ID before it
checked the frame type. A newer unsupported frame for another map could
cause a subsequent valid frame to be skipped. A truncated pixel buffer or
malformed trailing JSON could also reach native frame application and
displace the current map. The manager now checks these before updating frame
ordering. The native decoder still bounds decompression to 16 MiB, checks
for a complete stream and header, and enforces nonnegative dimensions, sides
up to 4096 and up to 4,194,304 pixels. Zero-sized and metadata-free frames
still reach native handling when otherwise valid. If native application
raises a handled error, the manager restores the ordering watermark. The
native metadata schema and its in-place update behavior remain upstream.

## Verification and remaining observation

The [guarded map tests](../tests/test_local_maps.py) exercise the actual HA
entity service and camera with a loopback bridge reply shaped like the saved
one. They assert one `6/1` action, the timestamp fallback, immediate camera
state and a decodable image different from the default. Reading that image
sends zero additional actions. Regressions also confirm that a newer
unsupported, truncated or malformed frame cannot block a subsequent valid
frame, and a simulated native application error leaves ordering usable.

During supervised acceptance, use the native service to request one frame
and compare the rendered camera to the robot's observed state. An empty map
is a normal outcome. A populated map and room boundaries require observed
cleaning or mapping progress, and a missing or malformed live frame requires
separately retained private evidence for decoder diagnosis.


## Cleaning-run uptime correction (local.11)

A subsequent supervised run supplied a new map through legacy inline property
13 (`0,<frame>`), while both embedded `timestamp_ms` and property 5 used robot
uptime. Treating property 5 as wall-clock time left the new map behind the
previous map's wall-clock ordering watermark, freezing the HA camera.

The local manager now anchors uptime to wall-clock time once per session,
ensuring the first normalized frame follows any existing watermark. Embedded
uptime preserves relative frame ordering when later reports arrive out of
order; repeated raw frames remain deduplicated before conversion. Resetting
the session clears the anchor. Valid wall-clock frame/report times retain
existing handling. This does not set the robot clock.

The synthetic regression replaces a wall-clock map with a legacy inline uptime
map, checks repeated-frame deduplication and applies out-of-order P frames.
All 15 map tests and 7 candidate diagnostics tests passed under the offline
guard. A privately retained real response also decoded and rendered through
the manager offline; its map bytes and image are not part of the repository.

## Cached uptime replay follow-up — local.12

Replaying cached frames spanning hours can project later frames into the future
when the first old frame is anchored to receipt time. Bound projected uptime
timestamps to receipt time and shift the current map, ordering watermark and
queued partial frames together. Genuine wall-clock timestamps are unchanged.
The parent ran 24 network-guarded map/diagnostic tests successfully, including
the full native HA service-to-camera legacy inline response path.

Actual saved-report replay confirms the projection fix. An empty successful
map-action reply still supplies no base frame; keep the partial map queued and
request a full frame through the bounded repair path. This is offline candidate
verification, not evidence of sustained physical-run map acceptance.
