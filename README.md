<center>
<div align="center">

<hr>
<a href="https://dreame-vacuum.tasshack.com/"><img alt="Dreame Vacuum" src="https://dreame-vacuum.tasshack.com/logo.svg" width=400></a>
<hr>

<a href="https://dreame-vacuum.tasshack.com/"><img alt="Dreame Vacuum" src="https://dreame-vacuum.tasshack.com/dvc.png" width=480 /></a>

Complete app replacement with **Home Assistant** for **Dreame** robot vacuums.

[![Version](https://img.shields.io/github/manifest-json/v/Tasshack/dreame-vacuum?filename=custom_components%2Fdreame_vacuum%2Fmanifest.json&color=18bcf2&label=Version&style=for-the-badge)](https://github.com/Tasshack/dreame-vacuum/releases)
![Downloads](https://img.shields.io/github/downloads/Tasshack/dreame-vacuum/total?label=Downloads&style=for-the-badge&color=18bcf2)
[![HACS](https://img.shields.io/badge/HACS-Default-orange.svg?logo=HomeAssistantCommunityStore&color=18bcf2&logoColor=white&style=for-the-badge)](https://github.com/hacs/integration)
[![Community Forum](https://img.shields.io/static/v1.svg?label=Community&message=Forum&color=18bcf2&logo=HomeAssistant&logoColor=white&style=for-the-badge)](https://community.home-assistant.io/t/custom-component-dreame-vacuum/473026)

</div>
</center>

<hr>

## Experimental Local MQTT Bridge

This fork adds a **Local MQTT Bridge** entry type on top of Tasshack v2.0.3
(`fec086caf064f2681e8bcc7be20a0d0ef54f444d`). It is a read-only Home Assistant
client for a separately managed bridge implementing the version 1 HTTP contract
below. The fork is intended for controlled offline testing and separately
authorized trials. It has the same Home Assistant domain as the upstream
integration, so evaluate it from a pinned checkout in a dedicated test instance.
See the [implementation boundary](docs/LOCAL_BRIDGE.md) for the device paths.

Local MQTT Bridge creates a vacuum with zero control features and a battery
sensor. The vacuum uses supported state codes from passive reports. The
battery sensor displays the reported percentage, including zero. The bridge
must supply fresh telemetry for the active session before the vacuum is
available. The battery needs its own report. On connection loss or invalid
state, the client clears its cache. If connectivity returns in the same epoch,
the client needs an advancing sequence before restoring availability. A new
epoch starts with an empty cache.

### Configuration

In an authorized test instance, load the pinned checkout's
`custom_components/dreame_vacuum` as a custom integration. Add **Dreame Vacuum**
and select **Local MQTT Bridge**. Enter a name, the bridge origin, and its bearer
token. Use HTTPS on an RFC 1918 IPv4 or ULA IPv6 address. The HA system trust
store can verify the certificate and address, or you can enter the SHA-256
fingerprint of the exact server certificate, obtained over a trusted channel.
HTTP is available for loopback testing. Use **Reconfigure** on the entry to
change these settings. The bearer token is excluded from diagnostics.

Local setup validates the bridge with a GET. Setup, reconfiguration, and the
10-second refresh cycle fetch passive snapshots only. The local entry creates
no Dreame cloud, miIO, or map manager and exposes no action, map, or setting
controls. Other entry types retain their existing transport and entities.

The administrator action `dreame_vacuum.read_local_battery` takes a loaded
local `config_entry_id` and requires response handling. Invoke it only when a
single robot request has been separately authorized. The action checks current
readiness, requests one battery reading, and verifies that the session is
still active. It returns `battery_level` to the caller. The sensor continues
to reflect passive telemetry. The bridge may refuse the action when its
one-shot read authorization is absent or exhausted.

The administrator action `dreame_vacuum.read_local_property` takes a loaded
local `config_entry_id` and one `property` from the table below. Enable
response handling and authorize the trial at the bridge before calling it.
Invoke each action separately in the table's order and stop on the first
failure. The action returns the property name, `did`, `siid`, `piid`, and
the raw integer `value`. An unfamiliar code is returned as received. Entities
and diagnostics continue to reflect passive reports.

| Property | `did` | `siid` | `piid` |
| --- | --- | --- | --- |
| `state` | `"0"` | 2 | 1 |
| `status` | `"5"` | 4 | 1 |
| `charging_status` | `"3"` | 3 | 2 |
| `task_status` | `"11"` | 4 | 7 |
| `error` | `"1"` | 2 | 2 |

### Bridge contract

Both endpoints require the configured bearer token. The client disables
redirects and environment proxies.

- `GET /v1/state` returns `version`, `epoch`, `connected`, `ready`, `sequence`,
  and `properties`. Version is `1`. Connected epochs are 32 lowercase
  hexadecimal characters. A disconnected session has a null epoch, false
  readiness, sequence zero, and an empty property list. A connected session
  without a fresh report has false readiness, sequence zero, and an empty
  property list. A ready snapshot carries a positive sequence and one or more
  validated properties. Each property includes `did`, `siid`, `piid`,
  `code: 0`, and an integer `value`. The supported addresses are state
  (`did: "0"`, `2.1`), battery (`did: "2"`, `3.1`), and status
  (`did: "5"`, `4.1`). Battery is bounded to 0–100.
- `POST /v1/read` sends `version: 1`, a unique 32-character lowercase hex
  `request_id`, the active `epoch`, `method: "get_properties"`, a five-second
  `timeout`, and exactly one `{did, siid, piid}` in `params`. The battery
  address is `("2", 3, 1)`. A successful reply returns `version`,
  `request_id`, `epoch`, and a one-item `result` with the exact requested
  address, `code: 0`, and an integer `value`. Battery is bounded to 0–100
  and the five trial properties to signed 32-bit integers.
- Requests for a trial property also carry `continuation`. It is null for
  the first request and contains the previous successful reply's receipt
  thereafter. Each successful trial reply carries the next receipt as 32
  lowercase hexadecimal characters, or null for the fifth and final reply.
  The client retains a receipt only after confirming the active epoch again.
  The battery action uses the original request and reply fields. An error
  reply carries `version`, `request_id`, `epoch`, and `error`. A refusal or
  transport or protocol failure ends the trial for that client and epoch.

### Offline verification

The test suite loads this integration through Home Assistant Core 2026.10.4
with a real loopback HTTP fixture. Use Python 3.14 to create a dedicated
environment and run the tests with external socket destinations disabled.

```sh
python3.14 -m venv .venv
.venv/bin/pip install -r tests/requirements.txt
.venv/bin/python -m pytest -q tests --disable-socket --allow-hosts=127.0.0.1,::1 --allow-unix-socket
```

## Features

- Wide range of Dreame, MOVA, Mijia and Trouver robot vacuums and mops, configurable with a DreameHome, MovaHome, Xiaomi Home account or fully locally without the cloud
- Almost every setting and state exposed dynamically as auto generated entities
- Live and multi-floor map support with zone, spot and room cleaning
- Per-room suction, water volume and cleaning order for customized cleaning
- Persistent notifications and events for automations
- Cleaning/cruising history, obstacle photos, map backup & recovery and saved WiFi maps
- Backend translations for 40 languages
- Complete interactive map editor via embedded [Dreame Vacuum Card](https://dreame-vacuum-card.tasshack.com/)
<hr>

## [Documentation](https://dreame-vacuum.tasshack.com/)

- [Installation](https://dreame-vacuum.tasshack.com/installation) - Minimum Home Assistant version and hardware requirements, with [HACS](https://dreame-vacuum.tasshack.com/installation/hacs) and [manual](https://dreame-vacuum.tasshack.com/installation/manually) setup steps
- [Configuration](https://dreame-vacuum.tasshack.com/configuration) - Adding the device with a [Dreamehome](https://dreame-vacuum.tasshack.com/configuration/dreamehome), [Xiaomi Home](https://dreame-vacuum.tasshack.com/configuration/xiaomihome), [MOVAhome](https://dreame-vacuum.tasshack.com/configuration/movahome) or [TROUVER](https://dreame-vacuum.tasshack.com/configuration/trouver) account, or [locally](https://dreame-vacuum.tasshack.com/configuration/local) over the LAN
- [Configuration Options](https://dreame-vacuum.tasshack.com/configuration-options) - Per device settings offered during setup and from the Configure button
- [Supported Devices](https://dreame-vacuum.tasshack.com/guide/more/supported-devices) - Model codes of every tested [Dreame](https://dreame-vacuum.tasshack.com/guide/more/supported-devices#dreame), [MOVA](https://dreame-vacuum.tasshack.com/guide/more/supported-devices#mova), [Mijia](https://dreame-vacuum.tasshack.com/guide/more/supported-devices#mijia) and [TROUVER](https://dreame-vacuum.tasshack.com/guide/more/supported-devices#trouver) robot with the features it exposes
- [How to Use](https://dreame-vacuum.tasshack.com/guide) - How entities, services and events come together on a dashboard
- [Dashboard](https://dreame-vacuum.tasshack.com/guide/dashboard) - Setup examples and YAML templates for every supported Lovelace card, including the built-in **[Dreame Vacuum Card](https://dvc.tasshack.com/)**
- [Entities](https://dreame-vacuum.tasshack.com/guide/entities) - What gets generated for a device, with [map](https://dreame-vacuum.tasshack.com/guide/entities/map-entities), [room](https://dreame-vacuum.tasshack.com/guide/entities/room-entities) and [consumable](https://dreame-vacuum.tasshack.com/guide/entities/consumable-entities) entities covered separately
- [Map Support](https://dreame-vacuum.tasshack.com/guide/map-support) - How map data is decoded and rendered, with [history](https://dreame-vacuum.tasshack.com/guide/map-support/history), [recovery](https://dreame-vacuum.tasshack.com/guide/map-support/recovery), [obstacle photos](https://dreame-vacuum.tasshack.com/guide/map-support/obstacles) and [WiFi maps](https://dreame-vacuum.tasshack.com/guide/map-support/wifi-map)
- [Services](https://dreame-vacuum.tasshack.com/guide/services) - Parameters and automation examples for [device](https://dreame-vacuum.tasshack.com/guide/services/vacuum-services) and [map](https://dreame-vacuum.tasshack.com/guide/services/map-services) control
- [Notifications](https://dreame-vacuum.tasshack.com/guide/notifications) - Which device events raise a persistent notification and how to turn them off
- [Events](https://dreame-vacuum.tasshack.com/guide/events) - Event types fired on the bus and the payloads they carry
- [FAQ](https://dreame-vacuum.tasshack.com/guide/more/faq) - Recurring questions from the issue tracker and their actual causes

<hr>

## Thanks To

 - [xiaomi_vacuum](https://github.com/pooyashahidi/xiaomi_vacuum) by [@pooyashahidi](https://github.com/pooyashahidi)
 - [Xiaomi MIoT for Home Assistant](https://github.com/ha0y/xiaomi_miot_raw) by [@ha0y](https://github.com/ha0y)
 - [Xiaomi Cloud Map Extractor](https://github.com/PiotrMachowski/Home-Assistant-custom-components-Xiaomi-Cloud-Map-Extractor) by [@PiotrMachowski](https://github.com/PiotrMachowski)
 - Dreame cloud authentication by [@kuudori](https://github.com/kuudori)
 - Mova cloud support by [@r1si](https://github.com/r1si)
