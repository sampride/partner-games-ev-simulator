# AVEVA adapter examples

This document provides **starter examples** for using the simulator with AVEVA adapters.

These examples are intentionally conservative and are designed around the simulator's current recommended ingestion shape:
- MQTT: one topic per signal, one JSON object per message
- CSV: one file per sensor stream with `timestamp,value`

These examples should be treated as a starting point and adjusted to match the exact adapter version and deployment environment in use.

## 1. MQTT payload shape expected from the simulator

Recommended simulator MQTT mode:

```yaml
- type: mqtt
  config:
    host: "localhost"
    port: 1883
    base_topic: "ev_network/Site_Melbourne_North"
    payload_mode: "single_object_per_signal"
    timestamp_field: "timestamp"
    value_field: "value"
    include_sensor_in_payload: false
    allow_backfill: false
    allow_realtime: true
```

Example MQTT topic:

```text
ev_network/Site_Melbourne_North/Charger_01/Output_Current_DC
```

Example MQTT payload:

```json
{
  "timestamp": "2026-04-14T22:55:29.924885",
  "value": 42.7
}
```

This is the safest shape for the MQTT adapter because the adapter documentation describes data discovery and field extraction in terms of topic patterns and JSONPath-style field selection. citeturn409641search5turn409641search25turn409641search18

## 2. Example MQTT adapter starter configuration

The exact JSON accepted by the adapter can vary by version and by whether you are configuring it through the UI, API, or exported config files.

The example below is therefore a **starter shape**, not a guaranteed importable file for every version.

```json
{
  "componentType": "AdapterForMqtt",
  "componentId": "mqtt-ev-sim",
  "description": "Hackathon EV charger simulator - generic MQTT source",
  "dataSource": {
    "name": "ev-sim-broker",
    "brokerHost": "mqtt-broker",
    "brokerPort": 1883,
    "protocol": "Tcp",
    "topic": "ev_network/Site_Melbourne_North/+/+",
    "qos": 0,
    "retain": false,
    "username": "",
    "password": ""
  },
  "dataSelection": {
    "discoveryMode": "Generic",
    "topicPattern": "ev_network/Site_Melbourne_North/{deviceId}/{metricName}",
    "indexField": "$.timestamp",
    "indexFormat": "yyyy-MM-dd'T'HH:mm:ss.ffffff",
    "valueField": "$.value"
  }
}
```

Interpretation:
- `deviceId` is the charger name, for example `Charger_01`
- `metricName` is the signal name, for example `Output_Current_DC`
- the payload timestamp is read from `$.timestamp`
- the metric value is read from `$.value`

If your adapter version does not use these exact field names, keep the same conceptual mapping:
- subscribe to a topic pattern that captures the charger and signal name
- read timestamp from the `timestamp` property
- read value from the `value` property

## 3. Suggested MQTT mapping strategy for contestants

The safest contest guidance is:
- keep the topic structure stable
- keep one MQTT message per signal value
- keep the payload fields stable

Suggested topic mapping:

```text
ev_network/Site_Melbourne_North/<asset>/<sensor>
```

Examples:

```text
ev_network/Site_Melbourne_North/Charger_01/Output_Current_DC
ev_network/Site_Melbourne_North/Charger_01/Power_Module_Temp
ev_network/Site_Melbourne_North/site_total_power_kw
```

For site-level values, the simulator currently publishes them directly beneath the site base topic.

## 4. Structured Data Files adapter guidance

The Structured Data Files adapter release notes mention support for stream identification based on column values, which implies that a single multi-stream CSV can be made to work with appropriate mapping. citeturn409641search2

However, for the hackathon, the simpler and safer format is:
- one stream per file
- header: `timestamp,value`

That is exactly what the `csv_per_sensor` writer is for.

Example file path:

```text
/data/split_sensors/Site_Melbourne_North/Charger_01/Output_Current_DC.csv
```

Example file content:

```csv
timestamp,value
2026-04-14T22:55:29.924885,42.7
2026-04-14T22:55:30.024885,42.6
```

Why this is recommended:
- the stream identity is obvious from the file path/name
- the file structure is stable
- contestants do not need to reason about tag columns or multi-stream parsing immediately

## 5. If the Structured Data Files adapter prefers explicit stream columns

If your eventual adapter configuration proves easier with explicit stream-identification columns, the fallback is the unified CSV writer.

Its conceptual row shape is:

```csv
timestamp,asset,sensor,value
2026-04-14T22:55:29.924885,Site_Melbourne_North/Charger_01,Output_Current_DC,42.7
```

That shape is more flexible, but it shifts more mapping work into the adapter configuration.

## 6. Recommendation summary

For the hackathon path:
- use **MQTT `single_object_per_signal`** for live data
- use **`csv_per_sensor`** for structured-file ingestion experiments
- keep unified CSV as a fallback/debug output

That combination gives the cleanest path into AVEVA CONNECT with the least ambiguity.
