# Partner Games Simulator

A configurable EV charging site simulator for hackathon devices, telemetry prototyping, historical dataset generation, and AVEVA CONNECT ingestion workflows.

This simulator was built to support a hackathon device that publishes:
- **historical data** to CSV files for bulk loading
- **realtime telemetry** to MQTT for live ingestion

The simulator models a single EV charging site containing multiple chargers, with support for:
- realistic sensor cadences
- progressive anomaly behaviour
- backfill and realtime execution
- restart persistence using a cursor and runtime state snapshot
- multiple writer types for different ingestion paths

---

## Current intended use

The current primary use case is:
- generate or ship a seed historical dataset
- deploy it to the device
- start the simulator from a known point in time
- backfill from the stored cursor to "now"
- continue publishing live MQTT data in realtime

This gives contestants:
- a body of historical data to view and train against
- a live MQTT stream that behaves like a field device/gateway feed

---

## Key features

### Simulation modes

The simulator supports two high-level runtime modes:

- **`realtime`**
  - starts at `now - backfill_days`
  - catches up to wall-clock time by running in virtual time
  - once caught up, continues in realtime
  - intended for the deployed hackathon device

- **`history`**
  - runs a bounded historical generation job
  - intended for creating seed data on a more powerful machine
  - stops when the configured history window is complete

### Writers

The simulator currently supports:

- **`csv_per_sensor`**
  - writes one file per asset/sensor stream
  - intended to be simple for file-based ingestion workflows
  - especially useful for AVEVA Adapter for Structured Data Files experiments

- **`csv`**
  - writes a single unified history file
  - useful for quick inspection, debugging, and alternate data-loading workflows

- **`jsonl`**
  - writes newline-delimited JSON files in dated folders
  - useful when a structured file export is easier to inspect or ingest than CSV

- **`omf`**
  - sends OMF 1.2 JSON messages to a CDS or EDS OMF REST endpoint
  - creates streams with container messages that reference existing OMF types
  - sends telemetry with batched data messages

- **`mqtt`**
  - publishes live telemetry in realtime mode
  - supports multiple payload modes
  - includes broker outage buffering so data is not immediately lost if the broker disappears temporarily

### Runtime persistence

The simulator persists runtime state to disk so that restart behaviour is more realistic than a simple timestamp reset.

Persisted information includes:
- the last simulated timestamp
- site and charger runtime state
- active anomaly state and severity
- sensor emission state for on-change and heartbeat behaviour

This improves continuity across:
- device restarts
- overnight shutdowns
- container restarts

---

## Current EV charger model

The model is intentionally simplified compared with a full commercial charger digital twin, but it is rich enough for telemetry, trending, and anomaly-detection exercises.

### Charger telemetry currently modeled

Electrical and power conversion signals:
- `Grid_Voltage_AC`
- `Grid_Current_AC`
- `Input_Power_kW`
- `DC_Bus_Voltage`
- `Output_Voltage_DC`
- `Output_Current_DC`
- `Requested_Current_DC`

Cooling and thermal signals:
- `Cooling_Fan_RPM`
- `Pump_Speed_RPM`
- `Coolant_Flow_LPM`
- `Coolant_Pressure_kPa`
- `Coolant_Inlet_Temp`
- `Coolant_Outlet_Temp`
- `Ambient_Temp`
- `Cabinet_Temp`
- `Power_Module_Temp`
- `Cable_Connector_Temp`
- `Connector_Resistance_mOhm`

Session and operating state signals:
- `Derate_Level_Percent`
- `EV_State_of_Charge`
- `Charger_State`
- `Session_Duration`
- `Warning_Code`
- `Error_Code`

Site-level signals:
- `site_total_power_kw`
- `site_grid_voltage_v`
- `site_ac_current_a`
- `site_power_available_kw`
- `main_breaker_load_percent`
- `number_of_active_sessions`
- `ambient_temp_c`

---

## Implemented anomaly families

The simulator currently includes these anomaly families:

- `FAN_FAILURE`
- `CONNECTOR_ARCING`
- `BMS_CHATTER`
- `PUMP_DEGRADATION`
- `CONTACTOR_CHATTER`
- `SENSOR_DRIFT`

These anomalies are designed to create precursor behaviour rather than only binary hard-fault steps. In other words, the model aims to produce gradual degradation, control-loop disturbance, thermal shifts, or intermittent instability before a hard fault or operator-visible issue becomes obvious.

### Important note on anomaly detection

This simulator does **not** guarantee that a downstream anomaly-detection product will automatically identify every anomaly cleanly. That depends on:
- the amount of training history
- the selected signals
- the configured model in the downstream tool
- the severity and duration of the simulated anomaly

For hackathon purposes, the simulator should be treated as a source of:
- plausible telemetry
- visible trends
- event-capable warning/error codes
- fault conditions that can be used as labels or events

---

## Hard faults, warnings, and error codes

For the current hackathon scope, an important goal is that the simulator emit warning and error conditions that can be consumed downstream and used to create events.

The current intention is:
- **warnings** indicate degraded, abnormal, or precursor conditions
- **errors** represent harder faults that would plausibly be surfaced by charger control/management software

Because the main downstream need is event creation and supervised/label-assisted workflows, the `Warning_Code` and `Error_Code` signals are intentionally important even if the underlying anomaly model continues to evolve over time.

---

## Runtime behaviour

### Realtime behaviour

When running in realtime mode:
- the simulator starts at a virtual time determined by `backfill_days`
- it runs faster than realtime until it catches up to wall clock
- once caught up, it publishes live data continuously

### Backfill behaviour

Backfill is expected and normal when:
- the device is first started
- the device was shut down overnight
- the cursor/state file points to a past timestamp

Backfill currently writes to:
- CSV writers: **yes**
- MQTT writer: **no** by default

This is intentional. MQTT is intended to behave like a live device feed, while CSV captures the historical stream.

Backfill progress logs include both density and throughput metrics:

- `avg_tick_rows` and `max_tick_rows` describe how many telemetry rows the model emits per simulation tick. These numbers are mainly a sensor cadence/profile signal, so they may not change after writer or buffering optimizations.
- `speed` / `virtual_sec_per_real_sec` describe actual catch-up speed. For example, `speed=500.0x` means the simulator processed 500 seconds of virtual time in one wall-clock second.
- `rows_per_sec` and `ticks_per_sec` describe wall-clock throughput.
- `window_rows` is the number of generated rows since the previous backfill log.
- `flushes`, `avg_flush_rows`, `flush_time`, and `flush_time_pct` show whether writer flushes are consuming meaningful wall-clock time.
- `eta` estimates wall-clock time remaining until the simulator catches up to realtime, based on the latest logging window.

### Realtime MQTT behaviour

In the current design:
- CSV writers use buffered chunk flushing for efficiency
- MQTT in realtime publishes promptly rather than waiting behind the CSV-oriented buffer cadence
- the MQTT writer still keeps its own outage buffer so transient broker problems do not immediately drop data

### On-change sensors

Some discrete signals are emitted **on change** with a heartbeat instead of at a fixed periodic rate. This currently reduces noise on:
- `Charger_State`
- `Warning_Code`
- `Error_Code`

This is closer to how many live systems behave and avoids writing repetitive unchanged values at high frequency.

---

## Configuration overview

All major simulator behaviour is configured through YAML. The committed full
example is `config/config.example.yaml`. Put real credentials and
environment-specific settings in an ignored local file such as
`config/default_sim.yaml` or `config/local_sim.yaml`.

By default, startup uses `config/default_sim.yaml` when it exists. If it does
not exist, startup falls back to `config/config.example.yaml`. You can always
override the path with `SIM_CONFIG_PATH`.

For OMF, `client_id_env` and `client_secret_env` are environment variable
names. They should normally stay as `OMF_CLIENT_ID` and `OMF_CLIENT_SECRET`;
the actual client id and secret are read from those environment variables at
runtime.

PowerShell example:

```powershell
$env:SIM_CONFIG_PATH = "config/local_sim.yaml"
$env:OMF_CLIENT_ID = "<client-id>"
$env:OMF_CLIENT_SECRET = "<client-secret>"
$env:PYTHONPATH = "src"
uv run python -m simulator.main
```

Bash example:

```bash
export SIM_CONFIG_PATH=config/local_sim.yaml
export OMF_CLIENT_ID=<client-id>
export OMF_CLIENT_SECRET=<client-secret>
PYTHONPATH=src uv run python -m simulator.main
```

For Docker Compose, copy `.env.example` to `.env` and fill in the values. The
compose file passes `OMF_CLIENT_ID` and `OMF_CLIENT_SECRET` into the container.

For PyCharm, put `OMF_CLIENT_ID`, `OMF_CLIENT_SECRET`, and optionally
`SIM_CONFIG_PATH` in the Run Configuration environment variables field.

The repository includes a pre-commit secret check in `.githooks/pre-commit`.
Enable it once per clone with:

```bash
git config core.hooksPath .githooks
```

You can also run the same check manually:

```bash
uv run python scripts/check_secrets.py --all
```

### Top-level sections

- `simulation`
- `writers`
- `assets`

### `simulation`

Important fields:

- `tick_rate_sec`
  - internal simulation tick interval
  - lower values increase fidelity but also CPU load

- `backfill_days`
  - how far behind wall-clock time the simulator should start in realtime mode
  - may be fractional, for example `0.3`

- `backfill_log_interval_sec`
  - virtual-time interval between backfill progress log messages

- `realtime_log_interval_sec`
  - wall-clock interval between realtime heartbeat messages

- `write_buffer_max_rows`
  - shared engine-side buffering threshold for non-immediate writers

- `write_buffer_max_age_sec`
  - maximum age before buffered rows are flushed to buffered writers

- `mode`
  - `realtime` or `history`

History-mode-only fields:
- `history_end_time`
- `history_duration_days`

### `writers`

Each writer entry contains:
- `type`
- `config`

Supported writer types:
- `csv_per_sensor`
- `csv`
- `jsonl`
- `omf`
- `mqtt`

### `assets`

The current configuration defines a single charging site with multiple chargers. Each charger can have:
- a static anomaly schedule
- random anomaly generation
- a sensor list with configured intervals and optional generic `data_type`

Sensor `data_type` is writer-neutral metadata. Existing CSV, JSONL, and MQTT writers can ignore it; OMF uses it to choose the target platform type ID. If not specified, `data_type` defaults to `double`.

---

## Recommended sensor cadence philosophy

The simulator intentionally does **not** sample everything at the same speed.

Recommended approach:
- faster electrical signals: sub-second to 1 second
- thermal and cooling signals: 1 to 10 seconds depending on the signal
- site aggregates: 1 to 2 seconds
- state/warning/error: on change plus heartbeat, or low-rate periodic if required

This is more realistic than sampling everything at the same rate, and it keeps runtime load under control.

---

## MQTT writer modes

The MQTT writer supports multiple payload modes.

### `single_object_per_signal` (recommended for the hackathon)

Each MQTT message contains a single signal value.

Example topic:

```text
ev_network/Site_Melbourne_North/Charger_01/Output_Current_DC
```

Example payload:

```json
{
  "timestamp": "2026-04-14T22:55:29.924885",
  "value": 42.7
}
```

This is the recommended mode for AVEVA Adapter for MQTT because the adapter is documented around topic subscription plus JSON payload extraction using fields such as `valueField` with JSONPath-style expressions. A stable JSON object is therefore the safest shape. citeturn409641search5turn409641search25turn409641search18

### `single_object_per_asset`

Each message contains multiple values for one asset in a single object.

### `batched_array`

This is the legacy batch-style mode. It is still supported, but it is **not** the recommended format for the hackathon ingestion path because adapters and downstream tooling often prefer a simpler, stable payload shape.

---

## CSV writer behaviour

### `csv_per_sensor`

This writer creates one file per sensor stream, for example:

```text
/data/split_sensors/Site_Melbourne_North/Charger_01/Output_Current_DC.csv
```

Expected file shape:

```csv
timestamp,value
2026-04-14T22:55:29.924885,42.7
2026-04-14T22:55:30.024885,42.6
```

This is deliberately simple and was added to support file-based ingestion workflows such as AVEVA Adapter for Structured Data Files.

### `csv`

This writer creates a single unified CSV history file containing:
- timestamp
- asset
- sensor
- value

This is ideal for:
- debugging
- quick inspection
- ad hoc analysis
- fallback bulk ingestion workflows

### `jsonl`

This writer creates newline-delimited JSON files under dated folders. Each line is one telemetry row:

```json
{"timestamp":"2026-04-14T22:55:29.924885","asset":"AC.North.C01","sensor":"Output_Current_DC","value":42.7,"stream_id":"AC.North.C01.Output_Current_DC"}
```

Example config:

```yaml
- type: jsonl
  config:
    output_dir: "/data/jsonl"
    filename: "ev_telemetry.jsonl"
    include_stream_id: true
    allow_backfill: true
    allow_realtime: false
```

### `omf`

This writer sends OMF 1.2 messages to an OMF REST endpoint. It currently supports:
- `endpoint_type: "eds"` with no authentication
- `endpoint_type: "cds"` with client-credentials bearer-token authentication

The writer does **not** create OMF types. It expects the target platform to already contain one or more compatible time-indexed types with at least `Timestamp` and `Value` properties. It lazily creates one container per simulator stream using the row's generic `data_type`, then sends data messages in configurable batches.

OMF message bodies are size-limited by the target platform. The writer batches
container and data messages by compact JSON body size using `max_body_bytes`
instead of relying only on a row count. The default is `184320` bytes, leaving
headroom below the 192 KiB OMF limit. `batch_size` and `container_batch_size`
remain as high row-count safety ceilings; for backfill throughput they should
normally be high enough that `max_body_bytes` is the limiting factor.

For backfill throughput, `max_concurrent_requests` can post multiple prepared
OMF requests at the same time. The default is `1` to keep realtime OMF behavior
conservative. A value of `2` is a practical first test for backfill; only raise
it further if the target endpoint does not rate-limit or slow down under
parallel load. This is OMF-specific and does not change MQTT realtime publishing,
which still uses its immediate writer path.

Stream IDs follow the existing convention:

```text
AC.North.C01.Output_Current_DC
```

The generated container shape is:

```json
{"id":"AC.North.C01.Output_Current_DC","typeid":"Timeindexed.Double"}
```

The generated data shape is:

```json
{
  "containerid": "AC.North.C01.Output_Current_DC",
  "values": [
    {"Timestamp": "2026-04-14T22:55:29Z", "Value": 42.7}
  ]
}
```

EDS example:

```yaml
- type: omf
  config:
    endpoint_type: "eds"
    resource: "http://localhost:5590"
    default_omf_type: "Timeindexed.Double"
    omf_type_map:
      double: "Timeindexed.Double"
      integer: "Timeindexed.Integer"
      string: "Timeindexed.String"
    max_body_bytes: 184320
    batch_size: 5000
    container_batch_size: 1000
    max_concurrent_requests: 2
    use_compression: true
    allow_backfill: true
    allow_realtime: false
```

CDS example:

```yaml
- type: omf
  config:
    endpoint_type: "cds"
    resource: "https://uswe.datahub.connect.aveva.com"
    # Optional when token discovery is hosted separately from the namespace API.
    # auth_resource: "https://<auth-host>"
    # token_url: "https://<auth-host>/connect/token"
    tenant_id: "<tenant-id>"
    namespace_id: "<namespace-id>"
    client_id_env: "OMF_CLIENT_ID"
    client_secret_env: "OMF_CLIENT_SECRET"
    default_omf_type: "Timeindexed.Double"
    omf_type_map:
      double: "Timeindexed.Double"
      integer: "Timeindexed.Integer"
      string: "Timeindexed.String"
    max_body_bytes: 184320
    batch_size: 5000
    max_concurrent_requests: 2
    use_compression: true
    allow_backfill: true
    allow_realtime: false
    fail_open: true
```

OMF type selection:
- each emitted row carries the sensor's generic `data_type`
- `omf_type_map` maps that generic value to an OMF type ID
- `default_omf_type` is used when the data type is absent or unmapped

OMF authentication endpoint selection:
- `resource` is always used to build the OMF namespace API URL unless `omf_endpoint` is supplied directly
- `auth_resource` is used for CDS OpenID discovery when the auth host differs from `resource`
- `token_discovery_url` can override the discovery document URL
- `token_url` can bypass discovery and point directly at the token endpoint

Example sensor config:

```yaml
- name: "Warning_Code"
  data_type: "integer"
  emit_on_change: true
  heartbeat_interval_sec: 30.0
```

### Which CSV writer should be used for the AVEVA file adapter?

For the hackathon, the safest assumption is:
- **keep `csv_per_sensor` enabled** for file-adapter experiments
- keep the file shape simple: `timestamp,value`
- treat the path or filename as the stream identity

The Structured Data Files adapter release notes explicitly mention support for **stream identification based on column values**, which suggests the adapter is flexible, but that also means a more complex multi-stream CSV may require more mapping work. The split-sensor approach avoids that complexity and is therefore a good default for the hackathon path. citeturn409641search2

---

## AVEVA CONNECT / adapter guidance

### MQTT adapter

The current recommended simulator setting for the contest is:
- MQTT payload mode: `single_object_per_signal`

Reason:
- the AVEVA MQTT adapter documentation is built around topic subscription plus data discovery / value extraction using fields such as JSONPath expressions. Stable single-object messages are therefore the lowest-risk choice. citeturn409641search5turn409641search25turn409641search18

A practical topic subscription pattern would be something like:

```text
ev_network/Site_Melbourne_North/+/+
```

or a broader pattern if needed.

### Structured Data Files adapter

For the Structured Data Files path, the current recommendation is:
- use the split-sensor CSV writer first
- keep one stream per file
- keep the file shape very simple

That is the least ambiguous ingestion shape and is easier to explain to contestants.

---

## Example workflows

### 1. Normal device startup

Use when the simulator is running on the hackathon device.

Outcome:
- loads previous cursor/state if available
- backfills CSV from the stored point in time to now
- resumes live MQTT publication in realtime

Command:

```bash
PYTHONPATH=src python main.py
```

### 2. Quick local test with small backfill

Edit config:

```yaml
simulation:
  backfill_days: 0.2
```

Outcome:
- starts a few hours behind
- catches up quickly
- then begins live MQTT publication

### 3. Historical generation on a stronger machine

Edit config:

```yaml
simulation:
  mode: "history"
  history_end_time: "2026-04-14T00:00:00"
  history_duration_days: 30
```

Outcome:
- writes a bounded history window
- useful for generating seed data before deployment

---

## Running locally

### Python

```bash
PYTHONPATH=src python main.py
```

### Docker

Use the included Dockerfile / compose setup as the base deployment path. Validate volume mappings carefully so that:
- `/data` persists across restarts
- generated CSV files survive container recreation
- the state file is not lost between runs

---

## State files and data directories

By default, the simulator persists state under the configured data directory.

Important files/directories:
- cursor/state file
- unified CSV output directory
- split-sensor CSV output directory

These should be treated as persistent runtime data. Do **not** assume that deleting the container is safe unless these directories are mounted to persistent storage.

---

## Logging expectations

Typical expected log messages include:
- startup and resolved runtime settings
- state load / state recovery
- backfill progress
- realtime heartbeat
- MQTT publish heartbeat
- anomaly transitions
- selected charger state transitions

Normal behaviour:
- backfill logs should appear periodically during catch-up
- MQTT publish heartbeat should appear only when realtime publishing is active
- a small amount of log flicker around the exact transition from backfill to realtime is acceptable

---

## Failure handling and hardening already in place

The simulator currently includes:
- config validation at startup
- writer failure isolation
- fail-open MQTT initialization option
- state-file backup recovery
- runtime state persistence beyond a simple cursor

This means:
- one bad writer should not kill the simulator
- a temporary MQTT failure should not stop CSV generation
- a corrupt state file has a recovery path

---

## Current limitations

This is a strong hackathon/developer simulator, but not a full production charger digital twin.

Important limitations:
- anomaly realism is still evolving
- a one-year historical dataset should be generated offline, not on the Raspberry Pi at startup
- Sparkplug B is not currently implemented
- some downstream adapter configuration will still need validation in your actual CONNECT environment

---

## Recommended hackathon defaults

For the contest device, recommended defaults are:
- MQTT payload mode: `single_object_per_signal`
- MQTT realtime only
- `csv_per_sensor` enabled
- unified CSV enabled for debugging
- modest backfill window on-device
- larger history generated offline and deployed onto the device

This gives contestants:
- easy-to-ingest file history
- live MQTT telemetry
- a realistic but manageable ingestion path

---

## Files added for AVEVA examples

See:
- `docs/AVEVA_ADAPTER_EXAMPLES.md`
- `examples/adapter-for-mqtt.generic.example.json`

These are intended as **starting points** and should be adjusted to match the exact adapter version and environment used at the event.
