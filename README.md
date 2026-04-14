# Partner Games Simulator

An EV charging site simulator for hackathon and telemetry prototyping use cases.

## Highlights

- Virtual-time backfill plus realtime execution
- Multi-charger site model
- CSV, per-sensor CSV, and MQTT writers
- Configurable sensor polling intervals in YAML
- Progressive anomalies that create precursors before hard faults

## Implemented anomaly families

- `FAN_FAILURE`
- `CONNECTOR_ARCING`
- `BMS_CHATTER`
- `PUMP_DEGRADATION`
- `CONTACTOR_CHATTER`
- `SENSOR_DRIFT`

## Running locally

```bash
PYTHONPATH=src python main.py
```

## Notes

Use the YAML sensor intervals to reduce load on a Raspberry Pi if needed. The default configuration keeps electrical sensors relatively fast and thermal or health signals slower.
