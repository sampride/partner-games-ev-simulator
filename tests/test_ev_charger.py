from datetime import datetime

from simulator.models.ev_charger import EVCharger


def tick_charger(charger: EVCharger, seconds: int, delta: float = 1.0) -> None:
    now = datetime(2026, 1, 1, 12, 0, 0)
    global_state = {"ambient_temp_c": 22.0, "current_grid_voltage": 480.0}
    for _ in range(int(seconds / delta)):
        charger.update_internal_state(delta, now, global_state)
        now = now.replace(second=(now.second + 1) % 60)


def test_connector_arcing_increases_resistance() -> None:
    charger = EVCharger("Test")
    charger.start_session(1800)
    charger.state["charger_state"] = EVCharger.STATE_CHARGING
    charger.scheduled_anomalies = [{"type": "CONNECTOR_ARCING", "start_sec": 0, "duration_sec": 900}]

    baseline = charger.state["connector_resistance_mohm"]
    tick_charger(charger, seconds=240)

    assert charger.state["active_anomaly"] == "CONNECTOR_ARCING"
    assert charger.state["connector_resistance_mohm"] > baseline
    assert charger.state["anomaly_severity"] > 0.0


def test_pump_degradation_reduces_pump_health() -> None:
    charger = EVCharger("Test")
    charger.start_session(1800)
    charger.state["charger_state"] = EVCharger.STATE_CHARGING
    charger.scheduled_anomalies = [{"type": "PUMP_DEGRADATION", "start_sec": 0, "duration_sec": 1200}]

    tick_charger(charger, seconds=300)

    assert charger.state["active_anomaly"] == "PUMP_DEGRADATION"
    assert charger.state["pump_health"] < 1.0


def test_bms_chatter_creates_request_instability() -> None:
    charger = EVCharger("Test")
    charger.start_session(1800)
    charger.state["charger_state"] = EVCharger.STATE_CHARGING
    charger.scheduled_anomalies = [{"type": "BMS_CHATTER", "start_sec": 0, "duration_sec": 1200}]

    values = []
    now = datetime(2026, 1, 1, 12, 0, 0)
    global_state = {"ambient_temp_c": 22.0, "current_grid_voltage": 480.0}
    for _ in range(120):
        charger.update_internal_state(1.0, now, global_state)
        values.append(charger.state["requested_current_a"])
        now = now.replace(second=(now.second + 1) % 60)

    assert charger.state["bms_instability_index"] > 0.0
    assert max(values) - min(values) > 10.0


def test_sensor_drift_accumulates_persistent_offset() -> None:
    charger = EVCharger("Test")
    charger.start_session(1800)
    charger.state["charger_state"] = EVCharger.STATE_CHARGING
    charger.scheduled_anomalies = [{"type": "SENSOR_DRIFT", "start_sec": 0, "duration_sec": 1800}]

    tick_charger(charger, seconds=600)

    assert abs(charger.state["sensor_drift_temp_c"]) > 0.05
    assert abs(charger.state["sensor_drift_current_a"]) > 0.05
