from datetime import datetime
from pathlib import Path

from simulator.models.charging_site import ChargingSite
from simulator.models.ev_charger import EVCharger
from simulator.utils.state import StateManager


def test_runtime_state_round_trip_restores_charger_and_site_state(tmp_path: Path) -> None:
    site = ChargingSite("Site_A")
    charger = EVCharger("Charger_01")
    site.add_charger(charger)

    current = datetime(2026, 4, 1, 12, 0, 0)
    charger.tick(current, 0.2, {"ambient_temp_c": 31.2, "current_grid_voltage": 480.0})
    charger.flush_data()

    charger.state["charger_state"] = EVCharger.STATE_THROTTLED
    charger.state["warning_code"] = 2
    charger.state["error_code"] = 0
    charger.state["ev_soc_percent"] = 63.5
    charger.state["cabinet_temp_c"] = 47.25
    charger.state["active_anomaly"] = "PUMP_DEGRADATION"
    charger.state["anomaly_severity"] = 0.42
    charger.state["pump_health"] = 0.71
    site.state["internal_queue_length"] = 2
    site.state["ambient_temp_c"] = 31.2

    manager = StateManager(tmp_path / "simulator_cursor.json")
    manager.save_runtime_state(current, [site])

    restored_site = ChargingSite("Site_A")
    restored_charger = EVCharger("Charger_01")
    restored_site.add_charger(restored_charger)

    restored_time = manager.load_runtime_state([restored_site], datetime(2025, 1, 1, 0, 0, 0))

    assert restored_time == current
    assert restored_site.state["internal_queue_length"] == 2
    assert restored_site.state["ambient_temp_c"] == 31.2
    assert restored_charger.state["charger_state"] == EVCharger.STATE_THROTTLED
    assert restored_charger.state["warning_code"] == 2
    assert restored_charger.state["ev_soc_percent"] == 63.5
    assert restored_charger.state["cabinet_temp_c"] == 47.25
    assert restored_charger.state["active_anomaly"] == "PUMP_DEGRADATION"
    assert restored_charger.state["anomaly_severity"] == 0.42
    assert restored_charger.state["pump_health"] == 0.71

    charger_state_sensor = next(sensor for sensor in restored_charger.sensors if sensor.name == "Charger_State")
    assert charger_state_sensor.has_emitted_value is True
    assert charger_state_sensor.last_emitted_at != datetime.min


def test_state_manager_recovers_from_backup(tmp_path: Path) -> None:
    path = tmp_path / "cursor.json"
    backup = tmp_path / "cursor.json.bak"
    path.write_text("not-json", encoding="utf-8")
    backup.write_text('{"last_tick": "2026-01-02T00:00:00", "assets": []}', encoding="utf-8")

    manager = StateManager(path)
    loaded = manager.load_cursor(datetime(2026, 1, 1, 0, 0, 0))
    assert loaded == datetime(2026, 1, 2, 0, 0, 0)
