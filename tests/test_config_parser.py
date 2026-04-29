from pathlib import Path

import pytest

from simulator.utils.config_parser import ConfigValidationError, build_simulation_components, validate_config


def test_build_components(tmp_path: Path) -> None:
    config = {
        "simulation": {"tick_rate_sec": 0.1},
        "writers": [
            {"type": "csv", "config": {"output_dir": "out", "filename": "x.csv"}},
        ],
        "assets": [
            {
                "name": "SiteA",
                "type": "ChargingSite",
                "chargers": [{"name": "C1"}],
            }
        ],
    }

    assets, writers, tick_rate = build_simulation_components(config, tmp_path)

    assert len(assets) == 1
    assert len(writers) == 1
    assert tick_rate == 0.1


def test_validate_config_rejects_invalid_tick_rate() -> None:
    with pytest.raises(ConfigValidationError):
        validate_config({"simulation": {"tick_rate_sec": 0}, "assets": [{"name": "a", "type": "ChargingSite"}]})


def test_sensor_data_type_config_is_applied(tmp_path: Path) -> None:
    config = {
        "simulation": {"tick_rate_sec": 0.1},
        "assets": [
            {
                "name": "SiteA",
                "type": "ChargingSite",
                "sensors": [
                    {"name": "number_of_active_sessions", "data_type": "integer"}
                ],
                "chargers": [
                    {
                        "name": "C1",
                        "sensors": [{"name": "Charger_State", "data_type": "integer"}],
                    }
                ],
            }
        ],
    }

    assets, _, _ = build_simulation_components(config, tmp_path)

    site = assets[0]
    site_sensor = next(
        sensor for sensor in site.sensors if sensor.name == "number_of_active_sessions"
    )
    charger = site.get_child_assets()[0]
    charger_sensor = next(sensor for sensor in charger.sensors if sensor.name == "Charger_State")
    default_sensor = next(sensor for sensor in charger.sensors if sensor.name == "Output_Current_DC")

    assert site_sensor.data_type == "integer"
    assert charger_sensor.data_type == "integer"
    assert default_sensor.data_type == "double"
