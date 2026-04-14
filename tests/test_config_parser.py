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
