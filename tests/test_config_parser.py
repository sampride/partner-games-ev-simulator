from pathlib import Path

from simulator.utils.config_parser import build_simulation_components, load_config


def test_default_config_builds_non_mqtt_components() -> None:
    config = load_config(Path("config/default_sim.yaml"))
    config["writers"] = [w for w in config["writers"] if w["type"] != "mqtt"]
    assets, writers, tick_rate = build_simulation_components(config, Path.cwd())

    assert assets
    assert writers
    assert tick_rate == 0.05
