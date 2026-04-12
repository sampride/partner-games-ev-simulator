import yaml
from pathlib import Path
from typing import Any

from simulator.models.base import Asset
from simulator.models.pump import CentrifugalPump
from simulator.models.sump_station import SumpStation
from simulator.models.ev_charger import EVCharger

from simulator.writers.base import Writer
from simulator.writers.mqtt_writer import MqttWriter
from simulator.writers.csv_writer import CsvWriter


# Simple registries to map YAML string names to actual Python classes
ASSET_REGISTRY: dict[str, type[Asset]] = {
    "CentrifugalPump": CentrifugalPump,
    "SumpStation": SumpStation,
    "EVCharger": EVCharger
}

WRITER_REGISTRY: dict[str, type[Writer]] = {
    "csv": CsvWriter,
    "mqtt": MqttWriter
}

def load_config(filepath: str | Path) -> dict[str, Any]:
    """Load and parse the YAML configuration file."""
    with open(filepath, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config or {}

def build_simulation_components(
        config: dict[str, Any], project_root: Path
) -> tuple[list[Asset], list[Writer], float]:
    """Parse the configuration dict and instantiate writers and assets."""

    # 1. Build Writers
    writers: list[Writer] = []
    for w_conf in config.get("writers", []):
        w_type = w_conf["type"]
        w_kwargs = w_conf.get("config", {})

        # Resolve relative paths for output directories based on the project root
        if "output_dir" in w_kwargs:
            w_kwargs["output_dir"] = str(project_root / w_kwargs["output_dir"])

        if w_type in WRITER_REGISTRY:
            writer_class = WRITER_REGISTRY[w_type]
            writers.append(writer_class(**w_kwargs))
        else:
            raise ValueError(f"Unknown writer type in config: {w_type}")

    # 2. Build Assets
        # Inside build_simulation_components(config, project_root):

        # 2. Build Assets
        assets: list[Asset] = []
        for a_conf in config.get("assets", []):
            a_name = a_conf["name"]
            a_type = a_conf["type"]
            a_state_overrides = a_conf.get("state", {})
            a_sensor_overrides = a_conf.get("sensors", []) # <-- NEW

            if a_type in ASSET_REGISTRY:
                asset_class = ASSET_REGISTRY[a_type]
                asset = asset_class(name=a_name)

                # Apply state overrides
                for key, value in a_state_overrides.items():
                    if key in asset.state:
                        asset.state[key] = value
                    else:
                        print(f"Warning: Tried to set unknown state '{key}' on {a_name}")

                # <-- NEW: Apply sensor overrides -->
                if a_sensor_overrides:
                    # Build a dictionary of the default sensors to easily update them
                    sensor_dict = {s.name: s for s in asset.sensors}

                    for s_conf in a_sensor_overrides:
                        s_name = s_conf["name"]
                        if s_name in sensor_dict:
                            sensor_dict[s_name].update_interval_sec = s_conf.get("interval", 1.0)
                            sensor_dict[s_name].jitter_sec = s_conf.get("jitter", 0.0)
                        else:
                            print(f"Warning: Tried to override unknown sensor '{s_name}' on {a_name}")

                assets.append(asset)
            else:
                raise ValueError(f"Unknown asset type in config: {a_type}")

    # 3. Get Simulation Settings
    tick_rate = config.get("simulation", {}).get("tick_rate_sec", 0.5)

    return assets, writers, tick_rate