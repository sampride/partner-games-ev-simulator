import os
import yaml
from pathlib import Path
from typing import Any

from simulator.models.base import Asset
from simulator.models.charging_site import ChargingSite
from simulator.models.ev_charger import EVCharger

# from simulator.models.sump_station import SumpStation # Kept for rollback
from simulator.writers.base import Writer
from simulator.writers.csv_writer import CsvWriter
from simulator.writers.sensor_csv_writer import SensorCsvWriter
from simulator.writers.mqtt_writer import MqttWriter

ASSET_REGISTRY: dict[str, type[Asset]] = {
    "ChargingSite": ChargingSite,
    "EVCharger": EVCharger,
    # "SumpStation": SumpStation
}

WRITER_REGISTRY: dict[str, type[Writer]] = {
    "csv": CsvWriter,
    "mqtt": MqttWriter,
    "csv_per_sensor": SensorCsvWriter,
}


def load_config(filepath: str | Path) -> dict[str, Any]:
    with open(filepath, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config or {}


def _apply_state_overrides(asset: Asset, state_overrides: dict[str, Any]) -> None:
    """Helper to apply initial state values from config."""
    if not state_overrides:
        return

    for key, value in state_overrides.items():
        if key in asset.state:
            asset.state[key] = value
        else:
            print(f"Warning: Tried to set unknown state '{key}' on {asset.name}")


def _apply_sensor_overrides(asset: Asset, sensor_overrides: list[dict[str, Any]]) -> None:
    """Helper to apply sensor polling intervals and jitter from config."""
    if not sensor_overrides:
        return

    sensor_dict = {s.name: s for s in asset.sensors}
    for s_conf in sensor_overrides:
        s_name = s_conf["name"]
        if s_name in sensor_dict:
            sensor_dict[s_name].update_interval_sec = s_conf.get("interval", 1.0)
            sensor_dict[s_name].jitter_sec = s_conf.get("jitter", 0.0)
        else:
            print(f"Warning: Tried to override unknown sensor '{s_name}' on {asset.name}")


def build_simulation_components(
    config: dict[str, Any], project_root: Path
) -> tuple[list[Asset], list[Writer], float]:
    """Parse the configuration dict and instantiate writers and assets."""

    # 1. Build Writers
    writers: list[Writer] = []
    for w_conf in config.get("writers", []):
        w_type = w_conf["type"]
        w_kwargs = w_conf.get("config", {})

        if "output_dir" in w_kwargs:
            out_dir = Path(w_kwargs["output_dir"])
            # Only prepend project_root if the path is relative (e.g., "data")
            # If it's absolute (e.g., "/data"), leave it alone.
            if not out_dir.is_absolute():
                w_kwargs["output_dir"] = str(project_root / out_dir)

        if w_type == "mqtt":
            # This overrides the YAML's 'host' key if the Docker ENV variable exists
            w_kwargs["host"] = os.getenv("MQTT_HOST", w_kwargs.get("host", "localhost"))

        if w_type in WRITER_REGISTRY:
            writer_class = WRITER_REGISTRY[w_type]
            writers.append(writer_class(**w_kwargs))
        else:
            raise ValueError(f"Unknown writer type in config: {w_type}")

    # 2. Build Assets
    assets: list[Asset] = []
    for a_conf in config.get("assets", []):
        a_name = a_conf["name"]
        a_type = a_conf["type"]
        a_state_overrides = a_conf.get("state", {})
        a_sensor_overrides = a_conf.get("sensors", [])

        if a_type not in ASSET_REGISTRY:
            raise ValueError(f"Unknown asset type in config: {a_type}")

        # Handle Hierarchical Assets (Charging Site)
        if a_type == "ChargingSite":
            max_q = a_conf.get("max_queue", 3)
            site = ChargingSite(name=a_name, max_queue=max_q)

            # Apply any site-level overrides
            _apply_state_overrides(site, a_state_overrides)
            _apply_sensor_overrides(site, a_sensor_overrides)

            # Build and attach child chargers
            for c_conf in a_conf.get("chargers", []):
                charger = EVCharger(name=c_conf["name"])

                # Re-use the helpers for the nested children
                _apply_state_overrides(charger, c_conf.get("state", {}))
                _apply_sensor_overrides(charger, c_conf.get("sensors", []))

                # <-- NEW: Attach Scheduled Anomalies -->
                if "anomalies" in c_conf:
                    charger.scheduled_anomalies = c_conf["anomalies"]

                if "random_anomalies" in c_conf:
                    charger.random_anomaly_config.update(c_conf["random_anomalies"])

                site.add_charger(charger)

            assets.append(site)

        # Handle Flat Assets (Sump Station)
        else:
            asset_class = ASSET_REGISTRY[a_type]
            asset = asset_class(name=a_name)

            _apply_state_overrides(asset, a_state_overrides)
            _apply_sensor_overrides(asset, a_sensor_overrides)

            assets.append(asset)

    # 3. Get Simulation Settings
    tick_rate = config.get("simulation", {}).get("tick_rate_sec", 0.5)

    return assets, writers, tick_rate