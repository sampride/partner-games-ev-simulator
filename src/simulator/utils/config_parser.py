from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from simulator.models.base import Asset
from simulator.models.charging_site import ChargingSite
from simulator.models.ev_charger import EVCharger
from simulator.writers.base import Writer
from simulator.writers.csv_writer import CsvWriter
from simulator.writers.jsonl_writer import JsonlWriter
from simulator.writers.mqtt_writer import MqttWriter
from simulator.writers.omf_writer import OmfWriter
from simulator.writers.sensor_csv_writer import SensorCsvWriter

logger = logging.getLogger("simulator.config")


class ConfigValidationError(ValueError):
    pass


ASSET_REGISTRY: dict[str, type[Asset]] = {
    "ChargingSite": ChargingSite,
    "EVCharger": EVCharger,
}

WRITER_REGISTRY: dict[str, type[Writer]] = {
    "csv": CsvWriter,
    "jsonl": JsonlWriter,
    "mqtt": MqttWriter,
    "omf": OmfWriter,
    "csv_per_sensor": SensorCsvWriter,
}


def load_config(filepath: str | Path) -> dict[str, Any]:
    with open(filepath, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    loaded = config or {}
    if not isinstance(loaded, dict):
        raise ConfigValidationError("Top-level config must be a YAML mapping")
    return loaded


def validate_config(config: dict[str, Any]) -> None:
    simulation = config.get("simulation", {})
    if simulation and not isinstance(simulation, dict):
        raise ConfigValidationError("simulation must be a mapping")

    tick_rate = float(simulation.get("tick_rate_sec", 0.5))
    if tick_rate <= 0:
        raise ConfigValidationError("simulation.tick_rate_sec must be > 0")

    backfill_days = float(simulation.get("backfill_days", 0))
    if backfill_days < 0:
        raise ConfigValidationError("simulation.backfill_days must be >= 0")

    run_mode = str(simulation.get("mode", "realtime")).lower()
    if run_mode not in {"realtime", "history"}:
        raise ConfigValidationError("simulation.mode must be 'realtime' or 'history'")

    writers = config.get("writers", [])
    if not isinstance(writers, list):
        raise ConfigValidationError("writers must be a list")
    for idx, writer_conf in enumerate(writers):
        if not isinstance(writer_conf, dict):
            raise ConfigValidationError(f"writers[{idx}] must be a mapping")
        writer_type = writer_conf.get("type")
        if writer_type not in WRITER_REGISTRY:
            raise ConfigValidationError(f"Unknown writer type in config: {writer_type}")

    assets = config.get("assets", [])
    if not isinstance(assets, list) or not assets:
        raise ConfigValidationError("assets must be a non-empty list")
    for idx, asset_conf in enumerate(assets):
        if not isinstance(asset_conf, dict):
            raise ConfigValidationError(f"assets[{idx}] must be a mapping")
        asset_name = asset_conf.get("name")
        asset_type = asset_conf.get("type")
        if not asset_name:
            raise ConfigValidationError(f"assets[{idx}] is missing name")
        if asset_type not in ASSET_REGISTRY:
            raise ConfigValidationError(f"Unknown asset type in config: {asset_type}")
        for sensor_conf in asset_conf.get("sensors", []):
            _validate_sensor_override(sensor_conf, context=f"asset {asset_name}")
        for charger_conf in asset_conf.get("chargers", []):
            if not charger_conf.get("name"):
                raise ConfigValidationError(f"charger on asset {asset_name} is missing name")
            for sensor_conf in charger_conf.get("sensors", []):
                _validate_sensor_override(sensor_conf, context=f"charger {charger_conf.get('name')}")


def _validate_sensor_override(sensor_conf: dict[str, Any], context: str) -> None:
    if not isinstance(sensor_conf, dict):
        raise ConfigValidationError(f"Sensor override on {context} must be a mapping")
    if not sensor_conf.get("name"):
        raise ConfigValidationError(f"Sensor override on {context} is missing name")
    interval = float(sensor_conf.get("interval", 1.0))
    if interval <= 0:
        raise ConfigValidationError(f"Sensor interval on {context} must be > 0")
    heartbeat = sensor_conf.get("heartbeat_interval")
    if heartbeat is not None and float(heartbeat) <= 0:
        raise ConfigValidationError(f"Sensor heartbeat_interval on {context} must be > 0")


def _apply_state_overrides(asset: Asset, state_overrides: dict[str, Any]) -> None:
    if not state_overrides:
        return
    for key, value in state_overrides.items():
        if key in asset.state:
            asset.state[key] = value
        else:
            logger.warning("Unknown state override '%s' on %s", key, asset.name)


def _apply_sensor_overrides(asset: Asset, sensor_overrides: list[dict[str, Any]]) -> None:
    if not sensor_overrides:
        return

    sensor_dict = {s.name: s for s in asset.sensors}
    for s_conf in sensor_overrides:
        s_name = s_conf["name"]
        if s_name in sensor_dict:
            sensor = sensor_dict[s_name]
            sensor.update_interval_sec = float(s_conf.get("interval", sensor.update_interval_sec))
            sensor.jitter_sec = float(s_conf.get("jitter", sensor.jitter_sec))
            if "emit_on_change" in s_conf:
                sensor.emit_on_change = bool(s_conf["emit_on_change"])
            if "heartbeat_interval" in s_conf:
                heartbeat = s_conf.get("heartbeat_interval")
                sensor.heartbeat_interval_sec = None if heartbeat is None else float(heartbeat)
            elif sensor.emit_on_change and sensor.heartbeat_interval_sec is None:
                sensor.heartbeat_interval_sec = sensor.update_interval_sec
            sensor.set_next_update(datetime.min, emitted=False)
        else:
            logger.warning("Unknown sensor override '%s' on %s", s_name, asset.name)


def build_simulation_components(
    config: dict[str, Any], project_root: Path
) -> tuple[list[Asset], list[Writer], float]:
    validate_config(config)

    writers: list[Writer] = []
    for w_conf in config.get("writers", []):
        w_type = w_conf["type"]
        w_kwargs = dict(w_conf.get("config", {}))
        fail_open = bool(w_kwargs.pop("fail_open", True))

        if "output_dir" in w_kwargs:
            out_dir = Path(w_kwargs["output_dir"])
            if not out_dir.is_absolute():
                w_kwargs["output_dir"] = str(project_root / out_dir)

        if w_type == "mqtt":
            w_kwargs["host"] = os.getenv("MQTT_HOST", w_kwargs.get("host", "localhost"))

        writer_class = WRITER_REGISTRY[w_type]
        try:
            writers.append(writer_class(**w_kwargs))
        except Exception as exc:
            if fail_open:
                logger.warning("Writer '%s' could not be initialized and will be disabled: %s", w_type, exc)
                continue
            raise

    assets: list[Asset] = []
    for a_conf in config.get("assets", []):
        a_name = a_conf["name"]
        a_type = a_conf["type"]
        a_state_overrides = a_conf.get("state", {})
        a_sensor_overrides = a_conf.get("sensors", [])

        if a_type == "ChargingSite":
            max_q = int(a_conf.get("max_queue", 3))
            site = ChargingSite(name=a_name, max_queue=max_q)
            _apply_state_overrides(site, a_state_overrides)
            _apply_sensor_overrides(site, a_sensor_overrides)

            for c_conf in a_conf.get("chargers", []):
                charger = EVCharger(name=c_conf["name"])
                _apply_state_overrides(charger, c_conf.get("state", {}))
                _apply_sensor_overrides(charger, c_conf.get("sensors", []))

                if "anomalies" in c_conf:
                    charger.scheduled_anomalies = list(c_conf["anomalies"])
                if "random_anomalies" in c_conf:
                    charger.random_anomaly_config.update(c_conf["random_anomalies"])

                site.add_charger(charger)

            assets.append(site)
        else:
            asset_class = ASSET_REGISTRY[a_type]
            asset = asset_class(name=a_name)
            _apply_state_overrides(asset, a_state_overrides)
            _apply_sensor_overrides(asset, a_sensor_overrides)
            assets.append(asset)

    tick_rate = float(config.get("simulation", {}).get("tick_rate_sec", 0.5))
    return assets, writers, tick_rate
