import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from simulator.models.base import Asset

logger = logging.getLogger("simulator.state")


class StateManager:
    def __init__(self, filepath: Path | str) -> None:
        self.filepath = Path(filepath)

    def _load_raw_state(self) -> dict[str, Any] | None:
        if not self.filepath.exists():
            return None
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Could not parse state file %s. Starting from default. (%s)", self.filepath, exc)
        return None

    def load_cursor(self, default_start: datetime) -> datetime:
        data = self._load_raw_state()
        if data and "last_tick" in data:
            try:
                loaded = datetime.fromisoformat(str(data["last_tick"]))
                logger.info("Loaded simulator cursor %s from %s", loaded.isoformat(), self.filepath)
                return loaded
            except ValueError as exc:
                logger.warning("Invalid last_tick in state file %s (%s)", self.filepath, exc)
        return default_start

    def load_runtime_state(self, assets: list[Asset], default_start: datetime) -> datetime:
        data = self._load_raw_state()
        if not data:
            return default_start

        virtual_time = default_start
        if "last_tick" in data:
            try:
                virtual_time = datetime.fromisoformat(str(data["last_tick"]))
                logger.info("Loaded simulator cursor %s from %s", virtual_time.isoformat(), self.filepath)
            except ValueError as exc:
                logger.warning("Invalid last_tick in state file %s (%s)", self.filepath, exc)

        asset_snapshots = data.get("assets", [])
        if isinstance(asset_snapshots, list) and asset_snapshots:
            self._restore_assets(assets, asset_snapshots)
            logger.info("Loaded runtime state for %d top-level assets from %s", len(asset_snapshots), self.filepath)

        return virtual_time

    def _restore_assets(self, assets: list[Asset], snapshots: list[dict[str, Any]]) -> None:
        asset_map = {asset.name: asset for asset in assets}
        for snapshot in snapshots:
            name = str(snapshot.get("name", ""))
            asset = asset_map.get(name)
            if asset is None:
                logger.warning("State file contains unknown asset snapshot '%s'", name)
                continue
            asset.restore_runtime_state(snapshot)
            children = asset.get_child_assets()
            child_snapshots = snapshot.get("children", [])
            if children and isinstance(child_snapshots, list):
                self._restore_assets(children, child_snapshots)

    def save_cursor(self, current_time: datetime) -> None:
        self.save_runtime_state(current_time, [])

    def save_runtime_state(self, current_time: datetime, assets: list[Asset]) -> None:
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        temp_file = self.filepath.with_suffix(".tmp")
        payload = {
            "version": 2,
            "last_tick": current_time.isoformat(),
            "assets": [asset.snapshot_runtime_state() for asset in assets],
        }
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        temp_file.replace(self.filepath)
