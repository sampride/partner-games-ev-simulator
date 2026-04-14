from __future__ import annotations

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
        self.backup_path = self.filepath.with_suffix(f"{self.filepath.suffix}.bak")

    def _read_json_file(self, path: Path) -> dict[str, Any] | None:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
                logger.warning("State file %s does not contain a JSON object", path)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Could not parse state file %s (%s)", path, exc)
        except OSError as exc:
            logger.warning("Could not read state file %s (%s)", path, exc)
        return None

    def _load_raw_state(self) -> dict[str, Any] | None:
        primary = self._read_json_file(self.filepath)
        if primary is not None:
            return primary
        backup = self._read_json_file(self.backup_path)
        if backup is not None:
            logger.warning("Recovered simulator state from backup file %s", self.backup_path)
            return backup
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
        try:
            if self.filepath.exists():
                self.filepath.replace(self.backup_path)
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            temp_file.replace(self.filepath)
        except OSError as exc:
            logger.exception("Failed to persist simulator state to %s (%s)", self.filepath, exc)
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except OSError:
                pass
