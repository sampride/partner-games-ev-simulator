import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("simulator.state")


class StateManager:
    def __init__(self, filepath: Path | str) -> None:
        self.filepath = Path(filepath)

    def load_cursor(self, default_start: datetime) -> datetime:
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "last_tick" in data:
                        loaded = datetime.fromisoformat(data["last_tick"])
                        logger.info("Loaded simulator cursor %s from %s", loaded.isoformat(), self.filepath)
                        return loaded
            except (json.JSONDecodeError, ValueError) as exc:
                logger.warning(
                    "Could not parse state file %s. Starting from default. (%s)", self.filepath, exc
                )
        return default_start

    def save_cursor(self, current_time: datetime) -> None:
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        temp_file = self.filepath.with_suffix(".tmp")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump({"last_tick": current_time.isoformat()}, f)
        temp_file.replace(self.filepath)
