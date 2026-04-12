import json
from datetime import datetime
from pathlib import Path

class StateManager:
    def __init__(self, filepath: Path | str) -> None:
        self.filepath = Path(filepath)

    def load_cursor(self, default_start: datetime) -> datetime:
        """Load the last saved timestamp, or return the default if none exists."""
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "last_tick" in data:
                        return datetime.fromisoformat(data["last_tick"])
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Warning: Could not parse state file. Starting from default. ({e})")

        return default_start

    def save_cursor(self, current_time: datetime) -> None:
        """Save the current virtual time to disk atomically."""
        temp_file = self.filepath.with_suffix(".tmp")

        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump({"last_tick": current_time.isoformat()}, f)

        # Atomic replace ensures we don't corrupt the file on sudden power loss
        temp_file.replace(self.filepath)