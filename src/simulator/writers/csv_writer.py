import csv
import os
from pathlib import Path
from typing import Any


class CsvWriter:
    def __init__(
        self,
        output_dir: str,
        filename: str,
        allow_backfill: bool = True,
        allow_realtime: bool = True,
        max_size_mb: float = 50.0,  # Default 50MB rollover
    ) -> None:
        self.base_output_dir = Path(output_dir)
        self.filename = filename
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.max_bytes = max_size_mb * 1024 * 1024

    def _get_filepath(self, directory: Path, base_filename: str) -> Path:
        """Checks file sizes and returns a filepath with an incremented counter if needed."""
        stem = Path(base_filename).stem
        suffix = Path(base_filename).suffix
        counter = 0

        while True:
            # Append _1, _2, etc., only if counter > 0
            filename = f"{stem}_{counter}{suffix}" if counter > 0 else base_filename
            filepath = directory / filename

            if not filepath.exists():
                return filepath

            # If the file exists but is under the size limit, keep using it
            if filepath.stat().st_size < self.max_bytes:
                return filepath

            counter += 1

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        batches_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            date_str = str(row["timestamp"])[:10]
            if date_str not in batches_by_date:
                batches_by_date[date_str] = []
            batches_by_date[date_str].append(row)

        for date_str, rows in batches_by_date.items():
            daily_dir = self.base_output_dir / date_str
            daily_dir.mkdir(parents=True, exist_ok=True)

            # Use our new rollover helper
            filepath = self._get_filepath(daily_dir, self.filename)
            headers_written = filepath.exists()

            with open(filepath, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["timestamp", "asset", "sensor", "value"])
                if not headers_written:
                    writer.writeheader()
                writer.writerows(rows)

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool: return self.allow_realtime