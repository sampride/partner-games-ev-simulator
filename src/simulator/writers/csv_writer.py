import csv
from pathlib import Path
from typing import Any

class CsvWriter:
    def __init__(self, output_dir: str, filename: str, allow_backfill: bool = True,allow_realtime: bool = True) -> None:
        self.base_output_dir = Path(output_dir)
        self.filename = filename
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        # Group data by date string (e.g., "2026-03-04") to handle rollover efficiently
        batches_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            # Extract YYYY-MM-DD from the ISO format timestamp
            date_str = str(row["timestamp"])[:10]
            if date_str not in batches_by_date:
                batches_by_date[date_str] = []
            batches_by_date[date_str].append(row)

        # Write each date's data to its respective folder
        for date_str, rows in batches_by_date.items():
            daily_dir = self.base_output_dir / date_str
            daily_dir.mkdir(parents=True, exist_ok=True)
            filepath = daily_dir / self.filename

            headers_written = filepath.exists()

            with open(filepath, mode="a", newline="", encoding="utf-8") as f:
                # We enforce our strict narrow schema here
                writer = csv.DictWriter(f, fieldnames=["timestamp", "asset", "sensor", "value"])

                if not headers_written:
                    writer.writeheader()

                writer.writerows(rows)

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime