import csv
from pathlib import Path
from typing import Any


class SensorCsvWriter:
    def __init__(
        self,
        output_dir: str,
        allow_backfill: bool = True,
        allow_realtime: bool = True,
        max_size_mb: float = 50.0,
    ) -> None:
        self.base_output_dir = Path(output_dir)
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.max_bytes = max_size_mb * 1024 * 1024

    def _get_filepath(self, directory: Path, base_name: str, date_compact: str) -> Path:
        """
        Generates filepaths like:
        site_melbourne_north.site_total_power_kw_20260413.csv
        site_melbourne_north.site_total_power_kw_20260413_1.csv
        """
        counter = 0
        while True:
            suffix = f"_{counter}" if counter > 0 else ""
            filename = f"{base_name}_{date_compact}{suffix}.csv"
            filepath = directory / filename

            if not filepath.exists():
                return filepath

            if filepath.stat().st_size < self.max_bytes:
                return filepath

            counter += 1

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        # Group by a composite key: (Date_Directory, Date_Compact, Asset_Name, Sensor_Name)
        batches: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        for row in data:
            ts = str(row["timestamp"])
            date_dir = ts[:10]  # "2026-04-13"
            date_compact = date_dir.replace("-", "")  # "20260413"

            # Lowercase for cleaner filenames
            asset_name = str(row.get("asset", "unknown")).lower()
            sensor_name = str(row.get("sensor", "unknown")).lower()

            key = (date_dir, date_compact, asset_name, sensor_name)
            if key not in batches:
                batches[key] = []
            batches[key].append(row)

        # Write the narrowed data to separate files
        for (date_dir, date_compact, asset_name, sensor_name), rows in batches.items():
            daily_dir = self.base_output_dir / date_dir
            daily_dir.mkdir(parents=True, exist_ok=True)

            base_name = f"{asset_name}.{sensor_name}"
            filepath = self._get_filepath(daily_dir, base_name, date_compact)

            headers_written = filepath.exists()

            with open(filepath, mode="a", newline="", encoding="utf-8") as f:
                # Switch to a standard CSV writer since we are dropping the dictionary keys
                writer = csv.writer(f)

                if not headers_written:
                    # Write the two-column header
                    writer.writerow(["timestamp", "value"])

                for r in rows:
                    # Strip out 'asset' and 'sensor', writing only the core data
                    writer.writerow([r["timestamp"], r["value"]])

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime