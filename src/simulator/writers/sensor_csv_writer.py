import csv
from pathlib import Path
from typing import Any, TextIO


class SensorCsvWriter:
    def __init__(
        self,
        output_dir: str,
        allow_backfill: bool = True,
        allow_realtime: bool = True,
        max_size_mb: float = 50.0,
        flush_every_batches: int = 8,
    ) -> None:
        self.base_output_dir = Path(output_dir)
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.max_bytes = int(max_size_mb * 1024 * 1024)
        self.flush_every_batches = max(1, int(flush_every_batches))
        self._open_files: dict[Path, tuple[TextIO, csv.writer]] = {}
        self._pending_batches = 0

    def _get_filepath(self, directory: Path, base_name: str, date_compact: str) -> Path:
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

    def _get_writer(self, filepath: Path) -> csv.writer:
        existing = self._open_files.get(filepath)
        if existing is not None:
            return existing[1]

        filepath.parent.mkdir(parents=True, exist_ok=True)
        headers_written = filepath.exists() and filepath.stat().st_size > 0
        handle = open(filepath, mode="a", newline="", encoding="utf-8")
        writer = csv.writer(handle)
        if not headers_written:
            writer.writerow(["timestamp", "value"])
        self._open_files[filepath] = (handle, writer)
        return writer

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        batches: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        for row in data:
            ts = str(row["timestamp"])
            date_dir = ts[:10]
            date_compact = date_dir.replace("-", "")
            asset_name = str(row.get("asset", "unknown")).lower()
            sensor_name = str(row.get("sensor", "unknown")).lower()
            key = (date_dir, date_compact, asset_name, sensor_name)
            batches.setdefault(key, []).append(row)

        for (date_dir, date_compact, asset_name, sensor_name), rows in batches.items():
            daily_dir = self.base_output_dir / date_dir
            base_name = f"{asset_name}.{sensor_name}"
            filepath = self._get_filepath(daily_dir, base_name, date_compact)
            writer = self._get_writer(filepath)
            writer.writerows([[r["timestamp"], r["value"]] for r in rows])

        self._pending_batches += 1
        if self._pending_batches >= self.flush_every_batches:
            await self.flush()

    async def flush(self) -> None:
        for handle, _ in self._open_files.values():
            handle.flush()
        self._pending_batches = 0

    async def close(self) -> None:
        await self.flush()
        for handle, _ in self._open_files.values():
            handle.close()
        self._open_files.clear()

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime
