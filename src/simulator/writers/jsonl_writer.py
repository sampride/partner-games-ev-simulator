import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, TextIO


class JsonlWriter:
    def __init__(
        self,
        output_dir: str,
        filename: str = "telemetry.jsonl",
        allow_backfill: bool = True,
        allow_realtime: bool = True,
        max_size_mb: float = 50.0,
        flush_every_batches: int = 8,
        include_stream_id: bool = True,
        stream_id_separator: str = ".",
    ) -> None:
        self.base_output_dir = Path(output_dir)
        self.filename = filename
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.max_bytes = int(max_size_mb * 1024 * 1024)
        self.flush_every_batches = max(1, int(flush_every_batches))
        self.include_stream_id = include_stream_id
        self.stream_id_separator = stream_id_separator
        self._open_files: dict[Path, TextIO] = {}
        self._pending_batches = 0

    def _get_filepath(self, directory: Path, base_filename: str) -> Path:
        stem = Path(base_filename).stem
        suffix = Path(base_filename).suffix or ".jsonl"
        counter = 0

        while True:
            filename = f"{stem}_{counter}{suffix}" if counter > 0 else f"{stem}{suffix}"
            filepath = directory / filename

            if not filepath.exists():
                return filepath
            if filepath.stat().st_size < self.max_bytes:
                return filepath
            counter += 1

    def _get_file(self, filepath: Path) -> TextIO:
        existing = self._open_files.get(filepath)
        if existing is not None:
            return existing

        filepath.parent.mkdir(parents=True, exist_ok=True)
        handle = open(filepath, mode="a", encoding="utf-8")
        self._open_files[filepath] = handle
        return handle

    def _build_stream_id(self, asset: str, sensor: str) -> str:
        safe_asset = asset.replace("/", self.stream_id_separator)
        return f"{safe_asset}{self.stream_id_separator}{sensor}"

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return value

    def _transform_row(self, row: dict[str, Any]) -> dict[str, Any]:
        asset = str(row.get("asset", "unknown_asset"))
        sensor = str(row.get("sensor", "unknown_sensor"))
        out: dict[str, Any] = {
            "timestamp": self._serialize_value(row.get("timestamp")),
            "asset": asset,
            "sensor": sensor,
            "value": self._serialize_value(row.get("value")),
        }
        if self.include_stream_id:
            out["stream_id"] = self._build_stream_id(asset, sensor)
        return out

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        batches_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            timestamp = self._serialize_value(row.get("timestamp"))
            date_str = str(timestamp)[:10]
            batches_by_date.setdefault(date_str, []).append(self._transform_row(row))

        for date_str, rows in batches_by_date.items():
            daily_dir = self.base_output_dir / date_str
            filepath = self._get_filepath(daily_dir, self.filename)
            handle = self._get_file(filepath)
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")

        self._pending_batches += 1
        if self._pending_batches >= self.flush_every_batches:
            await self.flush()

    async def flush(self) -> None:
        for handle in self._open_files.values():
            handle.flush()
        self._pending_batches = 0

    async def close(self) -> None:
        await self.flush()
        for handle in self._open_files.values():
            handle.close()
        self._open_files.clear()

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime
