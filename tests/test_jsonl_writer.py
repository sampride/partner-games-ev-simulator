import asyncio
import json
from datetime import datetime
from pathlib import Path

from simulator.utils.config_parser import build_simulation_components, validate_config
from simulator.writers.jsonl_writer import JsonlWriter


def test_jsonl_writer_support_flags(tmp_path: Path) -> None:
    writer = JsonlWriter(
        output_dir=str(tmp_path),
        allow_backfill=False,
        allow_realtime=True,
    )

    assert writer.supports_backfill() is False
    assert writer.supports_realtime() is True


def test_jsonl_writer_empty_batch_does_not_create_files(tmp_path: Path) -> None:
    writer = JsonlWriter(output_dir=str(tmp_path))

    asyncio.run(writer.write_batch([]))
    asyncio.run(writer.close())

    assert not list(tmp_path.rglob("*"))


def test_jsonl_writer_writes_row_shape_and_stream_id(tmp_path: Path) -> None:
    writer = JsonlWriter(output_dir=str(tmp_path), filename="ev.jsonl", flush_every_batches=1)

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": datetime(2026, 4, 14, 22, 55, 29),
                    "asset": "AC/North/C01",
                    "sensor": "Output_Current_DC",
                    "value": 42.7,
                }
            ]
        )
    )
    asyncio.run(writer.close())

    output_file = tmp_path / "2026-04-14" / "ev.jsonl"
    row = json.loads(output_file.read_text(encoding="utf-8").strip())

    assert row == {
        "timestamp": "2026-04-14T22:55:29",
        "asset": "AC/North/C01",
        "sensor": "Output_Current_DC",
        "value": 42.7,
        "stream_id": "AC.North.C01.Output_Current_DC",
    }


def test_jsonl_writer_close_flushes(tmp_path: Path) -> None:
    writer = JsonlWriter(output_dir=str(tmp_path), filename="ev.jsonl", flush_every_batches=99)

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": "2026-04-14T00:00:00",
                    "asset": "AC.North.C01",
                    "sensor": "Output_Current_DC",
                    "value": 1.0,
                }
            ]
        )
    )
    asyncio.run(writer.close())

    assert (tmp_path / "2026-04-14" / "ev.jsonl").read_text(encoding="utf-8").strip()


def test_jsonl_writer_registered_in_config_factory(tmp_path: Path) -> None:
    config = {
        "simulation": {"tick_rate_sec": 0.1},
        "writers": [
            {"type": "jsonl", "config": {"output_dir": "jsonl", "filename": "ev.jsonl"}},
        ],
        "assets": [
            {
                "name": "SiteA",
                "type": "ChargingSite",
                "chargers": [{"name": "C1"}],
            }
        ],
    }

    validate_config(config)
    _, writers, _ = build_simulation_components(config, tmp_path)

    assert len(writers) == 1
    assert isinstance(writers[0], JsonlWriter)
