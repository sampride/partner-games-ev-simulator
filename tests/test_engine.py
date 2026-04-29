import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

from simulator.core.engine import SimulationEngine
from simulator.models.base import Asset, SensorConfig
from simulator.utils.state import StateManager


class DummyAsset(Asset):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.sensors = [SensorConfig("x", 0.1)]
        self._refresh_next_sensor_due()
        self.state = {"v": 0.0}

    def update_internal_state(self, delta_sec: float, current_time: datetime, global_state: dict[str, object]) -> None:
        self.state["v"] += delta_sec

    def read_sensor(self, sensor_name: str, global_state: dict[str, object]) -> float:
        return self.state["v"]


class CaptureWriter:
    def __init__(self, allow_backfill=True, allow_realtime=True) -> None:
        self.rows = []
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime

    async def write_batch(self, data):
        self.rows.extend(data)

    async def flush(self):
        return

    async def close(self):
        return

    def supports_backfill(self):
        return self.allow_backfill

    def supports_realtime(self):
        return self.allow_realtime


def test_history_mode_stops(tmp_path: Path) -> None:
    asset = DummyAsset("A")
    writer = CaptureWriter()
    state = StateManager(filepath=tmp_path / "cursor.json")

    engine = SimulationEngine(
        assets=[asset],
        writers=[writer],
        state_manager=state,
        tick_rate_sec=0.05,
        backfill_days=0,
        write_buffer_max_rows=1,
        history_mode=True,
        history_end_time=datetime.now() + timedelta(seconds=0.25),
    )

    asyncio.run(engine.run())
    assert writer.rows


from simulator.models.base import Asset, SensorConfig


class _ConstantDiscreteAsset(Asset):
    def __init__(self) -> None:
        super().__init__("Discrete")
        self.sensors = [
            SensorConfig("Status", 1.0, emit_on_change=True, heartbeat_interval_sec=5.0),
            SensorConfig("Analog", 1.0),
        ]
        self._refresh_next_sensor_due()
        self.state = {"status": 1, "analog": 10.0}

    def update_internal_state(self, delta_sec, current_time, global_state):
        return

    def read_sensor(self, sensor_name, global_state):
        return self.state["status"] if sensor_name == "Status" else self.state["analog"]


def test_on_change_sensor_emits_once_with_heartbeat() -> None:
    from datetime import datetime, timedelta

    asset = _ConstantDiscreteAsset()
    now = datetime(2026, 1, 1, 0, 0, 0)
    rows_by_tick = []
    for step in range(6):
        current = now + timedelta(seconds=step)
        asset.tick(current, 1.0, {})
        rows_by_tick.append(asset.flush_data())

    assert [row["sensor"] for row in rows_by_tick[0]] == ["Status", "Analog"]
    for step in range(1, 5):
        assert rows_by_tick[step] == [
            {
                "timestamp": rows_by_tick[step][0]["timestamp"],
                "asset": "Discrete",
                "sensor": "Analog",
                "data_type": "double",
                "value": 10.0,
            }
        ]
    assert [row["sensor"] for row in rows_by_tick[5]] == ["Status", "Analog"]


def test_backfill_suppresses_unimportant_state_transition_logs(caplog) -> None:
    from datetime import datetime
    from simulator.models.ev_charger import EVCharger

    charger = EVCharger("Test")
    charger.start_session(1800)
    charger.state["charger_state"] = EVCharger.STATE_HANDSHAKING
    charger.state["session_duration_sec"] = 3.9

    with caplog.at_level("INFO"):
        charger.update_internal_state(0.2, datetime(2026, 1, 1, 12, 0, 0), {"ambient_temp_c": 22.0, "current_grid_voltage": 480.0, "is_backfilling": True})

    assert not any("state 1 -> 2" in message for message in caplog.messages)


def test_backfill_suppresses_important_state_and_anomaly_logs(caplog) -> None:
    from datetime import datetime
    from simulator.models.ev_charger import EVCharger

    charger = EVCharger("Test")
    charger.start_session(1800)
    charger.state["charger_state"] = EVCharger.STATE_CHARGING
    charger.scheduled_anomalies = [
        {"type": "PUMP_DEGRADATION", "start_sec": 0, "duration_sec": 1200}
    ]
    charger.state["power_module_temp_c"] = 73.0

    with caplog.at_level("INFO"):
        charger.update_internal_state(
            0.2,
            datetime(2026, 1, 1, 12, 0, 0),
            {
                "ambient_temp_c": 22.0,
                "current_grid_voltage": 480.0,
                "is_backfilling": True,
            },
        )

    assert not any("state" in message or "anomaly" in message for message in caplog.messages)


def test_writer_failure_isolated(tmp_path: Path) -> None:
    class FailingWriter(CaptureWriter):
        async def write_batch(self, data):
            raise RuntimeError("boom")

    asset = DummyAsset("A")
    good = CaptureWriter()
    bad = FailingWriter()
    state = StateManager(filepath=tmp_path / "cursor.json")

    engine = SimulationEngine(
        assets=[asset],
        writers=[bad, good],
        state_manager=state,
        tick_rate_sec=0.05,
        backfill_days=0,
        write_buffer_max_rows=1,
        history_mode=True,
        history_end_time=datetime.now() + timedelta(seconds=0.15),
    )

    asyncio.run(engine.run())
    assert good.rows


def test_backfill_does_not_buffer_when_no_writer_supports_backfill(tmp_path: Path) -> None:
    asset = DummyAsset("A")
    writer = CaptureWriter(allow_backfill=False, allow_realtime=True)
    state = StateManager(filepath=tmp_path / "cursor.json")

    engine = SimulationEngine(
        assets=[asset],
        writers=[writer],
        state_manager=state,
        tick_rate_sec=0.05,
        backfill_days=0,
        write_buffer_max_rows=1,
        history_mode=True,
        history_end_time=datetime.now() + timedelta(seconds=0.2),
    )

    asyncio.run(engine.run())

    assert writer.rows == []
    assert engine._write_buffer == []


def test_writer_shutdown_flushes_before_close(tmp_path: Path) -> None:
    class OrderedWriter(CaptureWriter):
        def __init__(self) -> None:
            super().__init__()
            self.calls = []

        async def flush(self):
            self.calls.append("flush")

        async def close(self):
            self.calls.append("close")

    writer = OrderedWriter()
    engine = SimulationEngine(
        assets=[],
        writers=[writer],
        state_manager=StateManager(filepath=tmp_path / "cursor.json"),
        tick_rate_sec=0.05,
        backfill_days=0,
        history_mode=True,
        history_end_time=datetime.now(),
    )

    asyncio.run(engine._flush_and_close_writers())

    assert writer.calls == ["flush", "close"]


def test_backfill_metrics_log_includes_throughput(caplog, tmp_path: Path) -> None:
    engine = SimulationEngine(
        assets=[],
        writers=[],
        state_manager=StateManager(filepath=tmp_path / "cursor.json"),
        tick_rate_sec=0.05,
        backfill_days=0,
        history_mode=True,
        history_end_time=datetime.now(),
    )
    engine._last_tick_rows = 9
    engine._tick_count = 20
    engine._tick_row_accumulator = 180
    engine._max_tick_rows = 139
    engine._rows_generated_total = 180
    engine._writer_target_rows_total = 360
    engine._buffer_flush_max_seconds = 0.25

    with caplog.at_level(logging.INFO, logger="simulator.engine"):
        engine._log_backfill_progress(
            lag=timedelta(seconds=100),
            window_virtual_seconds=50.0,
            window_real_seconds=2.0,
            window_ticks=1000,
            window_rows=9000,
            window_flushes=3,
            window_flush_rows=9000,
            window_flush_seconds=0.5,
        )

    message = caplog.messages[-1]
    assert "speed=25.0x" in message
    assert "rows_per_sec=4500" in message
    assert "ticks_per_sec=500" in message
    assert "flush_time_pct=25.0" in message
    assert "writer_target_rows=360" in message


class ImmediateCaptureWriter(CaptureWriter):
    prefer_realtime_immediate = True


class _AlwaysEmittingAsset(Asset):
    def __init__(self) -> None:
        super().__init__("Always")
        self.sensors = [SensorConfig("x", 0.05)]
        self._refresh_next_sensor_due()
        self.state = {"v": 0.0}

    def update_internal_state(self, delta_sec: float, current_time: datetime, global_state: dict[str, object]) -> None:
        self.state["v"] += 1.0

    def read_sensor(self, sensor_name: str, global_state: dict[str, object]) -> float:
        return self.state["v"]


def test_realtime_immediate_writer_bypasses_shared_buffer(tmp_path: Path) -> None:
    class BatchCountingWriter(CaptureWriter):
        def __init__(self) -> None:
            super().__init__()
            self.batch_count = 0

        async def write_batch(self, data):
            self.batch_count += 1
            await super().write_batch(data)

    asset = _AlwaysEmittingAsset()
    immediate = ImmediateCaptureWriter()
    immediate.batch_count = 0

    async def immediate_write(data):
        immediate.batch_count += 1
        immediate.rows.extend(data)

    immediate.write_batch = immediate_write

    buffered = BatchCountingWriter()
    state = StateManager(filepath=tmp_path / "cursor.json")

    engine = SimulationEngine(
        assets=[asset],
        writers=[immediate, buffered],
        state_manager=state,
        tick_rate_sec=0.05,
        backfill_days=0,
        write_buffer_max_rows=1000,
        write_buffer_max_age_sec=1000.0,
        history_mode=False,
    )
    engine.virtual_time = datetime.now()

    async def _run_short():
        try:
            await asyncio.wait_for(engine.run(), timeout=0.18)
        except asyncio.TimeoutError:
            pass

    asyncio.run(_run_short())

    assert immediate.batch_count > 1
    assert buffered.batch_count == 1
    assert immediate.rows
    assert buffered.rows
