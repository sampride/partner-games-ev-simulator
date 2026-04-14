import asyncio
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
    def __init__(self) -> None:
        self.rows = []

    async def write_batch(self, data):
        self.rows.extend(data)

    async def flush(self):
        return

    async def close(self):
        return

    def supports_backfill(self):
        return True

    def supports_realtime(self):
        return True


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
    assert rows_by_tick[1] == [{"timestamp": rows_by_tick[1][0]["timestamp"], "asset": "Discrete", "sensor": "Analog", "value": 10.0}]
    assert rows_by_tick[2] == [{"timestamp": rows_by_tick[2][0]["timestamp"], "asset": "Discrete", "sensor": "Analog", "value": 10.0}]
    assert rows_by_tick[3] == [{"timestamp": rows_by_tick[3][0]["timestamp"], "asset": "Discrete", "sensor": "Analog", "value": 10.0}]
    assert rows_by_tick[4] == [{"timestamp": rows_by_tick[4][0]["timestamp"], "asset": "Discrete", "sensor": "Analog", "value": 10.0}]
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
