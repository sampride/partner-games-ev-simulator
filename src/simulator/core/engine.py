import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from simulator.models.base import Asset
from simulator.utils.state import StateManager
from simulator.writers.base import Writer

logger = logging.getLogger("simulator.engine")


class SimulationEngine:
    def __init__(
        self,
        assets: list[Asset],
        writers: list[Writer],
        state_manager: StateManager,
        tick_rate_sec: float = 0.5,
        backfill_days: int = 3,
        backfill_log_interval_sec: float = 300.0,
        realtime_log_interval_sec: float = 30.0,
        write_buffer_max_rows: int = 10000,
        write_buffer_max_age_sec: float = 2.0,
        history_mode: bool = False,
        history_end_time: datetime | None = None,
    ) -> None:
        self.assets = assets
        self.writers = writers
        self.state_manager = state_manager
        self.tick_rate_sec = tick_rate_sec
        self.backfill_log_interval_sec = backfill_log_interval_sec
        self.realtime_log_interval_sec = realtime_log_interval_sec
        self.write_buffer_max_rows = max(1, int(write_buffer_max_rows))
        self.write_buffer_max_age_sec = max(0.1, float(write_buffer_max_age_sec))
        self.history_mode = history_mode
        self.history_end_time = history_end_time
        self._write_buffer: list[dict[str, Any]] = []

        default_start = datetime.now() - timedelta(days=backfill_days)
        self.virtual_time = self.state_manager.load_cursor(default_start)

        for asset in self.assets:
            if hasattr(asset, "_refresh_next_sensor_due"):
                asset._refresh_next_sensor_due()

    async def _flush_buffer(self, is_backfilling: bool) -> None:
        if not self._write_buffer:
            return

        batch = self._write_buffer
        self._write_buffer = []
        write_tasks = []
        active_writers = 0
        for writer in self.writers:
            if is_backfilling and not writer.supports_backfill():
                continue
            if not is_backfilling and not writer.supports_realtime():
                continue
            active_writers += 1
            write_tasks.append(writer.write_batch(batch))
        if write_tasks:
            await asyncio.gather(*write_tasks)
        logger.debug("Flushed %d rows to %d writers", len(batch), active_writers)

    async def run(self) -> None:
        logger.info("Starting simulation. Virtual time=%s", self.virtual_time.isoformat())
        last_save_time = datetime.now()
        last_backfill_log_virtual = self.virtual_time
        last_realtime_log_real = datetime.now()
        last_buffer_flush_real = datetime.now()

        global_state: dict[str, float] = {
            "rainfall_mm_hr": 0.0,
            "ambient_temp_c": 20.0,
            "current_grid_voltage": 480.0,
        }

        try:
            while True:
                now = datetime.now()
                if self.history_mode:
                    is_backfilling = True
                    if self.history_end_time is not None and self.virtual_time >= self.history_end_time:
                        logger.info("History generation complete at virtual=%s", self.virtual_time.isoformat())
                        break
                else:
                    is_backfilling = self.virtual_time < now

                if self.virtual_time.day % 4 == 0 and 14 <= self.virtual_time.hour <= 16:
                    global_state["rainfall_mm_hr"] = 25.0
                else:
                    global_state["rainfall_mm_hr"] = 0.0
                global_state["is_backfilling"] = is_backfilling

                for asset in self.assets:
                    asset.tick(self.virtual_time, self.tick_rate_sec, global_state)

                tick_rows = 0
                for asset in self.assets:
                    rows = asset.flush_data()
                    tick_rows += len(rows)
                    self._write_buffer.extend(rows)

                real_now = datetime.now()
                buffer_age = (real_now - last_buffer_flush_real).total_seconds()
                if self._write_buffer and (
                    len(self._write_buffer) >= self.write_buffer_max_rows
                    or buffer_age >= self.write_buffer_max_age_sec
                ):
                    await self._flush_buffer(is_backfilling=is_backfilling)
                    last_buffer_flush_real = real_now

                if (real_now - last_save_time).total_seconds() >= 1.0:
                    self.state_manager.save_cursor(self.virtual_time)
                    last_save_time = real_now

                if is_backfilling:
                    if (self.virtual_time - last_backfill_log_virtual).total_seconds() >= self.backfill_log_interval_sec:
                        lag = (self.history_end_time - self.virtual_time) if self.history_mode and self.history_end_time else (now - self.virtual_time)
                        logger.info(
                            "Backfill progress virtual=%s lag=%s tick_rows=%d buffered_rows=%d",
                            self.virtual_time.isoformat(),
                            str(lag).split(".")[0],
                            tick_rows,
                            len(self._write_buffer),
                        )
                        last_backfill_log_virtual = self.virtual_time
                    self.virtual_time += timedelta(seconds=self.tick_rate_sec)
                else:
                    if (real_now - last_realtime_log_real).total_seconds() >= self.realtime_log_interval_sec:
                        logger.info(
                            "Realtime heartbeat virtual=%s tick_rows=%d buffered_rows=%d writers=%d",
                            self.virtual_time.isoformat(),
                            tick_rows,
                            len(self._write_buffer),
                            len(self.writers),
                        )
                        last_realtime_log_real = real_now
                    self.virtual_time = datetime.now()
                    await asyncio.sleep(self.tick_rate_sec)
        finally:
            await self._flush_buffer(is_backfilling=self.history_mode or self.virtual_time < datetime.now())
            await asyncio.gather(*(writer.flush() for writer in self.writers), return_exceptions=True)
            await asyncio.gather(*(writer.close() for writer in self.writers), return_exceptions=True)
