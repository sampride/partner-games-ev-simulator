from __future__ import annotations

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
        backfill_days: float = 3.0,
        backfill_log_interval_sec: float = 300.0,
        realtime_log_interval_sec: float = 30.0,
        write_buffer_max_rows: int = 10000,
        write_buffer_max_age_sec: float = 2.0,
        history_mode: bool = False,
        history_end_time: datetime | None = None,
    ) -> None:
        self.assets = assets
        self.writers = list(writers)
        self.state_manager = state_manager
        self.tick_rate_sec = tick_rate_sec
        self.backfill_log_interval_sec = backfill_log_interval_sec
        self.realtime_log_interval_sec = realtime_log_interval_sec
        self.write_buffer_max_rows = max(1, int(write_buffer_max_rows))
        self.write_buffer_max_age_sec = max(0.1, float(write_buffer_max_age_sec))
        self.history_mode = history_mode
        self.history_end_time = history_end_time
        self._write_buffer: list[dict[str, Any]] = []
        self._disabled_writers: set[int] = set()
        self._realtime_immediate_writer_indexes: set[int] = {
            idx for idx, writer in enumerate(self.writers) if getattr(writer, "prefer_realtime_immediate", False)
        }
        self._tick_row_accumulator = 0
        self._tick_count = 0
        self._max_tick_rows = 0

        default_start = datetime.now() - timedelta(days=backfill_days)
        self.virtual_time = self.state_manager.load_runtime_state(self.assets, default_start)

        for asset in self.assets:
            if hasattr(asset, "_refresh_next_sensor_due"):
                asset._refresh_next_sensor_due()

    async def _write_with_writer(self, writer_index: int, writer: Writer, batch: list[dict[str, Any]]) -> None:
        try:
            await writer.write_batch(batch)
        except Exception as exc:
            self._disabled_writers.add(writer_index)
            logger.exception("Disabling writer %s after batch failure: %s", writer.__class__.__name__, exc)
            try:
                await writer.close()
            except Exception:
                logger.debug("Ignored error while closing failed writer %s", writer.__class__.__name__, exc_info=True)

    async def _write_realtime_immediate(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        write_tasks = []
        for index in sorted(self._realtime_immediate_writer_indexes):
            if index in self._disabled_writers:
                continue
            writer = self.writers[index]
            if not writer.supports_realtime():
                continue
            write_tasks.append(self._write_with_writer(index, writer, rows))
        if write_tasks:
            await asyncio.gather(*write_tasks)

    async def _flush_buffer(self, is_backfilling: bool) -> None:
        if not self._write_buffer:
            return

        batch = self._write_buffer
        self._write_buffer = []
        write_tasks = []
        active_writers = 0
        for index, writer in enumerate(self.writers):
            if index in self._disabled_writers:
                continue
            if not is_backfilling and index in self._realtime_immediate_writer_indexes:
                continue
            if is_backfilling and not writer.supports_backfill():
                continue
            if not is_backfilling and not writer.supports_realtime():
                continue
            active_writers += 1
            write_tasks.append(self._write_with_writer(index, writer, batch))
        if write_tasks:
            await asyncio.gather(*write_tasks)
        logger.debug("Flushed %d rows to %d buffered writers", len(batch), active_writers)

    async def _flush_and_close_writers(self) -> None:
        close_tasks = []
        for index, writer in enumerate(self.writers):
            if index in self._disabled_writers:
                continue
            close_tasks.append(writer.flush())
            close_tasks.append(writer.close())
        results = await asyncio.gather(*close_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Writer shutdown encountered an error: %s", result)

    async def run(self) -> None:
        logger.info(
            "Starting simulation. Virtual time=%s mode=%s assets=%d writers=%d",
            self.virtual_time.isoformat(),
            "history" if self.history_mode else "realtime",
            len(self.assets),
            len(self.writers) - len(self._disabled_writers),
        )
        last_save_time = datetime.now()
        last_backfill_log_virtual = self.virtual_time
        last_realtime_log_real = datetime.now()
        last_buffer_flush_real = datetime.now()

        global_state: dict[str, float | bool] = {
            "rainfall_mm_hr": 0.0,
            "ambient_temp_c": 20.0,
            "current_grid_voltage": 480.0,
            "is_backfilling": False,
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
                realtime_immediate_rows: list[dict[str, Any]] = []
                for asset in self.assets:
                    rows = asset.flush_data()
                    tick_rows += len(rows)
                    if not rows:
                        continue
                    if not is_backfilling and self._realtime_immediate_writer_indexes:
                        realtime_immediate_rows.extend(rows)
                    self._write_buffer.extend(rows)

                if realtime_immediate_rows:
                    await self._write_realtime_immediate(realtime_immediate_rows)

                real_now = datetime.now()
                buffer_age = (real_now - last_buffer_flush_real).total_seconds()
                if self._write_buffer and (
                    len(self._write_buffer) >= self.write_buffer_max_rows
                    or buffer_age >= self.write_buffer_max_age_sec
                ):
                    await self._flush_buffer(is_backfilling=is_backfilling)
                    last_buffer_flush_real = real_now

                if (real_now - last_save_time).total_seconds() >= 1.0:
                    self.state_manager.save_runtime_state(self.virtual_time, self.assets)
                    last_save_time = real_now

                if is_backfilling:
                    self._tick_row_accumulator += tick_rows
                    self._tick_count += 1
                    self._max_tick_rows = max(self._max_tick_rows, tick_rows)

                    if (self.virtual_time - last_backfill_log_virtual).total_seconds() >= self.backfill_log_interval_sec:
                        lag = (self.history_end_time - self.virtual_time) if self.history_mode and self.history_end_time else (now - self.virtual_time)
                        # logger.info(
                        #     "Backfill progress virtual=%s lag=%s tick_rows=%d buffered_rows=%d active_writers=%d",
                        #     self.virtual_time.isoformat(),
                        #     str(lag).split(".")[0],
                        #     tick_rows,
                        #     len(self._write_buffer),
                        #     len(self.writers) - len(self._disabled_writers),
                        # )

                        avg_tick_rows = self._tick_row_accumulator / max(self._tick_count, 1)

                        logger.info(
                            f"Backfill progress virtual={self.virtual_time.isoformat()} "
                            f"lag={str(lag).split('.')[0]} "
                            f"tick_rows={tick_rows} "
                            f"avg_tick_rows={avg_tick_rows:.2f} "
                            f"max_tick_rows={self._max_tick_rows} "
                            f"buffered_rows={len(self._write_buffer)} "
                            f"active_writers={len(self.writers) - len(self._disabled_writers)}"
                        )

                        last_backfill_log_virtual = self.virtual_time
                    self.virtual_time += timedelta(seconds=self.tick_rate_sec)
                else:
                    if (real_now - last_realtime_log_real).total_seconds() >= self.realtime_log_interval_sec:
                        logger.info(
                            "Realtime heartbeat virtual=%s tick_rows=%d buffered_rows=%d active_writers=%d",
                            self.virtual_time.isoformat(),
                            tick_rows,
                            len(self._write_buffer),
                            len(self.writers) - len(self._disabled_writers),
                        )
                        last_realtime_log_real = real_now
                    self.virtual_time = datetime.now()
                    await asyncio.sleep(self.tick_rate_sec)
        finally:
            await self._flush_buffer(is_backfilling=self.history_mode or self.virtual_time < datetime.now())
            await self._flush_and_close_writers()
