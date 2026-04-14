import asyncio
import logging
from datetime import datetime, timedelta

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
    ) -> None:
        self.assets = assets
        self.writers = writers
        self.state_manager = state_manager
        self.tick_rate_sec = tick_rate_sec
        self.backfill_log_interval_sec = backfill_log_interval_sec
        self.realtime_log_interval_sec = realtime_log_interval_sec

        default_start = datetime.now() - timedelta(days=backfill_days)
        self.virtual_time = self.state_manager.load_cursor(default_start)

    async def run(self) -> None:
        logger.info("Starting simulation. Virtual time=%s", self.virtual_time.isoformat())
        last_save_time = datetime.now()
        last_backfill_log_virtual = self.virtual_time
        last_realtime_log_real = datetime.now()

        global_state: dict[str, float] = {
            "rainfall_mm_hr": 0.0,
            "ambient_temp_c": 20.0,
            "current_grid_voltage": 480.0,
        }

        while True:
            now = datetime.now()
            is_backfilling = self.virtual_time < now

            if self.virtual_time.day % 4 == 0 and 14 <= self.virtual_time.hour <= 16:
                global_state["rainfall_mm_hr"] = 25.0
            else:
                global_state["rainfall_mm_hr"] = 0.0

            for asset in self.assets:
                asset.tick(self.virtual_time, self.tick_rate_sec, global_state)

            batch: list[dict[str, object]] = []
            for asset in self.assets:
                batch.extend(asset.flush_data())

            if batch:
                write_tasks = []
                for writer in self.writers:
                    if is_backfilling and not writer.supports_backfill():
                        continue
                    if not is_backfilling and not writer.supports_realtime():
                        continue
                    write_tasks.append(writer.write_batch(batch))
                if write_tasks:
                    await asyncio.gather(*write_tasks)

            real_now = datetime.now()
            if (real_now - last_save_time).total_seconds() >= 1.0:
                self.state_manager.save_cursor(self.virtual_time)
                last_save_time = real_now

            if is_backfilling:
                if (self.virtual_time - last_backfill_log_virtual).total_seconds() >= self.backfill_log_interval_sec:
                    lag = now - self.virtual_time
                    logger.info(
                        "Backfill progress virtual=%s lag=%s batch=%d",
                        self.virtual_time.isoformat(),
                        str(lag).split(".")[0],
                        len(batch),
                    )
                    last_backfill_log_virtual = self.virtual_time
                self.virtual_time += timedelta(seconds=self.tick_rate_sec)
            else:
                if (real_now - last_realtime_log_real).total_seconds() >= self.realtime_log_interval_sec:
                    logger.info(
                        "Realtime heartbeat virtual=%s batch=%d writers=%d",
                        self.virtual_time.isoformat(),
                        len(batch),
                        len(self.writers),
                    )
                    last_realtime_log_real = real_now
                self.virtual_time = datetime.now()
                await asyncio.sleep(self.tick_rate_sec)
