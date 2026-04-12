import asyncio
from datetime import datetime, timedelta

from simulator.models.base import Asset
from simulator.utils.state import StateManager
from simulator.writers.base import Writer

class SimulationEngine:
    def __init__(
            self,
            assets: list[Asset],
            writers: list[Writer],
            state_manager: StateManager,
            tick_rate_sec: float = 0.5,
            backfill_days: int = 3
    ) -> None:
        self.assets = assets
        self.writers = writers
        self.state_manager = state_manager
        self.tick_rate_sec = tick_rate_sec

        # Calculate the default start time if no cursor exists
        default_start = datetime.now() - timedelta(days=backfill_days)
        self.virtual_time = self.state_manager.load_cursor(default_start)

    async def run(self) -> None:
        print(f"Starting simulation. Virtual time: {self.virtual_time}")
        last_save_time = datetime.now()

        # The Global Environment State
        global_state = {
            "rainfall_mm_hr": 0.0,
            "ambient_temp_c": 20.0
        }

        while True:
            now = datetime.now()
            is_backfilling = self.virtual_time < now

            # Simple weather simulation: a storm every few days
            if self.virtual_time.day % 4 == 0 and 14 <= self.virtual_time.hour <= 16:
                global_state["rainfall_mm_hr"] = 25.0 # Heavy rain
            else:
                global_state["rainfall_mm_hr"] = 0.0

            # 1. Tick all assets (Pass the global state!)
            for asset in self.assets:
                asset.tick(self.virtual_time, self.tick_rate_sec, global_state)

            # ... (Rest of the engine loop remains unchanged)

            # 2. Collect generated data
            batch = []
            for asset in self.assets:
                batch.extend(asset.flush_data())

            # 3. Write data
            if batch:
                write_tasks = []
                for writer in self.writers:
                    # Skip writers that don't want historical data
                    if is_backfilling and not writer.supports_backfill():
                        continue
                    if not is_backfilling and not writer.supports_realtime():
                        continue

                    write_tasks.append(writer.write_batch(batch))

                if write_tasks:
                    await asyncio.gather(*write_tasks)

            # 4. Save state (Throttle saves to disk to once per real-time second)
            real_now = datetime.now()
            if (real_now - last_save_time).total_seconds() >= 1.0:
                self.state_manager.save_cursor(self.virtual_time)
                last_save_time = real_now

            # 5. Advance time
            if is_backfilling:
                self.virtual_time += timedelta(seconds=self.tick_rate_sec)
            else:
                self.virtual_time = datetime.now()
                await asyncio.sleep(self.tick_rate_sec)