import asyncio
import logging
import os
from pathlib import Path

from simulator.core.engine import SimulationEngine
from simulator.utils.config_parser import build_simulation_components, load_config
from simulator.utils.state import StateManager

log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("simulator.main")


async def main() -> None:
    logger.info("Initializing Partner Games Simulator")

    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent
    config_path = Path(os.getenv("SIM_CONFIG_PATH", project_root / "config" / "default_sim.yaml"))
    data_dir = Path(os.getenv("SIM_DATA_PATH", project_root / "data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    state_file_path = data_dir / "simulator_cursor.json"

    logger.info("Loading configuration from %s", config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Could not find config file at {config_path}")

    config_dict = load_config(config_path)
    sim_config = config_dict.get("simulation", {})
    backfill_days = int(sim_config.get("backfill_days", 3))
    tick_rate = float(sim_config.get("tick_rate_sec", 0.5))
    backfill_log_interval_sec = float(sim_config.get("backfill_log_interval_sec", 300.0))
    realtime_log_interval_sec = float(sim_config.get("realtime_log_interval_sec", 30.0))

    assets, writers, _ = build_simulation_components(config_dict, project_root)
    state_manager = StateManager(filepath=state_file_path)

    engine = SimulationEngine(
        assets=assets,
        writers=writers,
        state_manager=state_manager,
        tick_rate_sec=tick_rate,
        backfill_days=backfill_days,
        backfill_log_interval_sec=backfill_log_interval_sec,
        realtime_log_interval_sec=realtime_log_interval_sec,
    )

    try:
        await engine.run()
    finally:
        engine.state_manager.save_cursor(engine.virtual_time)
        logger.info("Simulation cursor saved to %s", state_file_path)


if __name__ == "__main__":
    asyncio.run(main())
