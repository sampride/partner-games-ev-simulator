import os
import asyncio
import logging
from pathlib import Path

from simulator.core.engine import SimulationEngine
from simulator.utils.config_parser import load_config, build_simulation_components
from simulator.utils.state import StateManager

# 1. Configure the Central Logger
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("simulator.main")

async def main() -> None:

    logger.info("Initializing Partner Games Simulator...")

    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent

    # Use Environment Variables for paths, falling back to local dev folders
    config_path = Path(os.getenv("SIM_CONFIG_PATH", project_root / "config" / "default_sim.yaml"))
    data_dir = Path(os.getenv("SIM_DATA_PATH", project_root / "data"))

    # Ensure the data directory exists
    data_dir.mkdir(parents=True, exist_ok=True)
    state_file_path = data_dir / "simulator_cursor.json"

    logger.info(f"Loading configuration from {config_path}...")
    if not config_path.exists():
        logger.critical(f"CRITICAL: Could not find the config file at {config_path}")
        raise FileNotFoundError(f"CRITICAL: Could not find the config file at {config_path}")

    # Load configuration
    config_dict = load_config(config_path)

    # Extract simulation settings
    sim_config = config_dict.get("simulation", {})
    backfill_days = sim_config.get("backfill_days", 3)
    tick_rate = sim_config.get("tick_rate_sec", 0.5)

    # Build components
    assets, writers, _ = build_simulation_components(config_dict, project_root)

    # Initialize State Manager
    state_manager = StateManager(filepath=state_file_path)

    # Initialize and Run Engine
    engine = SimulationEngine(
        assets=assets,
        writers=writers,
        state_manager=state_manager,
        tick_rate_sec=tick_rate,
        backfill_days=backfill_days
    )

    try:
        await engine.run()
    except KeyboardInterrupt:
        # Save exact cursor on graceful exit
        engine.state_manager.save_cursor(engine.virtual_time)
        logger.info("Simulation stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())