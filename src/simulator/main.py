import asyncio
import logging
import os
from datetime import datetime, timedelta
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


def _resolve_history_end_time(sim_config: dict[str, object]) -> datetime | None:
    run_mode = str(sim_config.get("mode", "realtime")).lower()
    if run_mode != "history":
        return None

    end_time_raw = sim_config.get("history_end_time")
    if end_time_raw:
        return datetime.fromisoformat(str(end_time_raw))

    duration_days = float(sim_config.get("history_duration_days", 0.0) or 0.0)
    if duration_days > 0:
        return datetime.now() + timedelta(days=duration_days)

    return datetime.now()


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
    write_buffer_max_rows = int(sim_config.get("write_buffer_max_rows", 10000))
    write_buffer_max_age_sec = float(sim_config.get("write_buffer_max_age_sec", 2.0))
    history_mode = str(sim_config.get("mode", "realtime")).lower() == "history"
    history_end_time = _resolve_history_end_time(sim_config)

    assets, writers, _ = build_simulation_components(config_dict, project_root)
    state_manager = StateManager(filepath=state_file_path)

    if history_mode:
        logger.info("History generation mode enabled. End time=%s", history_end_time.isoformat() if history_end_time else "immediate")

    engine = SimulationEngine(
        assets=assets,
        writers=writers,
        state_manager=state_manager,
        tick_rate_sec=tick_rate,
        backfill_days=backfill_days,
        backfill_log_interval_sec=backfill_log_interval_sec,
        realtime_log_interval_sec=realtime_log_interval_sec,
        write_buffer_max_rows=write_buffer_max_rows,
        write_buffer_max_age_sec=write_buffer_max_age_sec,
        history_mode=history_mode,
        history_end_time=history_end_time,
    )

    try:
        await engine.run()
    finally:
        engine.state_manager.save_cursor(engine.virtual_time)
        logger.info("Simulation cursor saved to %s", state_file_path)


if __name__ == "__main__":
    asyncio.run(main())
