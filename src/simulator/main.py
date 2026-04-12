import asyncio
from pathlib import Path

from simulator.core.engine import SimulationEngine
from simulator.utils.config_parser import load_config, build_simulation_components
from simulator.utils.state import StateManager

async def main() -> None:

    print("Initializing Partner Games Simulator...")

    # Define paths
    project_root = Path(__file__).parent.parent.parent
    config_path = project_root / "config" / "default_sim.yaml"
    state_file_path = project_root / "data" / "simulator_cursor.json"

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
        print("\nSimulation stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())