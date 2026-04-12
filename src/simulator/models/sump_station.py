import math
import random
from datetime import datetime
from simulator.models.base import Asset, SensorConfig

class SumpStation(Asset):
    def __init__(self, name: str) -> None:
        super().__init__(name)

        # High frequency for electrical, lower for level and flow
        self.sensors = [
            SensorConfig("level_m", 5.0, 0.5),
            SensorConfig("pump_1_amps", 1.0, 0.1),
            SensorConfig("pump_2_amps", 1.0, 0.1),
            SensorConfig("discharge_flow_lps", 5.0, 0.5)
        ]

        self.state = {
            # Physical dimensions
            "sump_area_m2": 4.0,
            "level_m": 0.8,

            # Control setpoints
            "start_pump1_m": 1.5,
            "start_pump2_m": 2.0,
            "stop_all_m": 0.5,

            # Pump mechanical state
            "pump_1_running": False,
            "pump_2_running": False,
            "pump_capacity_lps": 15.0,
            "pump_1_efficiency": 1.0,  # 1.0 is healthy, drop this to simulate clogs
            "pump_2_efficiency": 1.0
        }

    def update_internal_state(self, delta_sec: float, current_time: datetime, global_state: dict) -> None:
        # 1. Calculate Inflow
        # Simulate a diurnal human usage curve (peaks at 8 AM and 7 PM)
        hour = current_time.hour + current_time.minute / 60.0
        base_inflow = 2.0 + math.sin(math.pi * (hour - 8) / 12) * 1.5 + math.sin(math.pi * (hour - 19) / 12) * 1.0
        base_inflow = max(0.5, base_inflow) # Minimum trickle

        # Add environmental rainfall from global state
        rainfall_mm_hr = global_state.get("rainfall_mm_hr", 0.0)
        storm_inflow = rainfall_mm_hr * 0.5

        total_inflow_lps = base_inflow + storm_inflow

        # 2. Control Logic (Float Switches)
        level = self.state["level_m"]
        if level > self.state["start_pump2_m"]:
            self.state["pump_1_running"] = True
            self.state["pump_2_running"] = True
        elif level > self.state["start_pump1_m"]:
            self.state["pump_1_running"] = True
        elif level < self.state["stop_all_m"]:
            self.state["pump_1_running"] = False
            self.state["pump_2_running"] = False

        # 3. Calculate Outflow
        outflow_lps = 0.0
        if self.state["pump_1_running"]:
            outflow_lps += self.state["pump_capacity_lps"] * self.state["pump_1_efficiency"]
        if self.state["pump_2_running"]:
            outflow_lps += self.state["pump_capacity_lps"] * self.state["pump_2_efficiency"]

        self.state["current_outflow_lps"] = outflow_lps

        # 4. Update Water Level (Convert Liters to Cubic Meters)
        net_flow_m3_sec = (total_inflow_lps - outflow_lps) / 1000.0
        level_change = (net_flow_m3_sec * delta_sec) / self.state["sump_area_m2"]
        self.state["level_m"] = max(0.0, level + level_change)

    def read_sensor(self, sensor_name: str, global_state: dict[str, Any]) -> float:
        match sensor_name:
            case "level_m":
                return round(self.state["level_m"] + random.gauss(0, 0.01), 3)

            case "pump_1_amps":
                if not self.state["pump_1_running"]:
                    return round(random.uniform(0.0, 0.2), 2)
                # Lower efficiency (clogging) means higher current draw
                base_amps = 12.0 / self.state["pump_1_efficiency"]
                return round(base_amps + random.gauss(0, 0.5), 2)

            case "pump_2_amps":
                if not self.state["pump_2_running"]:
                    return round(random.uniform(0.0, 0.2), 2)
                base_amps = 12.0 / self.state["pump_2_efficiency"]
                return round(base_amps + random.gauss(0, 0.5), 2)

            case "discharge_flow_lps":
                flow = self.state.get("current_outflow_lps", 0.0)
                if flow > 0:
                    return round(flow + random.gauss(0, 0.8), 2)
                return 0.0
            case _:
                return 0.0