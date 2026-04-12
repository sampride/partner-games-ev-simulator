import random
from datetime import datetime
from simulator.models.base import Asset, SensorConfig


class EVCharger(Asset):
    def __init__(self, name: str) -> None:
        super().__init__(name)

        # High-frequency electrical sensors (20Hz)
        # Slower thermal/mechanical sensors (2Hz - 5Hz)
        self.sensors = [
            SensorConfig("grid_voltage_v", 0.05, 0.0),
            SensorConfig("output_current_a", 0.05, 0.0),
            SensorConfig("cable_temp_c", 0.5, 0.0),
            SensorConfig("cabinet_temp_c", 0.5, 0.0),
            SensorConfig("cooling_fan_rpm", 0.2, 0.0),
        ]

        self.state = {
            "target_current_a": 150.0,
            "output_current_a": 0.0,
            "cabinet_temp_c": 25.0,
            "cable_temp_c": 25.0,
            "cooling_fan_rpm": 0.0,
            "ambient_temp_c": 25.0,
        }

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict
    ) -> None:
        # 1. Electrical State
        # Ramp up to target current
        curr_diff = self.state["target_current_a"] - self.state["output_current_a"]
        self.state["output_current_a"] += curr_diff * 2.0 * delta_sec

        current = self.state["output_current_a"]

        # 2. Thermal Math (Heat generated is proportional to Current squared)
        heat_generated = (current / 150.0) ** 2 * 5.0

        # Fan Controller (PID-like proportional response to cabinet temp)
        target_rpm = max(0.0, min(4000.0, (self.state["cabinet_temp_c"] - 30.0) * 300.0))
        self.state["cooling_fan_rpm"] += (
            (target_rpm - self.state["cooling_fan_rpm"]) * 1.0 * delta_sec
        )

        # Cooling effect based on fan speed
        active_cooling = (self.state["cooling_fan_rpm"] / 4000.0) * 8.0
        passive_cooling = (self.state["cabinet_temp_c"] - self.state["ambient_temp_c"]) * 0.05

        self.state["cabinet_temp_c"] += (
            heat_generated - active_cooling - passive_cooling
        ) * delta_sec

        # Cable heats up directly from current, cools passively
        cable_heat = (current / 150.0) ** 2 * 2.0
        cable_cooling = (self.state["cable_temp_c"] - self.state["ambient_temp_c"]) * 0.1
        self.state["cable_temp_c"] += (cable_heat - cable_cooling) * delta_sec

    def read_sensor(self, sensor_name: str) -> float:
        match sensor_name:
            case "grid_voltage_v":
                # Will eventually tie into global state sag
                return round(480.0 + random.gauss(0, 1.5), 2)
            case "output_current_a":
                return round(self.state["output_current_a"] + random.gauss(0, 0.5), 2)
            case "cabinet_temp_c":
                return round(self.state["cabinet_temp_c"] + random.gauss(0, 0.1), 2)
            case "cable_temp_c":
                return round(self.state["cable_temp_c"] + random.gauss(0, 0.1), 2)
            case "cooling_fan_rpm":
                rpm = self.state["cooling_fan_rpm"]
                return round(rpm + random.gauss(0, rpm * 0.02) if rpm > 0 else 0, 0)
            case _:
                return 0.0