import random
from simulator.models.base import Asset, SensorConfig

class CentrifugalPump(Asset):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.sensors = [
            SensorConfig(name="flow_rate_lps", update_interval_sec=1.0, jitter_sec=0.1),
            SensorConfig(name="bearing_temp_c", update_interval_sec=5.0, jitter_sec=0.5),
            SensorConfig(name="speed_rpm", update_interval_sec=0.5, jitter_sec=0.0)
        ]

        self.state = {
            "target_rpm": 1500.0,
            "current_rpm": 0.0,
            "ambient_temp_c": 22.0,
            "friction_coefficient": 0.01
        }

    def update_internal_state(self, delta_sec: float) -> None:
        rpm_diff = self.state["target_rpm"] - self.state["current_rpm"]
        self.state["current_rpm"] += rpm_diff * 0.1 * delta_sec
        self.state["current_rpm"] += random.uniform(-5.0, 5.0)

    def read_sensor(self, sensor_name: str) -> float:
        rpm = self.state["current_rpm"]

        match sensor_name:
            case "speed_rpm":
                return round(rpm + random.gauss(0, 2.0), 1)
            case "flow_rate_lps":
                base_flow = rpm * 0.05
                return round(base_flow + random.gauss(0, 0.5), 2)
            case "bearing_temp_c":
                heat_generated = (rpm * self.state["friction_coefficient"]) * 0.2
                base_temp = self.state["ambient_temp_c"] + heat_generated
                return round(base_temp + random.gauss(0, 0.2), 2)
            case _:
                return 0.0