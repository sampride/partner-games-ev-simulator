import math
import random
from datetime import datetime
from simulator.models.base import Asset, SensorConfig
from simulator.models.ev_charger import EVCharger


class ChargingSite(Asset):
    def __init__(self, name: str, max_queue: int = 3) -> None:
        super().__init__(name)
        self.chargers: list[EVCharger] = []
        self.max_queue = max_queue

        self.sensors = [
            SensorConfig("site_total_power_kw", update_interval_sec=1.0),
            SensorConfig("site_grid_voltage_v", update_interval_sec=0.5),
            SensorConfig("ambient_temp_c", update_interval_sec=60.0),
            # Queue length is explicitly NOT a sensor, fulfilling your requirement
        ]

        self.state = {
            "internal_queue_length": 0,
            "site_total_power_kw": 0.0,
            "ambient_temp_c": 22.0,
        }

    def add_charger(self, charger: EVCharger) -> None:
        self.chargers.append(charger)

    def tick(self, current_time: datetime, delta_sec: float, global_state: dict) -> None:
        self.update_internal_state(delta_sec, current_time, global_state)

        # 1. Tick all child chargers with the updated global state
        for charger in self.chargers:
            charger.tick(current_time, delta_sec, global_state)
            # Pull child data up to the site level's pending data
            self._pending_data.extend(charger.flush_data())

        # 2. Flush site-level sensors
        for sensor in self.sensors:
            if sensor.should_update(current_time):
                payload = {
                    "timestamp": current_time.isoformat(),
                    "asset": self.name,
                    "sensor": sensor.name,
                    "value": self.read_sensor(sensor.name, global_state),
                }
                self._pending_data.append(payload)
                sensor.set_next_update(current_time)

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict
    ) -> None:
        # 1. Environment Simulation (Simple diurnal temperature curve)
        hour = current_time.hour + current_time.minute / 60.0
        self.state["ambient_temp_c"] = 15.0 + math.sin(math.pi * (hour - 8) / 12) * 10.0
        global_state["ambient_temp_c"] = self.state["ambient_temp_c"]

        # 2. Arrival Simulation (Poisson process based on time of day)
        # Peak arrivals at 8AM and 6PM, very low at 3AM
        arrival_rate_per_hour = (
            5.0
            + math.sin(math.pi * (hour - 8) / 12) * 15.0
            + math.sin(math.pi * (hour - 18) / 12) * 10.0
        )
        arrival_rate_per_hour = max(0.5, arrival_rate_per_hour)  # Always a slight chance
        arrival_prob_per_tick = (arrival_rate_per_hour / 3600.0) * delta_sec

        if random.random() < arrival_prob_per_tick:
            if self.state["internal_queue_length"] < self.max_queue:
                self.state["internal_queue_length"] += 1

        # 3. Queue Processing (Assign cars to AVAILABLE chargers)
        if self.state["internal_queue_length"] > 0:
            for charger in self.chargers:
                if charger.state["charger_state"] == 0:  # 0 = AVAILABLE
                    # Simulate a charging session between 15 and 45 minutes
                    duration = random.uniform(900.0, 2700.0)
                    charger.start_session(duration_sec=duration)
                    self.state["internal_queue_length"] -= 1
                    break  # Only assign one car per tick

        # 4. Rollups & Load Balancing
        total_power = sum(c.state["current_power_kw"] for c in self.chargers)
        self.state["site_total_power_kw"] = total_power

        # Calculate dynamic grid sag: assume a 10V drop for every 500kW
        grid_sag = (total_power / 500.0) * 10.0
        global_state["current_grid_voltage"] = 480.0 - grid_sag

    def read_sensor(self, sensor_name: str, global_state: dict) -> float:
        match sensor_name:
            case "site_total_power_kw":
                return round(self.state["site_total_power_kw"], 2)
            case "site_grid_voltage_v":
                return round(global_state.get("current_grid_voltage", 480.0), 2)
            case "ambient_temp_c":
                return round(self.state["ambient_temp_c"] + random.gauss(0, 0.5), 1)
            case _:
                return 0.0