import logging
import math
import random
from datetime import datetime
from typing import Any

from simulator.models.base import Asset, SensorConfig
from simulator.models.ev_charger import EVCharger

logger = logging.getLogger("simulator.models.charging_site")


class ChargingSite(Asset):
    def __init__(self, name: str, max_queue: int = 3) -> None:
        super().__init__(name)
        self.chargers: list[EVCharger] = []
        self.max_queue = max_queue

        self.sensors = [
            SensorConfig("site_total_power_kw", update_interval_sec=1.0),
            SensorConfig("site_grid_voltage_v", update_interval_sec=0.5),
            SensorConfig("site_ac_current_a", update_interval_sec=1.0),
            SensorConfig("site_power_available_kw", update_interval_sec=2.0),
            SensorConfig("main_breaker_load_percent", update_interval_sec=2.0),
            SensorConfig("number_of_active_sessions", update_interval_sec=1.0),
            SensorConfig("ambient_temp_c", update_interval_sec=60.0),
        ]
        self._refresh_next_sensor_due()

        self.state: dict[str, Any] = {
            "internal_queue_length": 0,
            "site_total_power_kw": 0.0,
            "site_ac_current_a": 0.0,
            "site_power_available_kw": 900.0,
            "main_breaker_load_percent": 0.0,
            "number_of_active_sessions": 0,
            "ambient_temp_c": 22.0,
            "transformer_rating_kw": 900.0,
        }

    def add_charger(self, charger: EVCharger) -> None:
        self.chargers.append(charger)

    def get_child_assets(self) -> list[Asset]:
        return list(self.chargers)

    def tick(self, current_time: datetime, delta_sec: float, global_state: dict[str, Any]) -> None:
        self.update_internal_state(delta_sec, current_time, global_state)

        for charger in self.chargers:
            charger.tick(current_time, delta_sec, global_state)
            self._pending_data.extend(charger.flush_data())

        total_power = sum(c.state["input_power_kw"] for c in self.chargers)
        active_sessions = sum(
            1
            for c in self.chargers
            if c.state["charger_state"]
            in [EVCharger.STATE_HANDSHAKING, EVCharger.STATE_CHARGING, EVCharger.STATE_THROTTLED]
        )
        self.state["site_total_power_kw"] = total_power
        self.state["number_of_active_sessions"] = active_sessions
        self.state["site_ac_current_a"] = total_power * 1000.0 / max(
            1.0, float(global_state.get("current_grid_voltage", 480.0))
        )
        self.state["main_breaker_load_percent"] = min(
            100.0, (total_power / max(1.0, self.state["transformer_rating_kw"])) * 100.0
        )

        self._emit_due_sensors(current_time, global_state)

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict[str, Any]
    ) -> None:
        hour = current_time.hour + current_time.minute / 60.0
        ambient = 16.0 + math.sin(math.pi * (hour - 7) / 12) * 9.0
        ambient += math.sin(math.pi * (hour - 15) / 6) * 1.5
        self.state["ambient_temp_c"] = ambient
        global_state["ambient_temp_c"] = ambient

        arrival_rate_per_hour = (
            1.5
            + max(0.0, math.sin(math.pi * (hour - 7.5) / 12)) * 8.5
            + max(0.0, math.sin(math.pi * (hour - 17.0) / 8)) * 5.0
        )
        arrival_prob_per_tick = max(0.0, arrival_rate_per_hour / 3600.0) * delta_sec
        if random.random() < arrival_prob_per_tick and self.state["internal_queue_length"] < self.max_queue:
            self.state["internal_queue_length"] += 1

        if self.state["internal_queue_length"] > 0:
            for charger in self.chargers:
                if charger.state["charger_state"] == EVCharger.STATE_IDLE:
                    duration = random.uniform(1200.0, 3600.0)
                    charger.start_session(duration_sec=duration)
                    self.state["internal_queue_length"] -= 1
                    break

        expected_site_limit = self.state["transformer_rating_kw"]
        lunch_derate = 0.0
        if 12.0 <= hour <= 16.0:
            lunch_derate = 120.0 * max(0.0, math.sin(math.pi * (hour - 12.0) / 4.0))
        self.state["site_power_available_kw"] = max(350.0, expected_site_limit - lunch_derate)

        pre_power = sum(c.state["input_power_kw"] for c in self.chargers)
        breaker_loading = pre_power / max(1.0, self.state["site_power_available_kw"])
        grid_sag = min(18.0, breaker_loading * 12.0)
        global_state["current_grid_voltage"] = 480.0 - grid_sag + random.gauss(0, 0.25)

    def read_sensor(self, sensor_name: str, global_state: dict[str, Any]) -> float:
        match sensor_name:
            case "site_total_power_kw":
                return round(self.state["site_total_power_kw"], 2)
            case "site_grid_voltage_v":
                return round(float(global_state.get("current_grid_voltage", 480.0)), 2)
            case "site_ac_current_a":
                return round(self.state["site_ac_current_a"], 2)
            case "site_power_available_kw":
                return round(self.state["site_power_available_kw"], 2)
            case "main_breaker_load_percent":
                return round(self.state["main_breaker_load_percent"], 1)
            case "number_of_active_sessions":
                return float(self.state["number_of_active_sessions"])
            case "ambient_temp_c":
                return round(self.state["ambient_temp_c"] + random.gauss(0, 0.4), 2)
            case _:
                logger.debug("Unknown site sensor requested: %s", sensor_name)
                return 0.0
