import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

@dataclass
class SensorConfig:
    name: str
    update_interval_sec: float
    jitter_sec: float = 0.0
    next_update: datetime = field(init=False)

    def __post_init__(self) -> None:
        self.next_update = datetime.min

    def should_update(self, current_time: datetime) -> bool:
        return current_time >= self.next_update

    def set_next_update(self, current_time: datetime) -> None:
        jitter = random.uniform(-self.jitter_sec, self.jitter_sec)
        self.next_update = current_time + timedelta(seconds=self.update_interval_sec + jitter)

class Asset:
    def __init__(self, name: str) -> None:
        self.name = name
        self.state: dict[str, Any] = {}
        self.sensors: list[SensorConfig] = []
        self._pending_data: list[dict[str, Any]] = []

    def tick(self, current_time: datetime, delta_sec: float, global_state: dict[str, Any]) -> None:
        # Pass the global state down to the physics engine
        self.update_internal_state(delta_sec, current_time, global_state)

        for sensor in self.sensors:
            if sensor.should_update(current_time):
                payload: dict[str, Any] = {
                    "timestamp": current_time.isoformat(),
                    "asset": self.name,
                    "sensor": sensor.name,
                    "value": self.read_sensor(sensor.name)
                }
                self._pending_data.append(payload)
                sensor.set_next_update(current_time)

    def update_internal_state(self, delta_sec: float, current_time: datetime, global_state: dict[str, Any]) -> None:
        raise NotImplementedError

    def read_sensor(self, sensor_name: str) -> float:
        raise NotImplementedError

    def flush_data(self) -> list[dict[str, Any]]:
        data = self._pending_data
        self._pending_data = []
        return data