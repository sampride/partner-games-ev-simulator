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
        self.next_update = current_time + timedelta(seconds=max(0.001, self.update_interval_sec + jitter))


class Asset:
    """Base class for all simulated equipment."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.state: dict[str, Any] = {}
        self.sensors: list[SensorConfig] = []
        self._pending_data: list[dict[str, Any]] = []
        self._next_sensor_due: datetime = datetime.min

    def _refresh_next_sensor_due(self) -> None:
        if self.sensors:
            self._next_sensor_due = min(sensor.next_update for sensor in self.sensors)
        else:
            self._next_sensor_due = datetime.max

    def _emit_due_sensors(self, current_time: datetime, global_state: dict[str, Any]) -> None:
        if not self.sensors or current_time < self._next_sensor_due:
            return

        next_due = datetime.max
        for sensor in self.sensors:
            if sensor.should_update(current_time):
                payload: dict[str, Any] = {
                    "timestamp": current_time.isoformat(),
                    "asset": self.name,
                    "sensor": sensor.name,
                    "value": self.read_sensor(sensor.name, global_state),
                }
                self._pending_data.append(payload)
                sensor.set_next_update(current_time)
            if sensor.next_update < next_due:
                next_due = sensor.next_update

        self._next_sensor_due = next_due

    def tick(self, current_time: datetime, delta_sec: float, global_state: dict[str, Any]) -> None:
        self.update_internal_state(delta_sec, current_time, global_state)
        self._emit_due_sensors(current_time, global_state)

    def update_internal_state(
        self, delta_sec: float, current_time: datetime, global_state: dict[str, Any]
    ) -> None:
        raise NotImplementedError("Subclasses must implement update_internal_state")

    def read_sensor(self, sensor_name: str, global_state: dict[str, Any]) -> float:
        raise NotImplementedError("Subclasses must implement read_sensor")

    def flush_data(self) -> list[dict[str, Any]]:
        data = self._pending_data
        self._pending_data = []
        return data
