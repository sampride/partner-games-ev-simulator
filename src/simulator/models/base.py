import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any


@dataclass
class SensorConfig:
    name: str
    update_interval_sec: float
    jitter_sec: float = 0.0
    emit_on_change: bool = False
    heartbeat_interval_sec: float | None = None
    next_update: datetime = field(init=False)
    last_emitted_value: Any = field(default=None, init=False)
    has_emitted_value: bool = field(default=False, init=False)
    last_emitted_at: datetime = field(default=datetime.min, init=False)

    def __post_init__(self) -> None:
        self.next_update = datetime.min
        if self.emit_on_change and self.heartbeat_interval_sec is None:
            self.heartbeat_interval_sec = self.update_interval_sec

    def should_update(self, current_time: datetime) -> bool:
        return current_time >= self.next_update

    def should_emit_value(self, value: Any, current_time: datetime) -> bool:
        if not self.emit_on_change:
            return True
        if not self.has_emitted_value:
            return True
        if value != self.last_emitted_value:
            return True
        if self.heartbeat_interval_sec is not None:
            return (current_time - self.last_emitted_at).total_seconds() >= self.heartbeat_interval_sec
        return False

    def record_emitted_value(self, value: Any, current_time: datetime) -> None:
        self.last_emitted_value = value
        self.has_emitted_value = True
        self.last_emitted_at = current_time

    def set_next_update(self, current_time: datetime, emitted: bool = True) -> None:
        base_interval = self.update_interval_sec
        if self.emit_on_change:
            if emitted and self.heartbeat_interval_sec is not None:
                base_interval = min(self.update_interval_sec, self.heartbeat_interval_sec)
            else:
                base_interval = self.update_interval_sec
        jitter = random.uniform(-self.jitter_sec, self.jitter_sec)
        self.next_update = current_time + timedelta(seconds=max(0.001, base_interval + jitter))


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
                value = self.read_sensor(sensor.name, global_state)
                emitted = sensor.should_emit_value(value, current_time)
                if emitted:
                    payload: dict[str, Any] = {
                        "timestamp": current_time.isoformat(),
                        "asset": self.name,
                        "sensor": sensor.name,
                        "value": value,
                    }
                    self._pending_data.append(payload)
                    sensor.record_emitted_value(value, current_time)
                sensor.set_next_update(current_time, emitted=emitted)
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
