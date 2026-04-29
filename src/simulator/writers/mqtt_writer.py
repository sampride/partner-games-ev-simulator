import asyncio
import json
import logging
import time
from collections import deque
from typing import Any, Iterable

logger = logging.getLogger("simulator.writers.mqtt")

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - environment dependent
    mqtt = None


class MqttWriter:
    def __init__(
        self,
        host: str,
        port: int = 1883,
        base_topic: str = "ev_network",
        allow_backfill: bool = False,
        allow_realtime: bool = True,
        max_buffer_size: int = 5000,
        reconnect_min_delay: int = 1,
        reconnect_max_delay: int = 60,
        boot_backoff_max: float = 30.0,
        max_rows_per_message: int = 100,
        payload_mode: str = "batched_array",
        timestamp_field: str = "timestamp",
        value_field: str = "value",
        include_sensor_in_payload: bool = False,
    ) -> None:
        self.prefer_realtime_immediate = True
        self.host = host
        self.port = port
        self.base_topic = base_topic.strip("/")
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.boot_backoff_max = boot_backoff_max
        self.max_rows_per_message = max(1, int(max_rows_per_message))
        self.payload_mode = str(payload_mode).strip().lower()
        self.timestamp_field = str(timestamp_field)
        self.value_field = str(value_field)
        self.include_sensor_in_payload = bool(include_sensor_in_payload)
        valid_payload_modes = {"batched_array", "single_object_per_asset", "single_object_per_signal"}
        if self.payload_mode not in valid_payload_modes:
            raise ValueError(
                f"Unsupported MQTT payload_mode '{self.payload_mode}'. Expected one of {sorted(valid_payload_modes)}"
            )
        self.is_connected = False
        self.buffer: deque[dict[str, list[dict[str, Any]]]] = deque(maxlen=max_buffer_size)
        self._published_messages = 0
        self._published_rows = 0
        self._publish_log_interval_sec = 30.0
        self._last_publish_log_time = time.monotonic()

        if mqtt is None:
            raise RuntimeError(
                "paho-mqtt is not installed. Install project dependencies before using the MQTT writer."
            )

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.reconnect_delay_set(
            min_delay=reconnect_min_delay, max_delay=reconnect_max_delay
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self._boot_task = asyncio.create_task(self._maintain_initial_connection())

    def _on_connect(
        self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any
    ) -> None:
        if reason_code == 0:
            self.is_connected = True
            logger.info(
                "MQTT connected to %s:%s with %d buffered batches",
                self.host,
                self.port,
                len(self.buffer),
            )
        else:
            logger.warning(
                "MQTT connection refused by %s:%s code=%s", self.host, self.port, reason_code
            )

    def _on_disconnect(
        self, client: Any, userdata: Any, disconnect_flags: Any, reason_code: Any, properties: Any
    ) -> None:
        self.is_connected = False
        logger.warning("MQTT disconnected reason=%s. Buffering mode active.", reason_code)

    async def _maintain_initial_connection(self) -> None:
        backoff = 1.0
        while not self.is_connected:
            try:
                self.client.connect_async(self.host, self.port)
                self.client.loop_start()
                break
            except Exception as exc:  # pragma: no cover - depends on runtime env
                logger.warning("MQTT broker unavailable (%s). Retrying in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.boot_backoff_max)

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        payloads_by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            asset_name = str(row.get("asset", "unknown_asset"))
            payloads_by_asset.setdefault(asset_name, []).append(row)

        if not self.is_connected:
            if payloads_by_asset:
                self.buffer.append(payloads_by_asset)
                if len(self.buffer) == self.buffer.maxlen:
                    logger.warning("MQTT buffer is full; oldest batches will be dropped.")
            return

        await self._drain_buffered_batches()

        if payloads_by_asset:
            self._publish_grouped_data(payloads_by_asset)

        self._maybe_log_publish_summary()

    async def _drain_buffered_batches(self) -> None:
        flush_limit = 20
        flushed = 0
        while self.buffer and self.is_connected and flushed < flush_limit:
            old_payloads = self.buffer.popleft()
            self._publish_grouped_data(old_payloads)
            flushed += 1

    def _publish_grouped_data(self, payloads_by_asset: dict[str, list[dict[str, Any]]]) -> None:
        if self.payload_mode == "batched_array":
            self._publish_batched_array(payloads_by_asset)
            return
        if self.payload_mode == "single_object_per_asset":
            self._publish_single_object_per_asset(payloads_by_asset)
            return
        self._publish_single_object_per_signal(payloads_by_asset)

    def _publish_batched_array(self, payloads_by_asset: dict[str, list[dict[str, Any]]]) -> None:
        for asset_name, rows in payloads_by_asset.items():
            topic = self._asset_topic(asset_name)
            for start in range(0, len(rows), self.max_rows_per_message):
                chunk = [
                    {
                        "timestamp": row.get("timestamp"),
                        "asset": row.get("asset"),
                        "sensor": row.get("sensor"),
                        "value": row.get("value"),
                    }
                    for row in rows[start : start + self.max_rows_per_message]
                ]
                self._publish_payload(topic, chunk, len(chunk))

    def _publish_single_object_per_asset(self, payloads_by_asset: dict[str, list[dict[str, Any]]]) -> None:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for asset_name, rows in payloads_by_asset.items():
            for row in rows:
                timestamp = str(row.get("timestamp", ""))
                key = (asset_name, timestamp)
                payload = grouped.setdefault(key, {self.timestamp_field: timestamp})
                sensor_name = str(row.get("sensor", "unknown_sensor"))
                payload[sensor_name] = row.get("value")

        for (asset_name, _timestamp), payload in grouped.items():
            topic = self._asset_topic(asset_name)
            self._publish_payload(topic, payload, 1)

    def _publish_single_object_per_signal(self, payloads_by_asset: dict[str, list[dict[str, Any]]]) -> None:
        for asset_name, rows in payloads_by_asset.items():
            asset_topic = self._asset_topic(asset_name)
            for row in rows:
                sensor_name = str(row.get("sensor", "unknown_sensor"))
                topic = f"{asset_topic}/{sensor_name}"
                payload: dict[str, Any] = {
                    self.timestamp_field: row.get("timestamp"),
                    self.value_field: row.get("value"),
                }
                if self.include_sensor_in_payload:
                    payload["sensor"] = sensor_name
                    payload["asset"] = asset_name
                self._publish_payload(topic, payload, 1)


    def _asset_topic(self, asset_name: str) -> str:
        asset_name = asset_name.strip("/")
        if not self.base_topic:
            return asset_name
        base_tail = self.base_topic.rsplit("/", 1)[-1]
        if base_tail == asset_name:
            return self.base_topic
        return f"{self.base_topic}/{asset_name}"

    def _publish_payload(self, topic: str, payload_obj: Any, row_count: int) -> None:
        payload = json.dumps(payload_obj)
        info = self.client.publish(topic, payload, qos=0)
        rc = getattr(info, "rc", 0)
        if rc != 0:
            logger.warning(
                "MQTT publish failed topic=%s rc=%s rows=%d",
                topic,
                rc,
                row_count,
            )
            return
        self._published_messages += 1
        self._published_rows += row_count
        logger.debug("MQTT published topic=%s rows=%d", topic, row_count)

    def _maybe_log_publish_summary(self) -> None:
        now = time.monotonic()
        if now - self._last_publish_log_time < self._publish_log_interval_sec:
            return
        logger.info(
            "MQTT publish heartbeat messages=%d rows=%d buffered_batches=%d connected=%s",
            self._published_messages,
            self._published_rows,
            len(self.buffer),
            self.is_connected,
        )
        self._published_messages = 0
        self._published_rows = 0
        self._last_publish_log_time = now

    async def flush(self) -> None:
        if self.is_connected and self.buffer:
            await self._drain_buffered_batches()
            self._maybe_log_publish_summary()

    async def close(self) -> None:
        if self.client is not None:
            self.client.loop_stop()

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime
