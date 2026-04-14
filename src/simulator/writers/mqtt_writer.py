import asyncio
import json
import logging
import time
from collections import deque
from typing import Any

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
    ) -> None:
        self.prefer_realtime_immediate = True
        self.host = host
        self.port = port
        self.base_topic = base_topic.strip("/")
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.boot_backoff_max = boot_backoff_max
        self.max_rows_per_message = max(1, int(max_rows_per_message))
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
        for asset_name, rows in payloads_by_asset.items():
            topic = f"{self.base_topic}/{asset_name}"
            for start in range(0, len(rows), self.max_rows_per_message):
                chunk = rows[start : start + self.max_rows_per_message]
                payload = json.dumps(chunk)
                info = self.client.publish(topic, payload, qos=0)
                rc = getattr(info, "rc", 0)
                if rc != 0:
                    logger.warning(
                        "MQTT publish failed topic=%s rc=%s rows=%d",
                        topic,
                        rc,
                        len(chunk),
                    )
                else:
                    self._published_messages += 1
                    self._published_rows += len(chunk)
                    logger.debug("MQTT published topic=%s rows=%d", topic, len(chunk))

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
