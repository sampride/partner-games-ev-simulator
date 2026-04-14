import asyncio
import json
import logging
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
    ) -> None:
        self.host = host
        self.port = port
        self.base_topic = base_topic.strip("/")
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.boot_backoff_max = boot_backoff_max
        self.is_connected = False
        self.buffer: deque[dict[str, list[dict[str, Any]]]] = deque(maxlen=max_buffer_size)

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
            logger.warning("MQTT connection refused by %s:%s code=%s", self.host, self.port, reason_code)

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
        if not data:
            return
        payloads_by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            asset_name = str(row.get("asset", "unknown_asset"))
            payloads_by_asset.setdefault(asset_name, []).append(row)

        if not self.is_connected:
            self.buffer.append(payloads_by_asset)
            if len(self.buffer) == self.buffer.maxlen:
                logger.warning("MQTT buffer is full; oldest batches will be dropped.")
            return

        flush_limit = 20
        flushed = 0
        while self.buffer and self.is_connected and flushed < flush_limit:
            old_payloads = self.buffer.popleft()
            self._publish_grouped_data(old_payloads)
            flushed += 1

        if self.is_connected:
            self._publish_grouped_data(payloads_by_asset)

    def _publish_grouped_data(self, payloads_by_asset: dict[str, list[dict[str, Any]]]) -> None:
        for asset_name, rows in payloads_by_asset.items():
            topic = f"{self.base_topic}/{asset_name}"
            payload = json.dumps(rows)
            self.client.publish(topic, payload, qos=0)

    async def flush(self) -> None:
        return

    async def close(self) -> None:
        if self.client is not None:
            self.client.loop_stop()

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime
