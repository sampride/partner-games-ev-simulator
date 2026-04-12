import json
import asyncio
from collections import deque
from typing import Any
import paho.mqtt.client as mqtt


class MqttWriter:
    def __init__(
        self,
        host: str,
        port: int = 1883,
        base_topic: str = "ev_network",
        allow_backfill: bool = False,
        allow_realtime: bool = True,
        max_buffer_size: int = 5000,  # Buffers ~4 minutes of data at 20Hz
    ) -> None:
        self.host = host
        self.port = port
        self.base_topic = base_topic.strip("/")
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime

        # Resiliency State
        self.is_connected = False
        self.buffer: deque[dict[str, list[dict[str, Any]]]] = deque(maxlen=max_buffer_size)

        # Paho Client Setup (v2 API)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        # Configure internal Paho exponential backoff for lost connections
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)

        # Attach Callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        # Start the background task to handle the initial boot connection
        self._boot_task = asyncio.create_task(self._maintain_initial_connection())

    def _on_connect(
        self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any
    ) -> None:
        if reason_code == 0:
            self.is_connected = True
            print(
                f"\n[MQTT] Connected to {self.host}:{self.port}. Buffer contains {len(self.buffer)} pending batches."
            )
        else:
            print(f"\n[MQTT] Connection refused. Code: {reason_code}")

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        self.is_connected = False
        print(f"\n[MQTT] Disconnected (Reason: {reason_code}). Entering buffering mode...")

    async def _maintain_initial_connection(self) -> None:
        """Background task to handle exponential backoff if the broker is missing on boot."""
        backoff = 1.0
        max_backoff = 30.0

        while not self.is_connected:
            try:
                # connect_async is non-blocking, but will throw an OS Error if the port is dead
                self.client.connect_async(self.host, self.port)
                self.client.loop_start()

                # Once loop_start succeeds, Paho handles all future reconnections automatically
                break

            except Exception as e:
                print(f"[MQTT] Broker unavailable ({e}). Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        # 1. Group payloads by asset to maintain the topic hierarchy
        payloads_by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            asset_name = row.get("asset", "unknown_asset")
            if asset_name not in payloads_by_asset:
                payloads_by_asset[asset_name] = []
            payloads_by_asset[asset_name].append(row)

        # 2. If Offline: Add to the right side of the buffer and exit
        if not self.is_connected:
            self.buffer.append(payloads_by_asset)
            return

        # 3. If Online & Buffer has data: Flush gracefully
        # We limit flushing to 20 batches per tick so we don't stall the asyncio physics engine
        flush_limit = 20
        flushed = 0

        while self.buffer and self.is_connected and flushed < flush_limit:
            old_payloads = self.buffer.popleft()  # Pull from the left (oldest first)
            self._publish_grouped_data(old_payloads)
            flushed += 1

        # 4. Publish the current real-time data
        if self.is_connected:
            self._publish_grouped_data(payloads_by_asset)

    def _publish_grouped_data(self, payloads_by_asset: dict[str, list[dict[str, Any]]]) -> None:
        """Helper to fire grouped payloads to their respective topics."""
        for asset_name, rows in payloads_by_asset.items():
            topic = f"{self.base_topic}/{asset_name}"
            payload = json.dumps(rows)
            # QoS 0 is fire-and-forget. Our offline buffer handles the resiliency.
            self.client.publish(topic, payload, qos=0)

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime