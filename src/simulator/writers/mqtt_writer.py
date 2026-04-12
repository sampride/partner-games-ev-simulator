import json
from collections import defaultdict
from typing import Any
import paho.mqtt.client as mqtt


class MqttWriter:
    def __init__(
        self,
        host: str,
        port: int = 1883,
        base_topic: str = "telemetry/site",
        allow_backfill: bool = False,
        allow_realtime: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        # Strip trailing slashes to prevent accidental double slashes in topic strings
        self.base_topic = base_topic.strip("/")
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        try:
            self.client.connect_async(self.host, self.port)
            self.client.loop_start()
            print(f"MQTT Writer initialized. Base topic: {self.base_topic}/*")
        except Exception as e:
            print(f"Failed to initialize MQTT connection: {e}")

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        # Group the flat EAV rows by the asset they belong to
        payloads_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in data:
            asset_name = row.get("asset", "unknown_asset")
            payloads_by_asset[asset_name].append(row)

        # Publish one batched payload per asset to its specific hierarchical topic
        for asset_name, rows in payloads_by_asset.items():
            # Constructs topics like: ev_network/Site_Melbourne_North/Charger_01
            topic = f"{self.base_topic}/{asset_name}"
            payload = json.dumps(rows)
            self.client.publish(topic, payload, qos=0)

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime