import json
from typing import Any
import paho.mqtt.client as mqtt

class MqttWriter:
    def __init__(
        self,
        host: str,
        port: int = 1883,
        topic_prefix: str = "sim",
        allow_backfill: bool = False,
        allow_realtime: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.topic_prefix = topic_prefix
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime

        # Initialize the Paho MQTT Client (v2 API is the modern standard)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        # Connect asynchronously and start the background network loop
        try:
            self.client.connect_async(self.host, self.port)
            self.client.loop_start()
            print(f"MQTT Writer initialized. Connecting to {self.host}:{self.port}...")
        except Exception as e:
            print(f"Failed to initialize MQTT connection: {e}")

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        payload = json.dumps(data)
        self.client.publish("hub/telemetry/bulk", payload, qos=0)




        # for row in data:
        #     # Construct a dynamic topic: e.g., sim/Pump_01/bearing_temp_c
        #     topic = f"{self.topic_prefix}/{row['asset']}/{row['sensor']}"
        #     payload = json.dumps(row)
        #
        #     # QoS 0 (Fire and forget) is standard for raw high-frequency telemetry
        #     self.client.publish(topic, payload, qos=0)

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime