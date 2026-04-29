import json

from simulator.writers.mqtt_writer import MqttWriter


class _PublishInfo:
    def __init__(self, rc: int = 0) -> None:
        self.rc = rc


class _FakeClient:
    def __init__(self) -> None:
        self.published = []

    def reconnect_delay_set(self, min_delay: int, max_delay: int) -> None:
        return None

    def publish(self, topic: str, payload: str, qos: int = 0):
        self.published.append((topic, payload, qos))
        return _PublishInfo(0)

    def connect_async(self, host: str, port: int) -> None:
        return None

    def loop_start(self) -> None:
        return None

    def loop_stop(self) -> None:
        return None


def _build_writer(monkeypatch, **kwargs) -> MqttWriter:
    monkeypatch.setattr("simulator.writers.mqtt_writer.mqtt", type("M", (), {"Client": lambda *a, **k: _FakeClient(), "CallbackAPIVersion": type("C", (), {"VERSION2": object()})})())
    monkeypatch.setattr("simulator.writers.mqtt_writer.asyncio.create_task", lambda coro: coro.close())
    writer = MqttWriter(host="localhost", **kwargs)
    writer.is_connected = True
    return writer


def test_single_object_per_signal_mode(monkeypatch):
    writer = _build_writer(monkeypatch, base_topic="ev_network", payload_mode="single_object_per_signal")
    writer._publish_grouped_data({
        "Charger_01": [
            {"timestamp": "2026-04-14T00:00:00Z", "asset": "Charger_01", "sensor": "Output_Current_DC", "value": 42.5}
        ]
    })

    assert len(writer.client.published) == 1
    topic, payload, _qos = writer.client.published[0]
    assert topic == "ev_network/Charger_01/Output_Current_DC"
    assert json.loads(payload) == {"timestamp": "2026-04-14T00:00:00Z", "value": 42.5}


def test_single_object_per_asset_mode(monkeypatch):
    writer = _build_writer(monkeypatch, base_topic="ev_network", payload_mode="single_object_per_asset")
    writer._publish_grouped_data({
        "Charger_01": [
            {"timestamp": "2026-04-14T00:00:00Z", "asset": "Charger_01", "sensor": "Output_Current_DC", "value": 42.5},
            {"timestamp": "2026-04-14T00:00:00Z", "asset": "Charger_01", "sensor": "Output_Voltage_DC", "value": 401.2},
        ]
    })

    assert len(writer.client.published) == 1
    topic, payload, _qos = writer.client.published[0]
    assert topic == "ev_network/Charger_01"
    assert json.loads(payload) == {
        "timestamp": "2026-04-14T00:00:00Z",
        "Output_Current_DC": 42.5,
        "Output_Voltage_DC": 401.2,
    }


def test_batched_array_mode_excludes_writer_neutral_metadata(monkeypatch):
    writer = _build_writer(monkeypatch, base_topic="ev_network", payload_mode="batched_array")
    writer._publish_grouped_data({
        "Charger_01": [
            {
                "timestamp": "2026-04-14T00:00:00Z",
                "asset": "Charger_01",
                "sensor": "Output_Current_DC",
                "data_type": "double",
                "value": 42.5,
            }
        ]
    })

    _topic, payload, _qos = writer.client.published[0]
    assert json.loads(payload) == [
        {
            "timestamp": "2026-04-14T00:00:00Z",
            "asset": "Charger_01",
            "sensor": "Output_Current_DC",
            "value": 42.5,
        }
    ]


def test_single_object_per_signal_mode_avoids_duplicate_site_segment(monkeypatch):
    writer = _build_writer(monkeypatch, base_topic="ev_network/Site_Melbourne_North", payload_mode="single_object_per_signal")
    writer._publish_grouped_data({
        "Site_Melbourne_North": [
            {"timestamp": "2026-04-14T00:00:00Z", "asset": "Site_Melbourne_North", "sensor": "site_total_power_kw", "value": 500.0}
        ],
        "Charger_01": [
            {"timestamp": "2026-04-14T00:00:00Z", "asset": "Charger_01", "sensor": "Output_Current_DC", "value": 42.5}
        ],
    })

    topics = [topic for topic, _payload, _qos in writer.client.published]
    assert "ev_network/Site_Melbourne_North/site_total_power_kw" in topics
    assert "ev_network/Site_Melbourne_North/Charger_01/Output_Current_DC" in topics
