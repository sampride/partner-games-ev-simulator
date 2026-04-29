import asyncio
import gzip
import json
import threading
import time
from datetime import datetime

from simulator.utils.config_parser import build_simulation_components, validate_config
from simulator.writers.omf_writer import OmfWriter


class CaptureOmfWriter(OmfWriter):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.posts = []
        self.gets = []
        self.form_posts = []

    def _get(self, url, headers):
        self.gets.append((url, headers))
        return (
            200,
            json.dumps(
                {
                    "token_endpoint": (
                        "https://example.connect/identity/connect/token"
                    )
                }
            ),
        )

    def _post_form(self, url, form):
        self.form_posts.append((url, form))
        return 200, json.dumps({"access_token": "token-1", "expires_in": 3600})

    def _post(self, url, headers, body):
        self.posts.append((url, headers, body))
        return 202, ""


class SlowCaptureOmfWriter(CaptureOmfWriter):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.active_posts = 0
        self.max_active_posts = 0
        self._lock = threading.Lock()

    def _post(self, url, headers, body):
        with self._lock:
            self.active_posts += 1
            self.max_active_posts = max(self.max_active_posts, self.active_posts)
        try:
            time.sleep(0.03)
            return super()._post(url, headers, body)
        finally:
            with self._lock:
                self.active_posts -= 1


def _json_body(body):
    raw = gzip.decompress(body).decode("utf-8") if isinstance(body, bytes) else body
    return json.loads(raw)


def _body_size(body) -> int:
    raw = gzip.decompress(body) if isinstance(body, bytes) else body.encode("utf-8")
    return len(raw)


def test_omf_writer_support_flags() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        allow_backfill=False,
        allow_realtime=True,
    )

    assert writer.supports_backfill() is False
    assert writer.supports_realtime() is True


def test_eds_writer_creates_containers_and_batches_data() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        batch_size=2,
        use_compression=False,
        default_omf_type="Timeindexed.Double",
    )

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": datetime(2026, 4, 14, 1, 0, 0),
                    "asset": "AC/North/C01",
                    "sensor": "Output_Current_DC",
                    "value": 42.7,
                },
                {
                    "timestamp": datetime(2026, 4, 14, 1, 0, 1),
                    "asset": "AC/North/C01",
                    "sensor": "Output_Current_DC",
                    "value": 43.1,
                },
                {
                    "timestamp": "2026-04-14T01:00:02Z",
                    "asset": "AC/North/C01",
                    "sensor": "Output_Current_DC",
                    "value": 44.2,
                },
            ]
        )
    )

    assert [post[1]["messagetype"] for post in writer.posts] == [
        "container",
        "data",
        "data",
    ]
    assert writer.posts[0][1]["Content-Type"] == "application/json"
    assert writer.posts[0][1]["Accept"] == "application/json"
    assert "Authorization" not in writer.posts[0][1]
    assert writer.posts[0][0] == (
        "http://localhost:5590/api/v1/tenants/default/namespaces/default/omf"
    )

    container_payload = _json_body(writer.posts[0][2])
    assert container_payload == [
        {"id": "AC.North.C01.Output_Current_DC", "typeid": "Timeindexed.Double"}
    ]

    first_data_payload = _json_body(writer.posts[1][2])
    assert first_data_payload == [
        {
            "containerid": "AC.North.C01.Output_Current_DC",
            "values": [
                {"Timestamp": "2026-04-14T01:00:00Z", "Value": 42.7},
                {"Timestamp": "2026-04-14T01:00:01Z", "Value": 43.1},
            ],
        }
    ]


def test_existing_omf_container_is_not_recreated() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        use_compression=False,
    )
    row = {
        "timestamp": "2026-04-14T01:00:00Z",
        "asset": "AC.North.C01",
        "sensor": "Output_Current_DC",
        "value": 1.0,
    }

    asyncio.run(writer.write_batch([row]))
    asyncio.run(writer.write_batch([{**row, "value": 2.0}]))

    message_types = [post[1]["messagetype"] for post in writer.posts]
    assert message_types.count("container") == 1
    assert message_types.count("data") == 2


def test_omf_writer_splits_data_batches_by_body_size() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        batch_size=1000,
        max_body_bytes=1200,
        use_compression=False,
    )

    rows = [
        {
            "timestamp": "2026-04-14T01:00:00Z",
            "asset": "AC.North.C01",
            "sensor": f"Status_{index}",
            "data_type": "string",
            "value": "x" * 250,
        }
        for index in range(8)
    ]

    asyncio.run(writer.write_batch(rows))

    data_posts = [post for post in writer.posts if post[1]["messagetype"] == "data"]
    assert len(data_posts) > 1
    assert all(_body_size(post[2]) <= writer.max_body_bytes for post in data_posts)


def test_omf_writer_direct_json_data_shape_matches_expected_payload() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        batch_size=1000,
        use_compression=False,
    )

    rows = [
        {
            "timestamp": "2026-04-14T01:00:00Z",
            "asset": "AC.North.C01",
            "sensor": "Output_Current_DC",
            "value": 42.7,
        },
        {
            "timestamp": "2026-04-14T01:00:01Z",
            "asset": "AC.North.C01",
            "sensor": "Output_Current_DC",
            "value": 43.1,
        },
    ]

    asyncio.run(writer.write_batch(rows))

    data_post = next(post for post in writer.posts if post[1]["messagetype"] == "data")
    assert _json_body(data_post[2]) == [
        {
            "containerid": "AC.North.C01.Output_Current_DC",
            "values": [
                {"Timestamp": "2026-04-14T01:00:00Z", "Value": 42.7},
                {"Timestamp": "2026-04-14T01:00:01Z", "Value": 43.1},
            ],
        }
    ]


def test_omf_writer_can_post_data_batches_concurrently() -> None:
    writer = SlowCaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        batch_size=1000,
        max_body_bytes=1200,
        max_concurrent_requests=3,
        use_compression=False,
    )

    rows = [
        {
            "timestamp": "2026-04-14T01:00:00Z",
            "asset": "AC.North.C01",
            "sensor": f"Status_{index}",
            "data_type": "string",
            "value": "x" * 250,
        }
        for index in range(12)
    ]

    asyncio.run(writer.write_batch(rows))

    assert writer.max_active_posts > 1


def test_omf_writer_logs_internal_timing_metrics(caplog) -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        batch_size=1000,
        max_body_bytes=1200,
        max_concurrent_requests=2,
        use_compression=True,
    )

    rows = [
        {
            "timestamp": "2026-04-14T01:00:00Z",
            "asset": "AC.North.C01",
            "sensor": f"Status_{index}",
            "data_type": "string",
            "value": "x" * 250,
        }
        for index in range(8)
    ]

    with caplog.at_level("DEBUG", logger="simulator.writers.omf"):
        asyncio.run(writer.write_batch(rows))

    message = caplog.messages[-1]
    assert "OMF write_batch rows=8" in message
    assert "data_batches=" in message
    assert "uncompressed_bytes=" in message
    assert "compressed_bytes=" in message
    assert "build_seconds=" in message
    assert "serialization_seconds=" in message
    assert "post_seconds=" in message
    assert "concurrency=2" in message


def test_omf_writer_splits_container_batches_by_body_size() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        container_batch_size=1000,
        max_body_bytes=1200,
        use_compression=False,
    )

    rows = [
        {
            "timestamp": "2026-04-14T01:00:00Z",
            "asset": "AC.North.C01",
            "sensor": f"Sensor_With_A_Long_Name_{index:03d}",
            "value": 42.7,
        }
        for index in range(30)
    ]

    asyncio.run(writer.write_batch(rows))

    container_posts = [
        post for post in writer.posts if post[1]["messagetype"] == "container"
    ]
    assert len(container_posts) > 1
    assert all(_body_size(post[2]) <= writer.max_body_bytes for post in container_posts)


def test_omf_writer_marks_compressed_json_payload() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        use_compression=True,
    )

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Output_Current_DC",
                    "value": 42.7,
                }
            ]
        )
    )

    headers = writer.posts[0][1]
    assert headers["Content-Type"] == "application/json"
    assert headers["Accept"] == "application/json"
    assert headers["compression"] == "gzip"
    assert _json_body(writer.posts[0][2]) == [
        {"id": "AC.North.C01.Output_Current_DC", "typeid": "Timeindexed.Double"}
    ]


def test_omf_writer_maps_row_data_type_to_omf_type() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        use_compression=False,
        omf_type_map={
            "double": "Timeindexed.Double",
            "integer": "Timeindexed.Integer",
            "string": "Timeindexed.String",
        },
    )

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Output_Current_DC",
                    "data_type": "double",
                    "value": 42.7,
                },
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Session_Duration",
                    "data_type": "integer",
                    "value": 12.0,
                },
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Charger_State",
                    "data_type": "string",
                    "value": "Charging",
                },
            ]
        )
    )

    container_payload = _json_body(writer.posts[0][2])
    assert container_payload == [
        {"id": "AC.North.C01.Output_Current_DC", "typeid": "Timeindexed.Double"},
        {"id": "AC.North.C01.Session_Duration", "typeid": "Timeindexed.Integer"},
        {"id": "AC.North.C01.Charger_State", "typeid": "Timeindexed.String"},
    ]

    data_payload = _json_body(writer.posts[1][2])
    assert data_payload == [
        {
            "containerid": "AC.North.C01.Output_Current_DC",
            "values": [{"Timestamp": "2026-04-14T01:00:00Z", "Value": 42.7}],
        },
        {
            "containerid": "AC.North.C01.Session_Duration",
            "values": [{"Timestamp": "2026-04-14T01:00:00Z", "Value": 12}],
        },
        {
            "containerid": "AC.North.C01.Charger_State",
            "values": [{"Timestamp": "2026-04-14T01:00:00Z", "Value": "Charging"}],
        },
    ]


def test_cds_writer_authenticates_with_bearer_token() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="cds",
        resource="https://example.connect",
        tenant_id="tenant",
        namespace_id="namespace",
        client_id="client",
        client_secret="secret",
        use_compression=False,
    )

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Charger_State",
                    "data_type": "string",
                    "value": "Charging",
                }
            ]
        )
    )

    assert writer.gets == [
        (
            "https://example.connect/identity/.well-known/openid-configuration",
            {"Accept": "application/json"},
        )
    ]
    assert writer.form_posts == [
        (
            "https://example.connect/identity/connect/token",
            {
                "client_id": "client",
                "client_secret": "secret",
                "grant_type": "client_credentials",
            },
        )
    ]
    assert all(post[1]["Authorization"] == "Bearer token-1" for post in writer.posts)
    container_payload = _json_body(writer.posts[0][2])
    assert container_payload == [
        {"id": "AC.North.C01.Charger_State", "typeid": "Timeindexed.String"}
    ]


def test_cds_writer_allows_auth_resource_to_differ_from_namespace_resource() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="cds",
        resource="https://namespace.example",
        auth_resource="https://auth.example",
        tenant_id="tenant",
        namespace_id="namespace",
        client_id="client",
        client_secret="secret",
        use_compression=False,
    )

    def auth_get(url, headers):
        writer.gets.append((url, headers))
        return (
            200,
            json.dumps({"token_endpoint": "https://auth.example/connect/token"}),
        )

    writer._get = auth_get

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Output_Current_DC",
                    "value": 42.7,
                }
            ]
        )
    )

    assert writer.gets == [
        (
            "https://auth.example/identity/.well-known/openid-configuration",
            {"Accept": "application/json"},
        )
    ]
    assert writer.form_posts[0][0] == "https://auth.example/connect/token"
    assert writer.posts[0][0] == (
        "https://namespace.example/api/v1/tenants/tenant/namespaces/namespace/omf"
    )


def test_omf_writer_registered_in_config_factory(tmp_path) -> None:
    config = {
        "simulation": {"tick_rate_sec": 0.1},
        "writers": [
            {
                "type": "omf",
                "config": {
                    "endpoint_type": "eds",
                    "resource": "http://localhost:5590",
                },
            },
        ],
        "assets": [
            {
                "name": "SiteA",
                "type": "ChargingSite",
                "chargers": [{"name": "C1"}],
            }
        ],
    }

    validate_config(config)
    _, writers, _ = build_simulation_components(config, tmp_path)

    assert len(writers) == 1
    assert isinstance(writers[0], OmfWriter)
