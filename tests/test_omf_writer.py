import asyncio
import gzip
import json
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


def _json_body(body):
    raw = gzip.decompress(body).decode("utf-8") if isinstance(body, bytes) else body
    return json.loads(raw)


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
        type_id="Timeindexed.Double",
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


def test_omf_writer_uses_configured_type_mappings() -> None:
    writer = CaptureOmfWriter(
        endpoint_type="eds",
        resource="http://localhost:5590",
        use_compression=False,
        type_ids={
            "double": "Timeindexed.Double",
            "integer": "Timeindexed.Integer",
            "string": "Timeindexed.String",
        },
        sensor_type_ids={"Charger_State": "Timeindexed.String"},
        stream_type_ids={"AC.North.C01.Warning_Code": "Timeindexed.Integer"},
    )

    asyncio.run(
        writer.write_batch(
            [
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Output_Current_DC",
                    "value": 42.7,
                },
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Session_Duration",
                    "value": 12,
                },
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Charger_State",
                    "value": "Charging",
                },
                {
                    "timestamp": "2026-04-14T01:00:00Z",
                    "asset": "AC.North.C01",
                    "sensor": "Warning_Code",
                    "value": "2",
                },
            ]
        )
    )

    container_payload = _json_body(writer.posts[0][2])
    assert container_payload == [
        {"id": "AC.North.C01.Output_Current_DC", "typeid": "Timeindexed.Double"},
        {"id": "AC.North.C01.Session_Duration", "typeid": "Timeindexed.Integer"},
        {"id": "AC.North.C01.Charger_State", "typeid": "Timeindexed.String"},
        {"id": "AC.North.C01.Warning_Code", "typeid": "Timeindexed.Integer"},
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
