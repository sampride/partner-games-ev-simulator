from __future__ import annotations

import gzip
import json
import logging
import os
import ssl
import time
from datetime import date, datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger("simulator.writers.omf")


class OmfWriter:
    def __init__(
        self,
        endpoint_type: str,
        resource: str = "",
        omf_endpoint: str | None = None,
        api_version: str = "v1",
        tenant_id: str | None = None,
        namespace_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        client_id_env: str = "OMF_CLIENT_ID",
        client_secret_env: str = "OMF_CLIENT_SECRET",
        token_url: str | None = None,
        allow_backfill: bool = True,
        allow_realtime: bool = True,
        batch_size: int = 500,
        container_batch_size: int = 100,
        use_compression: bool = True,
        verify_ssl: bool = True,
        timeout_seconds: float = 30.0,
        stream_id_separator: str = ".",
        default_omf_type: str = "Timeindexed.Double",
        omf_type_map: dict[str, str] | None = None,
    ) -> None:
        self.endpoint_type = endpoint_type.strip().lower()
        if self.endpoint_type not in {"cds", "eds"}:
            raise ValueError("OMF endpoint_type must be 'cds' or 'eds'")

        self.resource = resource.rstrip("/")
        self.api_version = api_version.strip("/")
        self.tenant_id = tenant_id
        self.namespace_id = namespace_id
        self.client_id = client_id or os.getenv(client_id_env)
        self.client_secret = client_secret or os.getenv(client_secret_env)
        self.token_url = token_url
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.batch_size = max(1, int(batch_size))
        self.container_batch_size = max(1, int(container_batch_size))
        self.use_compression = use_compression
        self.verify_ssl = verify_ssl
        self.timeout_seconds = timeout_seconds
        self.stream_id_separator = stream_id_separator
        self.default_omf_type = default_omf_type
        self.omf_type_map = {
            "default": default_omf_type,
            "number": default_omf_type,
            "double": default_omf_type,
            "integer": "Timeindexed.Integer",
            "string": "Timeindexed.String",
            "boolean": "Timeindexed.Boolean",
        }
        if omf_type_map:
            self.omf_type_map.update(
                {str(key).lower(): value for key, value in omf_type_map.items()}
            )

        self.omf_endpoint = omf_endpoint or self._build_omf_endpoint()
        self._access_token: str | None = None
        self._token_expiration = 0.0
        self._known_containers: set[str] = set()
        self._container_types: dict[str, str] = {}

        if self.endpoint_type == "cds" and (not self.client_id or not self.client_secret):
            raise ValueError(
                "OMF CDS endpoint requires client_id/client_secret or configured env vars"
            )

    def _build_omf_endpoint(self) -> str:
        if not self.resource:
            raise ValueError("OMF resource is required when omf_endpoint is not configured")

        if self.endpoint_type == "cds":
            if not self.tenant_id or not self.namespace_id:
                raise ValueError("OMF CDS endpoint requires tenant_id and namespace_id")
            base_endpoint = (
                f"{self.resource}/api/{self.api_version}/tenants/{self.tenant_id}"
                f"/namespaces/{self.namespace_id}"
            )
        else:
            base_endpoint = (
                f"{self.resource}/api/{self.api_version}/tenants/default/namespaces/default"
            )
        return f"{base_endpoint}/omf"

    def _data_type_for_row(self, row: dict[str, Any]) -> str:
        return str(row.get("data_type") or "double").strip().lower()

    def _container_type_for_row(self, row: dict[str, Any]) -> str:
        data_type = self._data_type_for_row(row)
        return self.omf_type_map.get(data_type, self.default_omf_type)

    def _build_stream_id(self, row: dict[str, Any]) -> str:
        asset = str(row.get("asset", "unknown_asset"))
        sensor = str(row.get("sensor", "unknown_sensor"))
        safe_asset = asset.replace("/", self.stream_id_separator)
        return f"{safe_asset}{self.stream_id_separator}{sensor}"

    def _serialize_timestamp(self, value: Any) -> str:
        if isinstance(value, datetime):
            timestamp = value
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

    def _serialize_value(self, value: Any, data_type: str) -> Any:
        if isinstance(value, datetime | date):
            return value.isoformat()
        if data_type == "integer":
            return int(value)
        if data_type in {"double", "number", "float"}:
            return float(value)
        if data_type == "boolean":
            return bool(value)
        if data_type == "string":
            return str(value)
        return value

    def _build_container(self, container_id: str, omf_type: str) -> dict[str, str]:
        return {"id": container_id, "typeid": omf_type}

    def _build_data_message(
        self, container_id: str, rows: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return {
            "containerid": container_id,
            "values": [
                {
                    "Timestamp": self._serialize_timestamp(row.get("timestamp")),
                    "Value": self._serialize_value(
                        row.get("value"), self._data_type_for_row(row)
                    ),
                }
                for row in rows
            ],
        }

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return

        rows_by_container: dict[str, list[dict[str, Any]]] = {}
        for row in data:
            container_id = self._build_stream_id(row)
            rows_by_container.setdefault(container_id, []).append(row)

        await self._ensure_containers(rows_by_container)
        data_messages: list[dict[str, Any]] = []
        for container_id, rows in rows_by_container.items():
            data_messages.extend(self._chunk_data_messages(container_id, rows))

        self._send_data_messages(data_messages)

    async def _ensure_containers(
        self, rows_by_container: dict[str, list[dict[str, Any]]]
    ) -> None:
        containers: list[dict[str, str]] = []
        for container_id, rows in rows_by_container.items():
            if container_id in self._known_containers:
                continue
            omf_type = self._container_type_for_row(rows[0])
            self._container_types[container_id] = omf_type
            self._known_containers.add(container_id)
            containers.append(self._build_container(container_id, omf_type))

        for start in range(0, len(containers), self.container_batch_size):
            self._send_omf_message(
                "container", containers[start : start + self.container_batch_size]
            )

    def _chunk_data_messages(
        self, container_id: str, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        messages = []
        for start in range(0, len(rows), self.batch_size):
            chunk = rows[start : start + self.batch_size]
            messages.append(
                self._build_data_message(container_id, chunk)
            )
        return messages

    def _send_data_messages(self, data_messages: list[dict[str, Any]]) -> None:
        batch: list[dict[str, Any]] = []
        batch_rows = 0

        for message in data_messages:
            message_rows = len(message.get("values", []))
            if batch and batch_rows + message_rows > self.batch_size:
                self._send_omf_message("data", batch)
                batch = []
                batch_rows = 0

            batch.append(message)
            batch_rows += message_rows

        if batch:
            self._send_omf_message("data", batch)

    def _send_omf_message(
        self, message_type: str, payload: list[dict[str, Any]], action: str = "create"
    ) -> None:
        body: str | bytes
        headers = self._headers(message_type=message_type, action=action)
        if self.use_compression:
            body = gzip.compress(json.dumps(payload).encode("utf-8"))
            headers["compression"] = "gzip"
        else:
            body = json.dumps(payload)

        status, text = self._post(self.omf_endpoint, headers, body)
        if status == 409:
            return
        if status < 200 or status >= 300:
            raise RuntimeError(f"OMF {message_type} message failed: {status}:{text}")

    def _headers(self, message_type: str, action: str) -> dict[str, str]:
        headers = {
            "messagetype": message_type,
            "action": action,
            "messageformat": "JSON",
            "omfversion": "1.2",
        }
        if self.endpoint_type == "cds":
            headers["Authorization"] = f"Bearer {self._get_token()}"
        return headers

    def _get_token(self) -> str:
        if self.endpoint_type != "cds":
            return ""
        if self._access_token and self._token_expiration - time.time() > 300:
            return self._access_token

        token_url = self.token_url or self._discover_token_url()
        status, text = self._post_form(
            token_url,
            {
                "client_id": str(self.client_id),
                "client_secret": str(self.client_secret),
                "grant_type": "client_credentials",
            },
        )
        if status < 200 or status >= 300:
            raise RuntimeError(f"OMF token request failed: {status}:{text}")

        token = json.loads(text)
        self._access_token = str(token["access_token"])
        self._token_expiration = time.time() + float(token.get("expires_in", 3600))
        return self._access_token

    def _discover_token_url(self) -> str:
        discovery_url = f"{self.resource}/identity/.well-known/openid-configuration"
        status, text = self._get(discovery_url, {"Accept": "application/json"})
        if status < 200 or status >= 300:
            raise RuntimeError(f"OMF discovery request failed: {status}:{text}")

        token_endpoint = str(json.loads(text)["token_endpoint"])
        parsed = urlparse(token_endpoint)
        if parsed.scheme != "https" or not token_endpoint.startswith(self.resource):
            raise RuntimeError("OMF discovery returned an unexpected token endpoint")
        self.token_url = token_endpoint
        return token_endpoint

    def _get(self, url: str, headers: dict[str, str]) -> tuple[int, str]:
        request = Request(url, headers=headers, method="GET")
        return self._open(request)

    def _post(
        self, url: str, headers: dict[str, str], body: str | bytes
    ) -> tuple[int, str]:
        request = Request(
            url,
            data=body if isinstance(body, bytes) else body.encode("utf-8"),
            headers=headers,
            method="POST",
        )
        return self._open(request)

    def _post_form(self, url: str, form: dict[str, str]) -> tuple[int, str]:
        body = urlencode(form)
        request = Request(
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        return self._open(request)

    def _open(self, request: Request) -> tuple[int, str]:
        context = None if self.verify_ssl else ssl._create_unverified_context()
        try:
            with urlopen(request, timeout=self.timeout_seconds, context=context) as response:
                return int(response.status), response.read().decode("utf-8")
        except HTTPError as exc:
            return int(exc.code), exc.read().decode("utf-8")

    async def flush(self) -> None:
        return

    async def close(self) -> None:
        return

    def supports_backfill(self) -> bool:
        return self.allow_backfill

    def supports_realtime(self) -> bool:
        return self.allow_realtime
