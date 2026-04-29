from __future__ import annotations

import asyncio
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

DEFAULT_MAX_OMF_BODY_BYTES = 180 * 1024
MAX_OMF_BODY_BYTES = 192 * 1024


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
        auth_resource: str | None = None,
        token_discovery_url: str | None = None,
        token_url: str | None = None,
        allow_backfill: bool = True,
        allow_realtime: bool = True,
        batch_size: int = 5000,
        container_batch_size: int = 1000,
        max_body_bytes: int = DEFAULT_MAX_OMF_BODY_BYTES,
        max_concurrent_requests: int = 1,
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
        self.auth_resource = (auth_resource.rstrip("/") if auth_resource else self.resource)
        self.token_discovery_url = token_discovery_url
        self.token_url = token_url
        self.allow_backfill = allow_backfill
        self.allow_realtime = allow_realtime
        self.batch_size = max(1, int(batch_size))
        self.container_batch_size = max(1, int(container_batch_size))
        self.max_body_bytes = min(MAX_OMF_BODY_BYTES, max(1024, int(max_body_bytes)))
        self.max_concurrent_requests = max(1, int(max_concurrent_requests))
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
        await self._send_data_rows(rows_by_container)

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

        await self._send_sized_omf_batches(
            "container",
            containers,
            max_items_per_batch=self.container_batch_size,
        )

    async def _send_data_rows(self, rows_by_container: dict[str, list[dict[str, Any]]]) -> None:
        batches: list[list[dict[str, Any]]] = []
        batch: list[dict[str, Any]] = []
        batch_rows = 0
        batch_size_bytes = self._list_payload_size([])

        for container_id, rows in rows_by_container.items():
            current_values: list[dict[str, Any]] = []
            current_values_size = self._list_payload_size([])
            message_base_size = self._data_message_base_size(container_id)
            for row in rows:
                value = self._build_value(row)
                value_size = self._dict_payload_size(value)
                candidate_values_size = self._append_size(
                    current_values_size, bool(current_values), value_size
                )
                candidate_message_size = message_base_size + candidate_values_size
                if current_values and (
                    len(current_values) + 1 > self.batch_size
                    or self._append_size(
                        batch_size_bytes, bool(batch), candidate_message_size
                    ) > self.max_body_bytes
                ):
                    message = self._build_data_message_from_values(
                        container_id, current_values
                    )
                    message_size = message_base_size + current_values_size
                    if batch and (
                        batch_rows + len(current_values) > self.batch_size
                        or self._append_size(batch_size_bytes, True, message_size)
                        > self.max_body_bytes
                    ):
                        batches.append(batch)
                        batch = []
                        batch_rows = 0
                        batch_size_bytes = self._list_payload_size([])
                    batch.append(message)
                    batch_rows += len(current_values)
                    batch_size_bytes = self._append_size(
                        batch_size_bytes, bool(batch) and len(batch) > 1, message_size
                    )
                    current_values = [value]
                    current_values_size = self._append_size(
                        self._list_payload_size([]), False, value_size
                    )
                    continue
                current_values.append(value)
                current_values_size = candidate_values_size

            if not current_values:
                continue

            message = self._build_data_message_from_values(container_id, current_values)
            message_size = message_base_size + current_values_size
            if batch and (
                batch_rows + len(current_values) > self.batch_size
                or self._append_size(batch_size_bytes, True, message_size)
                > self.max_body_bytes
            ):
                batches.append(batch)
                batch = []
                batch_rows = 0
                batch_size_bytes = self._list_payload_size([])

            batch.append(message)
            batch_rows += len(current_values)
            batch_size_bytes = self._append_size(
                batch_size_bytes, bool(batch) and len(batch) > 1, message_size
            )

        if batch:
            batches.append(batch)

        await self._send_omf_batches("data", batches)

    def _build_value(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "Timestamp": self._serialize_timestamp(row.get("timestamp")),
            "Value": self._serialize_value(row.get("value"), self._data_type_for_row(row)),
        }

    def _build_data_message_from_values(
        self, container_id: str, values: list[dict[str, Any]]
    ) -> dict[str, Any]:
        return {"containerid": container_id, "values": values}

    def _payload_json(self, payload: list[dict[str, Any]]) -> str:
        return json.dumps(payload, separators=(",", ":"))

    def _payload_size(self, payload: list[dict[str, Any]]) -> int:
        return len(self._payload_json(payload).encode("utf-8"))

    def _dict_payload_size(self, payload: dict[str, Any]) -> int:
        return len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))

    def _list_payload_size(self, payload: list[dict[str, Any]]) -> int:
        return len(self._payload_json(payload).encode("utf-8"))

    def _append_size(self, current_list_size: int, has_items: bool, item_size: int) -> int:
        separator_size = 1 if has_items else 0
        return current_list_size + separator_size + item_size

    def _data_message_base_size(self, container_id: str) -> int:
        empty_message = self._build_data_message_from_values(container_id, [])
        return self._dict_payload_size(empty_message) - self._list_payload_size([])

    async def _send_sized_omf_batches(
        self,
        message_type: str,
        payloads: list[dict[str, Any]],
        *,
        max_items_per_batch: int,
    ) -> None:
        batches: list[list[dict[str, Any]]] = []
        batch: list[dict[str, Any]] = []
        batch_size_bytes = self._list_payload_size([])
        for payload in payloads:
            payload_size = self._dict_payload_size(payload)
            candidate_size = self._append_size(
                batch_size_bytes, bool(batch), payload_size
            )
            if batch and (
                len(batch) + 1 > max_items_per_batch
                or candidate_size > self.max_body_bytes
            ):
                batches.append(batch)
                batch = []
                batch_size_bytes = self._list_payload_size([])
            batch.append(payload)
            batch_size_bytes = self._append_size(
                batch_size_bytes, bool(batch) and len(batch) > 1, payload_size
            )
        if batch:
            batches.append(batch)

        await self._send_omf_batches(message_type, batches)

    async def _send_omf_batches(
        self, message_type: str, batches: list[list[dict[str, Any]]]
    ) -> None:
        if not batches:
            return

        if self.max_concurrent_requests == 1 or len(batches) == 1:
            for batch in batches:
                self._send_omf_message(message_type, batch)
            return

        if self.endpoint_type == "cds":
            self._get_token()

        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def send_batch(batch: list[dict[str, Any]]) -> None:
            async with semaphore:
                await asyncio.to_thread(self._send_omf_message, message_type, batch)

        await asyncio.gather(*(send_batch(batch) for batch in batches))

    def _send_omf_message(
        self, message_type: str, payload: list[dict[str, Any]], action: str = "create"
    ) -> None:
        body: str | bytes
        headers = self._headers(message_type=message_type, action=action)
        payload_json = self._payload_json(payload)
        if self.use_compression:
            body = gzip.compress(payload_json.encode("utf-8"))
            headers["compression"] = "gzip"
        else:
            body = payload_json

        status, text = self._post(self.omf_endpoint, headers, body)
        if status == 409:
            return
        if status < 200 or status >= 300:
            raise RuntimeError(f"OMF {message_type} message failed: {status}:{text}")

    def _headers(self, message_type: str, action: str) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
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
        discovery_url = (
            self.token_discovery_url
            or f"{self.auth_resource}/identity/.well-known/openid-configuration"
        )
        status, text = self._get(discovery_url, {"Accept": "application/json"})
        if status < 200 or status >= 300:
            raise RuntimeError(f"OMF discovery request failed: {status}:{text}")

        token_endpoint = str(json.loads(text)["token_endpoint"])
        parsed = urlparse(token_endpoint)
        if parsed.scheme != "https":
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
