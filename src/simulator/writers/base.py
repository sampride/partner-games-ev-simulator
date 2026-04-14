from typing import Any, Protocol


class Writer(Protocol):
    """Protocol defining how output writers must behave."""

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        """Process and output a batch of generated sensor data."""
        ...

    async def flush(self) -> None:
        """Flush any buffered data to the underlying sink."""
        ...

    async def close(self) -> None:
        """Flush and close any resources held by the writer."""
        ...

    def supports_backfill(self) -> bool:
        """Return True if this writer should receive data during rapid backfill."""
        ...

    def supports_realtime(self) -> bool:
        """Return True if this writer should receive data during normal real-time execution."""
        ...
