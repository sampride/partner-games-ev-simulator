from typing import Protocol, Any

class Writer(Protocol):
    """Protocol defining how output writers must behave."""

    async def write_batch(self, data: list[dict[str, Any]]) -> None:
        """Process and output a batch of generated sensor data."""
        ...

    def supports_backfill(self) -> bool:
        """Return True if this writer should receive data during rapid backfill."""
        ...

    def supports_realtime(self) -> bool:
        """Return True if this writer should receive data during normal real-time execution."""
        ...