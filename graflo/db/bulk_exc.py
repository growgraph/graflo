"""Exceptions for optional bulk-ingestion features (not all backends support them)."""


class UnsupportedBulkLoad(NotImplementedError):
    """Raised when native bulk load is requested but the connection flavor does not implement it."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
