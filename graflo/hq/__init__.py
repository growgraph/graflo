"""High-level orchestration modules for graflo.

This package provides high-level orchestration classes that coordinate
multiple components for graph database operations.

The façade is lazy (PEP 562): importing ``graflo.hq`` is cheap; orchestration
classes (and their DB/data-source dependencies) load on first attribute access.
"""

from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, str] = {
    "CastBatchResult": "graflo.hq.caster",
    "Caster": "graflo.hq.caster",
    "DocCastFailure": "graflo.hq.caster",
    "DocErrorBudgetExceeded": "graflo.hq.caster",
    "IngestionParams": "graflo.hq.caster",
    "DBWriter": "graflo.hq.db_writer",
    "DocErrorSink": "graflo.hq.doc_error_sink",
    "JsonlGzDocErrorSink": "graflo.hq.doc_error_sink",
    "failure_sinks_from_ingestion_params": "graflo.hq.doc_error_sink",
    "GraphEngine": "graflo.hq.graph_engine",
    "RegistryBuilder": "graflo.hq.registry_builder",
    "ResourceMapper": "graflo.hq.resource_mapper",
    "Sanitizer": "graflo.hq.sanitizer",
    "SQLInferenceManager": "graflo.hq.sql_inferencer",
}

__all__ = [
    "CastBatchResult",
    "Caster",
    "DBWriter",
    "DocCastFailure",
    "DocErrorBudgetExceeded",
    "DocErrorSink",
    "GraphEngine",
    "IngestionParams",
    "JsonlGzDocErrorSink",
    "RegistryBuilder",
    "ResourceMapper",
    "SQLInferenceManager",
    "Sanitizer",
    "failure_sinks_from_ingestion_params",
]


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        import importlib

        value = getattr(importlib.import_module(_EXPORTS[name]), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
