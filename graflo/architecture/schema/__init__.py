from .core import CoreSchema
from .db_aware import (
    EdgeConfigDBAware,
    EdgeRuntime,
    SchemaDBAware,
    VertexConfigDBAware,
)
from .document import Schema
from .graflo_output import GraFloOutput
from .metadata import GraphMetadata

# Server and legacy code refer to the logical graph (A) as ``GraphModel``.
GraphModel = CoreSchema

__all__ = [
    "CoreSchema",
    "EdgeConfigDBAware",
    "EdgeRuntime",
    "GraFloOutput",
    "GraphMetadata",
    "GraphModel",
    "Schema",
    "SchemaDBAware",
    "VertexConfigDBAware",
]
