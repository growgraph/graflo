"""GraFlo typed output: full schema document plus graph data."""

from __future__ import annotations

from pydantic import AliasChoices, Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema


class GraFloOutput(ConfigBaseModel):
    """Self-describing GraFlo dataset: schema metadata/profile and graph data."""

    graph_schema: Schema = PydanticField(
        ...,
        description="Full graph schema (metadata, core schema, db profile).",
        validation_alias=AliasChoices("schema", "graph_schema"),
        serialization_alias="schema",
    )
    data: GraphContainer = PydanticField(
        ...,
        description="Graph data container (vertices, edges, lineage).",
    )

    @property
    def schema(self) -> Schema:
        """Alias for :attr:`graph_schema` (avoids shadowing Pydantic's ``schema``)."""
        return self.graph_schema

    @property
    def core_schema(self) -> CoreSchema:
        """Logical graph model (vertices and edges) from :attr:`graph_schema`."""
        return self.graph_schema.core_schema
