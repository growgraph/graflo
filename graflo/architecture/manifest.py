"""Graph manifest model for complete ingestion contracts."""

from __future__ import annotations

from typing import Any

from pydantic import Field as PydanticField, model_validator, ConfigDict

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.bindings import Bindings
from graflo.architecture.ingestion_model import IngestionModel
from graflo.architecture.schema import Schema


class GraphManifest(ConfigBaseModel):
    """Canonical config contract for graph schema, ingestion, and bindings."""

    model_config = ConfigDict(populate_by_name=True)

    graph_schema: Schema | None = PydanticField(
        default=None, description="Logical graph schema contract.", alias="schema"
    )
    ingestion_model: IngestionModel | None = PydanticField(
        default=None,
        description="Ingestion resources and transforms.",
    )
    bindings: Bindings | None = PydanticField(
        default=None,
        description="Bindings mapping resources to concrete data sources.",
    )

    @classmethod
    def from_config(cls, data: dict[str, Any]) -> "GraphManifest":
        """Build a manifest from a Python mapping payload."""
        return cls.from_dict(data)

    @model_validator(mode="after")
    def _validate_manifest(self) -> "GraphManifest":
        if (
            self.graph_schema is None
            and self.ingestion_model is None
            and self.bindings is None
        ):
            raise ValueError(
                "GraphManifest requires at least one block: "
                "schema, ingestion_model, or bindings."
            )
        return self

    def finish_init(
        self,
        *,
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
    ) -> None:
        """Initialize model internals and cross-block runtime links."""
        if self.graph_schema is not None:
            self.graph_schema.finish_init()
        if self.graph_schema is not None and self.ingestion_model is not None:
            self.ingestion_model.finish_init(
                self.graph_schema.graph,
                strict_references=strict_references,
                dynamic_edge_feedback=dynamic_edge_feedback,
            )

    def require_schema(self) -> Schema:
        if self.graph_schema is None:
            raise ValueError("GraphManifest is missing required 'schema' block.")
        return self.graph_schema

    def require_ingestion_model(self) -> IngestionModel:
        if self.ingestion_model is None:
            raise ValueError(
                "GraphManifest is missing required 'ingestion_model' block."
            )
        return self.ingestion_model

    def require_bindings(self) -> Bindings:
        if self.bindings is None:
            raise ValueError("GraphManifest is missing required 'bindings' block.")
        return self.bindings
