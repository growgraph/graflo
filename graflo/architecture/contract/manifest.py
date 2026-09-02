"""Graph manifest model for complete ingestion contracts."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, ConfigDict, model_validator
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema import Schema

from .bindings import Bindings
from .ingestion.model import IngestionModel
from .provenance import ManifestMetadata


class GraphManifest(ConfigBaseModel):
    """Canonical config contract for graph schema, ingestion, and bindings."""

    model_config = ConfigDict(populate_by_name=True)

    graph_schema: Schema | None = PydanticField(
        default=None,
        description="Logical graph schema contract.",
        validation_alias=AliasChoices("schema", "graph_schema"),
        serialization_alias="schema",
    )
    ingestion_model: IngestionModel | None = PydanticField(
        default=None,
        description="Ingestion resources and transforms.",
    )
    bindings: Bindings | None = PydanticField(
        default=None,
        description="Bindings mapping resources to concrete data sources.",
    )
    metadata: ManifestMetadata | None = PydanticField(
        default=None,
        description=(
            "Manifest-level metadata, currently provenance only. Excluded from "
            "this manifest's own content hash: a content address that covered "
            "its lineage would not be path-independent, and two routes to the "
            "same world model could never be recognised as equal."
        ),
    )

    @classmethod
    def from_config(cls, data: dict[str, Any]) -> GraphManifest:
        """Build a manifest from a Python mapping payload."""
        return cls.from_dict(data)

    @model_validator(mode="after")
    def _validate_manifest(self) -> GraphManifest:
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
                self.graph_schema.core_schema,
                strict_references=strict_references,
                dynamic_edge_feedback=dynamic_edge_feedback,
                target_db_flavor=self.graph_schema.db_profile.db_flavor,
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
