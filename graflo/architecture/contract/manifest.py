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
            "Manifest-level identity: name, description and provenance. "
            "Excluded from this manifest's own content hash — a content "
            "address that covered its name or its lineage would not be "
            "path-independent, and two routes to the same world model could "
            "never be recognised as equal."
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

    def normalize(self) -> None:
        """Bring the schema, then the ingestion model against it, to normal form.

        The only mutator of the contract; idempotent. A constructed manifest is
        already in normal form unless it was changed in place since.
        """
        if self.graph_schema is None:
            return
        self.graph_schema.normalize()
        if self.ingestion_model is not None:
            self.ingestion_model.normalize(self.graph_schema.core_schema)

    def validate(self, *, strict_references: bool = False) -> None:  # ty: ignore[incompatible-method-override]
        """Check the schema, then every resource against it.

        Pure: reads a normalized manifest and never changes it. Refuses exactly
        what :meth:`finish_init` refuses (without ``dynamic_edge_feedback``).

        Raises:
            ValueError: The first check that fails.
        """
        self._validate_bound_resources()
        if self.graph_schema is None:
            return
        self.graph_schema.validate()
        if self.ingestion_model is not None:
            self.ingestion_model.validate(
                self.graph_schema.core_schema,
                strict_references=strict_references,
                target_db_flavor=self.graph_schema.db_profile.db_flavor,
            )

    def finish_init(
        self,
        *,
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
    ) -> None:
        """Normalize, validate, and build the ingestion runtimes.

        ``dynamic_edge_feedback`` writes the edges each resource's pipeline
        registers into the schema's edge config; it is the one step here that
        changes the schema after :meth:`normalize`.
        """
        self._validate_bound_resources()
        if self.graph_schema is None:
            return
        self.graph_schema.normalize()
        self.graph_schema.validate()
        if self.ingestion_model is not None:
            # Normalizes, then builds the runtimes once, which validates them.
            self.ingestion_model.finish_init(
                self.graph_schema.core_schema,
                strict_references=strict_references,
                dynamic_edge_feedback=dynamic_edge_feedback,
                target_db_flavor=self.graph_schema.db_profile.db_flavor,
            )

    def dangling_bound_resources(self) -> list[str]:
        """Resources the bindings name that the ingestion model does not declare.

        Empty without an ingestion block: bindings alone may name any resource.
        """
        if self.bindings is None or self.ingestion_model is None:
            return []
        declared = {resource.name for resource in self.ingestion_model.resources}
        return sorted(self.bindings.bound_resource_names() - declared)

    def _validate_bound_resources(self) -> None:
        if dangling := self.dangling_bound_resources():
            raise ValueError(
                f"bindings name resources the ingestion model does not declare: "
                f"{dangling}"
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
