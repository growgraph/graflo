"""Ops over a whole manifest: bindings, DB profile, projection, sanitizing."""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.bindings.core import Bindings
from graflo.architecture.graph_types import EdgeDirection
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.onto import DBType

from .edges import EdgeSelector


class SetBindingsOp(ConfigBaseModel):
    """Replace the whole ``bindings`` block.

    The bindings block had **no op at all**, so ``diff_manifests`` could only
    report it as inexpressible and a change set that touched it was not
    replayable -- which is why a merge that unions two bindings registries
    could not be recorded as a commit.

    Wholesale rather than granular (add/remove/rename a connector) because that
    is what the diff needs to say: the block became this. The cost is
    coarseness in a three-way merge -- the whole block is one slot, so two
    independent bindings edits conflict where granular ops would merge. Granular
    connector ops can be added later without changing this one's meaning.
    """

    op: Literal["set_bindings"] = "set_bindings"
    bindings: Bindings | None = PydanticField(
        default=None,
        description=(
            "The bindings block after the op. ``None`` removes it, which is how "
            "the op stays total: every before/after pair is expressible."
        ),
    )


class SetDbProfileOp(ConfigBaseModel):
    """Replace the whole ``db_profile`` of the schema block.

    ``vertex_indexes`` and each edge spec's ``indexes`` already have four
    authoring ops; nothing else on the profile had any, so ``db_flavor``,
    ``target_namespace``, ``vertex_storage_names``, ``default_property_values``
    and the non-index parts of ``edge_specs`` were inexpressible -- and they are
    part of the content hash, so a change set that moved one of them could not
    replay.

    This op carries the **whole** profile, indexes included, and therefore
    subsumes the index ops when it is emitted; the differ emits it *instead of*
    them rather than alongside, so the two can never fight over ordering. When
    only indexes differ, the index ops are still what gets emitted -- they say
    more about intent and merge at a finer slot.
    """

    op: Literal["set_db_profile"] = "set_db_profile"
    profile: DatabaseProfile = PydanticField(
        ...,
        description="The database profile after the op, replacing the current one.",
    )


class ProjectManifestOp(ConfigBaseModel):
    """Project a manifest to a vertex/edge subgraph with consistent cascade.

    Keeps only the requested logical vertices and edges (and optionally resources).
    All schema, ``db_profile``, ingestion, and bindings references to removed
    entities are pruned. Inverse edges are **not** kept by default; list them in
    ``keep_edges``, or set ``keep_inverse_edges`` to keep the declared mirror of
    every edge that is kept.

    With ``connectivity=\"induced_prune\"`` (v1 default), when ``keep_vertices`` is
    set, vertex types from that list with no incident surviving edge are dropped.

    ``depth`` turns ``keep_vertices`` from a literal list into seeds for a
    neighbourhood walk. One rule covers every combination: let ``E`` be
    ``keep_edges`` when given and every declared edge otherwise; the survivors are
    the vertex types within ``depth`` hops of a seed along ``E`` under
    ``direction``, and then ``E`` restricted to surviving endpoints. So the result
    is the *induced* subgraph on the hop ball — an edge between two neighbours
    survives even though no walk needed it — and ``keep_edges`` bounds the walk
    rather than being overridden by it. Pruning is unchanged: a seed left with no
    surviving edge is still dropped, whatever the depth.

    ``Edge.by`` (the third vertex type on an ``EdgeType.INDIRECT`` edge) is not part
    of schema adjacency, so a walk never pulls it in — the same blind spot the flat
    selection already has.
    """

    op: Literal["project_manifest"] = "project_manifest"
    keep_vertices: list[str] | None = PydanticField(
        default=None,
        description="Vertex type names to retain (after induced connectivity pruning).",
    )
    keep_edges: list[EdgeSelector] | None = PydanticField(
        default=None,
        description="Edge triples ``(source, target, relation)`` to retain.",
    )
    connectivity: Literal["induced_prune"] = PydanticField(
        default="induced_prune",
        description="How to interpret ``keep_vertices`` relative to surviving edges.",
    )
    depth: int = PydanticField(
        default=0,
        ge=0,
        description=(
            "Hops to expand ``keep_vertices`` by before the induced slice. "
            "``0`` (default) keeps the literal list."
        ),
    )
    direction: EdgeDirection = PydanticField(
        default=EdgeDirection.ANY,
        description=(
            "Orientation followed when expanding by ``depth``. Edges declared "
            "``directed: false`` are followed both ways regardless. Ignored when "
            "``depth`` is 0."
        ),
    )
    keep_resources: list[str] | None = PydanticField(
        default=None,
        description="Optional ingestion resource names to retain after graph slice.",
    )
    keep_inverse_edges: bool = PydanticField(
        default=False,
        description=(
            "With ``keep_edges``: also keep the declared mirror ``(T, S, inv)`` of "
            "each kept ``(S, T, r)``, so a materialized pair survives as a pair. "
            "Without ``keep_edges`` every edge between surviving vertices is kept "
            "already."
        ),
    )
    strict: bool = PydanticField(
        default=True,
        description="When True, unknown vertex/edge selectors raise ``ValueError``.",
    )
    partial_resources: Literal["trim", "drop"] = PydanticField(
        default="trim",
        description=(
            "A resource whose pipeline the projection shortens: ``trim`` keeps "
            "what survives and logs a warning naming each one; ``drop`` removes "
            "it with the bindings that served it, keeping only resources the "
            "projection leaves whole."
        ),
    )

    @model_validator(mode="after")
    def _validate_projection_selectors(self) -> ProjectManifestOp:
        if not self.keep_vertices and not self.keep_edges:
            raise ValueError(
                "project_manifest requires at least one of keep_vertices or keep_edges"
            )
        if self.keep_vertices and len(self.keep_vertices) != len(
            set(self.keep_vertices)
        ):
            raise ValueError("keep_vertices entries must be unique")
        if self.keep_edges:
            edge_ids = [selector.edge_id() for selector in self.keep_edges]
            if len(edge_ids) != len(set(edge_ids)):
                raise ValueError(
                    "keep_edges entries must be unique by (source, target, relation)"
                )
        if self.depth > 0 and not self.keep_vertices:
            # Without seeds `keep_vertices=None` already means "every vertex type",
            # so there is nothing for a walk to expand — the request is a mistake
            # rather than a no-op, and saying so beats silently ignoring `depth`.
            raise ValueError("project_manifest: depth > 0 requires keep_vertices")
        return self


class SanitizeOp(ConfigBaseModel):
    """Record the physical names a target flavor needs in ``DatabaseProfile``.

    Vertex storage names, relation names and vertex/edge property names that the
    flavor cannot store (reserved word, invalid character, forbidden prefix) get
    a stored name in the profile, deduplicated within each database namespace.
    The logical schema and the ingestion model are left untouched: a backend
    naming constraint is a physical fact, so it never renames a logical property.
    Idempotent.
    """

    op: Literal["sanitize"] = "sanitize"
    db_flavor: DBType = PydanticField(
        ...,
        description="Target database flavor whose reserved words/constraints drive the sanitization.",
    )
    reserved_words: list[str] | None = PydanticField(
        default=None,
        description=(
            "Optional override for the flavor's reserved words. "
            "When unset, ``graflo.db.util.load_reserved_words(db_flavor)`` is used."
        ),
    )
