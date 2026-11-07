"""What a batch becomes before a backend sees it, without a connection.

Every write of a cast batch runs the same steps: derived fields are set,
generated identities minted, documents with no identity value dropped, and
logical names translated to the names the target stores. They read the schema
and the target flavor only, so the upsert writer and any other writer of the
same graph run them identically.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from graflo.architecture.evolution.sanitize import with_physical_names
from graflo.architecture.schema import EdgeRuntime, Schema, SchemaDBAware
from graflo.architecture.schema.derive import derive_fields
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_digest import (
    ensure_digest_identities_on_docs,
)
from graflo.architecture.schema.identity_uuid import (
    ensure_assigned_uuids_on_docs,
    validate_uuid_typed_identity_fields,
)
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.onto import DBType

logger = logging.getLogger(__name__)

CYPHER_FLAVORS = frozenset({DBType.NEO4J, DBType.MEMGRAPH, DBType.FALKORDB})


class WriteContext(BaseModel):
    """The schema resolved for one target flavor, with its stored names.

    Built from the flavor alone: no connection is opened.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    named_schema: Schema
    db_aware: SchemaDBAware
    keys: PhysicalKeys
    flavor: DBType

    @classmethod
    def for_flavor(cls, schema: Schema, flavor: DBType) -> WriteContext:
        """Resolve *schema* for *flavor*, physical names filled in as DDL declares them."""
        named = with_physical_names(schema, flavor)
        return cls(
            named_schema=named,
            db_aware=named.resolve_db_aware(flavor),
            keys=PhysicalKeys(named.db_profile),
            flavor=flavor,
        )


class StoredVertices(BaseModel):
    """One vertex type's documents as the target stores them."""

    docs: list[dict[str, Any]]
    class_name: str
    match_keys: list[str]
    union_fields: tuple[str, ...]


class StoredEdges(BaseModel):
    """One concrete edge's rows as the target stores them."""

    docs: list[Any]
    source_class: str
    target_class: str
    relation_name: str | None
    match_keys_source: tuple[str, ...]
    match_keys_target: tuple[str, ...]
    collection_name: str | None
    merge_properties: tuple[str, ...] | None
    union_fields: tuple[str, ...]


# -- vertices ---------------------------------------------------------------


def prepare_vertices(ctx: WriteContext, vcol: str, data: list[dict]) -> list[dict]:
    """Set derived fields and generated identities on *data*; return what can be written.

    *data* is changed in place. Documents with none of their vertex's identity
    fields are left out of the result.
    """
    derive_vertex_fields(ctx, vcol, data)
    mint_identities(ctx, vcol, data)
    return drop_unkeyed(ctx, vcol, data)


def derive_vertex_fields(ctx: WriteContext, vcol: str, data: list[dict]) -> None:
    """Set *vcol*'s derived fields on every document, overwriting a stated value."""
    vertex = ctx.db_aware.vertex_config.logical._get_vertex_by_name(vcol)
    if vertex.derived_fields:
        for doc in data:
            derive_fields(vertex, doc)


def mint_identities(ctx: WriteContext, vcol: str, data: list[dict]) -> None:
    """Fill *vcol*'s generated identity on *data*, by its identity mode.

    ``hash`` vertices get their digest and ``assigned`` vertices a uuid4, each
    only where no value is set (the cast normally sets both); ``blank`` vertices
    get a random id. A natural identity is never invented: UUID-typed identity
    fields are validated instead.

    Raises:
        ValueError: A UUID-typed identity value that is not a UUID.
    """
    vc = ctx.db_aware.vertex_config
    _, preferred = default_and_preferred(ctx, vcol)
    if vc.is_hash_identity(vcol):
        vertex = vc.logical._get_vertex_by_name(vcol)
        ensure_digest_identities_on_docs(data, vertex, preferred_field=preferred)
    elif vc.is_assigned(vcol):
        ensure_assigned_uuids_on_docs(data, preferred_field=preferred)
    elif vc.is_blank(vcol):
        for doc in data:
            if doc.get(preferred) is None or doc.get(preferred) == "":
                doc[preferred] = str(uuid4())
    else:
        vertex = vc.logical._get_vertex_by_name(vcol)
        for doc in data:
            validate_uuid_typed_identity_fields(doc, vertex)
        return
    mirror_default_identity(ctx, vcol, data)


def drop_unkeyed(ctx: WriteContext, vcol: str, data: list[dict]) -> list[dict]:
    """The documents of *data* that carry at least one identity value.

    A document with no key cannot be upserted: every backend would either
    invent one or fold the batch onto a single keyless vertex. It usually means
    the resource references the vertex rather than owning it.
    """
    identity_fields = ctx.db_aware.vertex_config.identity_fields(vcol)
    if not identity_fields:
        return data
    writable = [
        doc for doc in data if any(doc.get(f) is not None for f in identity_fields)
    ]
    dropped = len(data) - len(writable)
    if dropped:
        logger.warning(
            "Skipped %s '%s' document(s) with no identity value for %s; "
            "they cannot be upserted. Mark the step lookup_only if the "
            "resource only references this vertex.",
            dropped,
            vcol,
            identity_fields,
        )
    return writable


def default_and_preferred(ctx: WriteContext, vcol: str) -> tuple[str, str]:
    """The flavor's default key field and *vcol*'s first identity field.

    The default is ``_key`` on Arango and ``id`` elsewhere; a vertex without
    identity fields is keyed on the default itself.
    """
    identity_fields = ctx.db_aware.vertex_config.identity_fields(vcol)
    default_field = "_key" if ctx.flavor == DBType.ARANGO else "id"
    return default_field, identity_fields[0] if identity_fields else default_field


def mirror_default_identity(ctx: WriteContext, vcol: str, data: list[dict]) -> None:
    """Copy the first identity value into the flavor's default key field.

    A value already in the default field is never overwritten.
    """
    default_field, preferred = default_and_preferred(ctx, vcol)
    if default_field == preferred:
        return
    for doc in data:
        value = doc.get(preferred)
        if value is not None and value != "" and default_field not in doc:
            doc[default_field] = value


def union_fields(ctx: WriteContext, vcol: str) -> tuple[str, ...]:
    """Logical names of *vcol*'s ``merge: union`` fields."""
    return tuple(
        f.name
        for f in ctx.db_aware.vertex_config.properties(vcol)
        if f.merge == "union"
    )


def project_vertices(
    ctx: WriteContext, vcol: str, docs: list[dict[str, Any]]
) -> StoredVertices:
    """*docs* of *vcol*, with the storage name, key and union fields, as stored."""
    vc = ctx.db_aware.vertex_config
    keys = ctx.keys
    return StoredVertices(
        docs=keys.vertex_docs(vcol, docs),
        class_name=vc.vertex_dbname(vcol),
        match_keys=keys.vertex_fields(vcol, vc.identity_fields(vcol)),
        union_fields=tuple(keys.vertex_fields(vcol, list(union_fields(ctx, vcol)))),
    )


# -- edges ------------------------------------------------------------------


def edge_union_fields(edge: Edge) -> list[str]:
    """Logical names of *edge*'s ``merge: union`` fields."""
    return [f.name for f in edge.properties if f.merge == "union"]


def project_edge_docs(
    ctx: WriteContext, docs: list, relation: str | None, runtime: EdgeRuntime
) -> tuple[list, str | None]:
    """*docs* of one concrete relation, and the relation name the target stores.

    Cypher targets take the relationship type the profile names for the
    relation. TigerGraph keeps one edge type and, when configured, stores an
    extracted relation as an edge attribute.
    """
    if ctx.flavor in CYPHER_FLAVORS:
        edge = runtime.edge
        return docs, runtime.db_profile.edge_relation_name(
            (edge.source, edge.target, relation), default_relation=relation
        )
    if ctx.flavor != DBType.TIGERGRAPH:
        return docs, relation

    relation_name = runtime.relation_name
    relation_field = runtime.effective_relation_field
    if not runtime.store_extracted_relation_as_weight or relation_field is None:
        return docs, relation_name
    projected: list = []
    for source_doc, target_doc, weight in docs:
        next_weight = dict(weight)
        if relation is not None:
            next_weight[relation_field] = relation
        projected.append((source_doc, target_doc, next_weight))
    return projected, relation_name


def project_edges(
    ctx: WriteContext, edge: Edge, relation: str | None, docs: list
) -> StoredEdges:
    """Rows of *edge* under the concrete *relation*, with everything a target needs, as stored."""
    vc = ctx.db_aware.vertex_config
    ec = ctx.db_aware.edge_config
    keys = ctx.keys
    runtime = ec.runtime(edge)
    merge = ec.relationship_merge_property_names(edge)
    data, relation_name = project_edge_docs(ctx, docs, relation, runtime)
    return StoredEdges(
        docs=keys.edge_triples(edge.edge_id, edge.source, edge.target, data),
        source_class=vc.vertex_dbname(edge.source),
        target_class=vc.vertex_dbname(edge.target),
        relation_name=relation_name,
        match_keys_source=tuple(
            keys.vertex_fields(edge.source, vc.identity_fields(edge.source))
        ),
        match_keys_target=tuple(
            keys.vertex_fields(edge.target, vc.identity_fields(edge.target))
        ),
        collection_name=runtime.storage_name(),
        merge_properties=tuple(keys.edge_fields(edge.edge_id, merge))
        if merge
        else None,
        union_fields=tuple(keys.edge_fields(edge.edge_id, edge_union_fields(edge))),
    )
