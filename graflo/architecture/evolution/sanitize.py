"""Physical naming for a target database flavor.

A name the database cannot store -- a reserved word, a character it rejects, a
forbidden prefix -- is a fact about the database, not about the graph. So
sanitization never rewrites the logical schema or the ingestion model: it
records the stored name in
:class:`~graflo.architecture.schema.database_features.DatabaseProfile`, next to
the other physical choices:

- ``vertex_storage_names`` -- vertex collection / label / tag names
- ``edge_specs[].relation_name`` -- relation (edge type) names
- ``vertex_property_names`` -- vertex attribute names
- ``edge_specs[].property_names`` -- edge attribute names

:func:`assign_physical_names` computes those names. The write path needs two
views of a schema, and this module builds both:

- :func:`with_physical_names` -- the logical schema with a complete profile.
  Documents keep their logical keys; the writer translates them at the
  database call.
- :func:`materialize_physical_schema` -- the schema as the database stores it,
  for DDL and for backends that take a whole schema. :func:`physical_schema`
  composes the two.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from functools import lru_cache

from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema import Schema
from graflo.architecture.schema.database_features import (
    DatabaseProfile,
    EdgePhysicalSpec,
)
from graflo.architecture.schema.edge import Edge
from graflo.onto import DBType

logger = logging.getLogger(__name__)

VERTEX_SUFFIX = "_vertex"
RELATION_SUFFIX = "_relation"
PROPERTY_SUFFIX = "_attr"

#: ``(name, suffix) -> stored name``; returns *name* when it is already valid.
IdentifierSanitizer = Callable[[str, str], str]

_EDGE_IDENTITY_TOKENS = frozenset({"source", "target", "relation"})


def identifier_sanitizer(
    reserved_words: set[str], db_flavor: DBType
) -> IdentifierSanitizer | None:
    """The name rewriter for *db_flavor*, or ``None`` when every name is valid.

    TigerGraph also replaces invalid characters and forbidden prefixes, using
    the rules its DDL validation rejects on. Other flavors only avoid
    *reserved_words* (uppercase), and have none unless a caller supplies them.
    """
    from graflo.db.util import (
        load_tigergraph_identifier_rules,
        sanitize_attribute_name,
        sanitize_tigergraph_identifier,
    )

    if db_flavor == DBType.TIGERGRAPH:
        rules = load_tigergraph_identifier_rules()
        if rules is not None:
            effective_reserved = reserved_words or set(rules.reserved_words_upper)

            def sanitize(name: str, suffix: str) -> str:
                return sanitize_tigergraph_identifier(
                    name,
                    effective_reserved,
                    rules.forbidden_prefixes,
                    rules.invalid_characters,
                    suffix=suffix,
                )

            return sanitize

    if not reserved_words:
        return None
    return lambda name, suffix: sanitize_attribute_name(
        name, reserved_words, suffix=suffix
    )


def _assign(
    names: Iterable[str],
    sanitize: IdentifierSanitizer,
    suffix: str,
    taken: set[str],
) -> dict[str, str]:
    """Stored name for each of *names* that needs one, unique within *taken*.

    A name needs one when the database cannot store it or when *taken* (names
    another kind of element already holds in the same namespace) contains it.
    Names that are fine are claimed first, so they never move to make room for
    a sanitized neighbour; the rest are sanitized in order and deduplicated
    with ``_N``. *taken* is extended with every name claimed.

    Deterministic, and idempotent: rerun over its own output it changes nothing,
    because every stored name it produces is valid and unique.
    """
    from graflo.db.util import unique_name

    sanitized = {name: sanitize(name, suffix) for name in dict.fromkeys(names)}
    external = set(taken)
    bad = [n for n, fixed in sanitized.items() if n in external or fixed != n]
    if not bad:
        taken.update(sanitized)
        return {}
    taken.update(n for n in sanitized if n not in bad)
    out: dict[str, str] = {}
    for name in bad:
        candidate = sanitized[name]
        if candidate in external:
            candidate = f"{candidate}{suffix}"
        stored = unique_name(candidate, taken)
        taken.add(stored)
        out[name] = stored
    return out


def assign_physical_names(
    schema: Schema,
    reserved_words: set[str],
    *,
    db_flavor: DBType,
    profile: DatabaseProfile | None = None,
) -> None:
    """Record in the profile the stored name of every element of *schema* that needs one.

    Covers vertex storage names, relation names, and vertex and edge property
    names. Existing profile names are the starting point, so a name already
    chosen (by an author or an earlier run) is kept while it stays valid; only
    names the database would reject are replaced.

    Scopes follow the database's namespaces: vertex and relation names share
    one (a relation may not reuse a vertex's name), each vertex's attributes
    form one, and so do the attributes of all edges sharing a relation, since
    TigerGraph declares one attribute list per edge type.

    Mutates *profile* (default ``schema.db_profile``) only; ``schema.core_schema``
    is read, never written. Linear in the schema's size: it runs on every write
    and schema-aware read, so edge specs are indexed once rather than scanned
    per edge, and each distinct name is sanitized once.
    """
    raw = identifier_sanitizer(reserved_words, db_flavor)
    if raw is None:
        return
    # Property names repeat across types (``id``, ``name``...), so most calls hit.
    sanitize: IdentifierSanitizer = lru_cache(maxsize=None)(raw)

    if profile is None:
        profile = schema.db_profile
    vertices = schema.core_schema.vertex_config.vertices
    edges = schema.core_schema.edge_config.edges

    base_specs: dict[EdgeId, EdgePhysicalSpec] = {
        spec.edge_id: spec for spec in profile.edge_specs if spec.purpose is None
    }

    def base_spec(edge_id: EdgeId) -> EdgePhysicalSpec:
        spec = base_specs.get(edge_id)
        if spec is None:
            source, target, relation = edge_id
            spec = EdgePhysicalSpec(source=source, target=target, relation=relation)
            profile.edge_specs.append(spec)
            base_specs[edge_id] = spec
        return spec

    # -- vertex storage names -------------------------------------------------
    stored_vertex = {v.name: profile.vertex_storage_name(v.name) for v in vertices}
    vertex_namespace: set[str] = set()
    renamed = _assign(stored_vertex.values(), sanitize, VERTEX_SUFFIX, vertex_namespace)
    for vertex_name, stored in stored_vertex.items():
        if stored in renamed:
            logger.debug(
                "Physical name for vertex '%s': '%s' -> '%s'",
                vertex_name,
                stored,
                renamed[stored],
            )
            profile.vertex_storage_names[vertex_name] = renamed[stored]

    # -- relation names -------------------------------------------------------
    stored_relation: dict[EdgeId, str | None] = {}
    for edge in edges:
        spec = base_specs.get(edge.edge_id)
        stored_relation[edge.edge_id] = (
            spec.relation_name
            if spec is not None and spec.relation_name is not None
            else edge.relation
        )
    renamed = _assign(
        (
            name
            for edge in edges
            if edge.relation and (name := stored_relation[edge.edge_id]) is not None
        ),
        sanitize,
        RELATION_SUFFIX,
        set(vertex_namespace),
    )
    for edge in edges:
        stored = stored_relation[edge.edge_id]
        if edge.relation and stored in renamed:
            base_spec(edge.edge_id).relation_name = renamed[stored]
            stored_relation[edge.edge_id] = renamed[stored]

    # -- vertex property names ------------------------------------------------
    for vertex in vertices:
        current = profile.vertex_property_names.get(vertex.name, {})
        stored_props = {p: current.get(p, p) for p in vertex.property_names}
        renamed = _assign(stored_props.values(), sanitize, PROPERTY_SUFFIX, set())
        if renamed:
            profile.vertex_property_names[vertex.name] = {
                **current,
                **{
                    logical: renamed[stored]
                    for logical, stored in stored_props.items()
                    if stored in renamed
                },
            }

    # -- edge property names, one scope per stored relation -------------------
    by_relation: dict[str | None, list[Edge]] = {}
    for edge in edges:
        by_relation.setdefault(stored_relation[edge.edge_id], []).append(edge)
    for group in by_relation.values():
        current: dict[str, str] = {}
        for edge in group:
            spec = base_specs.get(edge.edge_id)
            if spec is not None:
                current.update(spec.property_names)
        logical_props = list(
            dict.fromkeys(p for edge in group for p in edge.property_names)
        )
        renamed = _assign(
            (current.get(p, p) for p in logical_props),
            sanitize,
            PROPERTY_SUFFIX,
            set(),
        )
        if not renamed:
            continue
        for edge in group:
            names = {
                p: renamed[current.get(p, p)]
                for p in edge.property_names
                if current.get(p, p) in renamed
            }
            if names:
                spec = base_spec(edge.edge_id)
                spec.property_names = {**spec.property_names, **names}


def _revalidated(profile: DatabaseProfile) -> DatabaseProfile:
    return DatabaseProfile.model_validate(profile.to_dict(skip_defaults=False))


def with_physical_names(
    schema: Schema,
    db_flavor: DBType | None = None,
    *,
    reserved_words: Iterable[str] | None = None,
) -> Schema:
    """*schema* for *db_flavor*, with a profile that names every stored element.

    Names already in the profile win; the rest are filled in by
    :func:`assign_physical_names`. The logical schema is unchanged, so this is
    safe to apply on every write: a manifest that was never sanitized deploys
    with the same names a sanitized one would record.

    Cheap by construction, since it runs on every write and read: only the
    profile is copied, and the returned schema **shares** ``metadata`` and
    ``core_schema`` with *schema* -- treat it as read-only. When the flavor has
    no naming rules and *schema* already targets it, *schema* itself is
    returned.

    Args:
        schema: The authored schema; not mutated.
        db_flavor: Target flavor. Defaults to ``schema.db_profile.db_flavor``.
        reserved_words: Override for the flavor's reserved words.
    """
    from graflo.db.util import load_reserved_words

    flavor = db_flavor if db_flavor is not None else schema.db_profile.db_flavor
    words = (
        {word.upper() for word in reserved_words}
        if reserved_words is not None
        else load_reserved_words(flavor)
    )
    has_rules = identifier_sanitizer(words, flavor) is not None
    if not has_rules and schema.db_profile.db_flavor == flavor:
        return schema

    profile = schema.db_profile.model_copy(deep=True)
    profile.db_flavor = flavor
    if has_rules:
        assign_physical_names(schema, words, db_flavor=flavor, profile=profile)
        profile = _revalidated(profile)
        profile.reconcile_property_names(
            schema.core_schema.vertex_config, schema.core_schema.edge_config
        )
    return schema.model_copy(update={"db_profile": profile})


def _renamed_fields(fields: list[str], names: dict[str, str]) -> list[str]:
    return [names.get(field, field) for field in fields]


def materialize_physical_schema(schema: Schema) -> Schema:
    """*schema* as the database stores it: property names are the physical ones.

    The profile's property maps are folded into the core schema (properties,
    identities, secondary identities, indexes, GSQL defaults) and then cleared,
    so every backend keeps reading names straight off the schema. Storage and
    relation names already live in the profile and stay there.

    Returns *schema* itself when no property is renamed.
    """
    if not schema.db_profile.has_property_names():
        return schema

    from .apply import relabel_vertex_fields

    view = schema.model_copy(deep=True)
    profile = view.db_profile
    vertex_maps = {
        name: dict(names) for name, names in profile.vertex_property_names.items()
    }
    edge_maps = {
        spec.edge_id: dict(spec.property_names)
        for spec in profile.edge_specs
        if spec.purpose is None and spec.property_names
    }

    vertex_config = view.core_schema.vertex_config
    vertex_config.vertices = [
        relabel_vertex_fields(vertex, vertex_maps.get(vertex.name, {}))
        for vertex in vertex_config.vertices
    ]
    for edge in view.core_schema.edge_config.edges:
        names = edge_maps.get(edge.edge_id)
        if not names:
            continue
        edge.properties = [
            field
            if field.name not in names
            else field.model_copy(update={"name": names[field.name]})
            for field in edge.properties
        ]
        edge.identities = [
            [
                token if token in _EDGE_IDENTITY_TOKENS else names.get(token, token)
                for token in identity
            ]
            for identity in edge.identities
        ]

    # Profile entries are keyed by logical names; in this view the stored names
    # are the logical ones, so the entries follow. Rewritten as one plain dict
    # and validated once: per-entry model copies dominate on large profiles.
    payload = profile.to_dict(skip_defaults=False)
    payload["vertex_property_names"] = {}
    payload["vertex_indexes"] = {
        vertex_name: [
            {**idx, "fields": _renamed_fields(idx["fields"], vertex_maps[vertex_name])}
            for idx in indexes
        ]
        if vertex_name in vertex_maps
        else indexes
        for vertex_name, indexes in payload["vertex_indexes"].items()
    }
    for spec in payload["edge_specs"]:
        names = edge_maps.get((spec["source"], spec["target"], spec["relation"]))
        spec["property_names"] = {}
        if names:
            spec["indexes"] = [
                {**idx, "fields": _renamed_fields(idx["fields"], names)}
                for idx in spec["indexes"]
            ]
    dpv = payload.get("default_property_values")
    if dpv is not None:
        dpv["vertices"] = {
            vertex_name: {
                vertex_maps.get(vertex_name, {}).get(prop, prop): value
                for prop, value in values.items()
            }
            for vertex_name, values in dpv["vertices"].items()
        }
        for entry in dpv["edges"]:
            names = edge_maps.get((entry["source"], entry["target"], entry["relation"]))
            if names:
                entry["values"] = {
                    names.get(prop, prop): value
                    for prop, value in entry["values"].items()
                }

    view.db_profile = DatabaseProfile.model_validate(payload)
    view.finish_init()
    return view


def physical_schema(schema: Schema, db_flavor: DBType | None = None) -> Schema:
    """*schema* as *db_flavor* stores it: names filled in, then materialized.

    What DDL and any backend call that takes a whole schema should receive.
    Never mutates *schema*.
    """
    return materialize_physical_schema(with_physical_names(schema, db_flavor))
