"""Identity-plane evolution ops.

The identity policy of a vertex is the one thing every other reference hangs off:
upserts address it, edge endpoints resolve through it, and the physical profile indexes
it. Replacing it therefore cascades further than any other contract op, which is why it
lives in its own module rather than in :mod:`~graflo.architecture.evolution.apply`.
"""

from __future__ import annotations

import logging
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.vertex import (
    SECONDARY_IDENTITY_SUGAR,
    SecondaryIdentity,
    Vertex,
)

from .ops import (
    AddSecondaryIdentitiesOp,
    IdentityReplacement,
    IdentityTarget,
    RemoveSecondaryIdentitiesOp,
    ReplaceEdgeIdentitiesOp,
    ReplaceIdentityOp,
)

logger = logging.getLogger(__name__)

#: Name given to a demoted identity when the op does not supply ``retire_as``.
DEFAULT_RETIRED_IDENTITY_NAME = "retired_identity"

#: Synthetic identity field used by every non-natural identity mode.
SYNTHETIC_ID_FIELD = "id"


def _target_state(target: IdentityTarget) -> tuple[list[str], bool, bool, list[str]]:
    """``(identity, blank, assigned, hash_identity_properties)`` for *target*."""
    if target.mode == "natural":
        return list(target.identity), False, False, []
    if target.mode == "hash":
        return [SYNTHETIC_ID_FIELD], False, False, list(target.hash_from)
    if target.mode == "assigned":
        return [SYNTHETIC_ID_FIELD], False, True, []
    return [SYNTHETIC_ID_FIELD], True, False, []


def _is_noop(
    vertex: Vertex,
    *,
    mode: str,
    identity: list[str],
    hash_properties: list[str],
) -> bool:
    return (
        vertex.identity_mode == mode
        and list(vertex.identity) == identity
        and list(vertex.hash_identity_properties) == hash_properties
    )


def _require_properties_exist(vertex: Vertex, required: list[str]) -> None:
    """Refuse to key a vertex on properties it does not declare.

    ``Vertex.set_identity`` would silently synthesise them as ``type=None`` fields,
    which makes an empty column the primary key — a footgun worth an error.
    """
    if not required:
        return
    declared = {field.name for field in vertex.properties}
    missing = [name for name in required if name not in declared]
    if missing:
        raise ValueError(
            f"replace_identity: vertex '{vertex.name}' does not declare "
            f"{missing}; add them with AddVertexPropertiesOp before making them "
            "the identity"
        )


def _resolve_retire_policy(
    vertex: Vertex,
    spec: IdentityReplacement,
    *,
    old_identity: list[str],
    new_identity: list[str],
    new_is_blank: bool,
) -> str:
    """Downgrade ``demote`` where demotion is meaningless or forbidden."""
    if spec.retire != "demote":
        return spec.retire

    if new_is_blank:
        raise ValueError(
            f"replace_identity: vertex '{vertex.name}' cannot demote its old identity "
            "when the new mode is blank — a blank vertex has no source-visible key and "
            "cannot declare secondary_identities. Use retire: keep or retire: drop"
        )
    if vertex.identity_mode != "natural":
        logger.warning(
            "replace_identity: vertex %r had a synthetic %s identity; demoting %s "
            "would create a lookup key no source carries. Retiring as 'keep' instead.",
            vertex.name,
            vertex.identity_mode,
            old_identity,
        )
        return "keep"
    if frozenset(old_identity) == frozenset(new_identity):
        logger.warning(
            "replace_identity: vertex %r keeps the same identity field-set %s; "
            "demotion skipped (it would restate the primary identity).",
            vertex.name,
            old_identity,
        )
        return "keep"
    return "demote"


def _demoted_secondary_identities(
    vertex: Vertex, spec: IdentityReplacement, old_identity: list[str]
) -> tuple[list[SecondaryIdentity], str]:
    """Existing secondary identities plus the retired one; returns its resolved name."""
    old_field_set = frozenset(old_identity)
    requested = spec.retire_as or DEFAULT_RETIRED_IDENTITY_NAME

    for entry in vertex.secondary_identities:
        if entry.field_set == old_field_set:
            # Already declared as a lookup key — reuse it rather than duplicating.
            return list(vertex.secondary_identities), entry.name or requested
        if entry.name == requested:
            raise ValueError(
                f"replace_identity: vertex '{vertex.name}' already declares a "
                f"secondary identity named '{requested}' with fields "
                f"{entry.fields}; pass a different retire_as"
            )

    retired = SecondaryIdentity(name=requested, fields=list(old_identity))
    return [*vertex.secondary_identities, retired], requested


def _drop_promoted_secondary_identities(
    vertex_name: str,
    secondary_identities: list[SecondaryIdentity],
    new_identity: list[str],
) -> list[SecondaryIdentity]:
    """Drop lookup keys that the replacement promotes to the primary identity.

    ``Vertex._validated_secondary_identities`` rejects a secondary identity that
    restates the primary, so a set which *becomes* the new identity has to go. It is
    not a loss: the primary identity is already the strongest lookup path.
    """
    new_field_set = frozenset(new_identity)
    kept: list[SecondaryIdentity] = []
    for entry in secondary_identities:
        if entry.field_set == new_field_set:
            logger.debug(
                "replace_identity: vertex %r secondary identity %r is now the primary "
                "identity; dropping the redundant declaration",
                vertex_name,
                entry.name,
            )
            continue
        kept.append(entry)
    return kept


def _replaced_vertex(
    vertex: Vertex,
    *,
    identity: list[str],
    blank: bool,
    assigned: bool,
    hash_properties: list[str],
    secondary_identities: list[SecondaryIdentity],
) -> Vertex:
    """Rebuild *vertex* with a new identity policy.

    Built through ``model_validate`` rather than field assignment: the identity flags
    are mutually constrained, so a piecewise assignment can trip
    ``Vertex.set_identity`` on an intermediate state that the caller never asked for.
    """
    payload: dict[str, Any] = vertex.to_dict(skip_defaults=False)
    payload["identity"] = identity
    payload["blank"] = blank
    payload["assigned"] = assigned
    payload["hash_identity_properties"] = hash_properties
    payload["secondary_identities"] = [
        entry.to_dict(skip_defaults=False) for entry in secondary_identities
    ]
    return Vertex.model_validate(payload)


def _drop_old_identity_indexes(
    manifest: GraphManifest, vertex_name: str, old_identity: list[str]
) -> None:
    """Drop profile indexes that encoded the retired identity's uniqueness."""
    schema = manifest.graph_schema
    if schema is None:
        return
    indexes = schema.db_profile.vertex_indexes.get(vertex_name)
    if not indexes:
        return
    old_field_set = frozenset(old_identity)
    schema.db_profile.vertex_indexes[vertex_name] = [
        index for index in indexes if frozenset(index.fields) != old_field_set
    ]


def apply_replace_identity(manifest: GraphManifest, op: ReplaceIdentityOp) -> None:
    """Replace vertex identity policies and cascade the consequences.

    Mutates *manifest* in place:

    - Rewrites ``identity`` and the identity-mode flags on each named vertex.
    - Applies the retire policy: demote the old field-set to a secondary identity,
      keep it as a plain property, or drop it.
    - Drops ``db_profile`` vertex indexes that encoded the retired identity.
    - With ``endpoints: pin_to_retired``, repoints edge steps that matched the vertex
      on its primary identity at the demoted secondary identity.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("replace_identity requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.vertices) - vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"replace_identity: unknown vertices: {unknown}")

    replacements: dict[str, Vertex] = {}
    dropped_fields: dict[str, list[str]] = {}
    pinned_selectors: dict[str, str] = {}
    retired_indexes: dict[str, list[str]] = {}

    for vertex in vertex_config.vertices:
        spec = op.vertices.get(vertex.name)
        if spec is None:
            continue

        identity, blank, assigned, hash_properties = _target_state(spec.to)
        if _is_noop(
            vertex,
            mode=spec.to.mode,
            identity=identity,
            hash_properties=hash_properties,
        ):
            logger.debug(
                "replace_identity: vertex %r already has the requested identity; "
                "skipping",
                vertex.name,
            )
            continue

        _require_properties_exist(
            vertex, identity if spec.to.mode == "natural" else hash_properties
        )

        old_identity = list(vertex.identity)
        retire = _resolve_retire_policy(
            vertex,
            spec,
            old_identity=old_identity,
            new_identity=identity,
            new_is_blank=blank,
        )

        secondary_identities = list(vertex.secondary_identities)
        if retire == "demote":
            secondary_identities, retired_name = _demoted_secondary_identities(
                vertex, spec, old_identity
            )
            if spec.endpoints == "pin_to_retired":
                pinned_selectors[vertex.name] = retired_name
        elif retire == "drop":
            preserved = (
                set(identity)
                | set(hash_properties)
                | {name for entry in secondary_identities for name in entry.fields}
            )
            to_drop = [name for name in old_identity if name not in preserved]
            if to_drop:
                dropped_fields[vertex.name] = to_drop
        elif spec.endpoints == "pin_to_retired":
            # Unreachable via the op model (validated there); guard the direct-call path.
            raise ValueError(
                f"replace_identity: vertex '{vertex.name}' cannot pin endpoints to a "
                "retired identity because demotion did not happen"
            )

        if blank:
            secondary_identities = []
        else:
            secondary_identities = _drop_promoted_secondary_identities(
                vertex.name, secondary_identities, identity
            )

        replacements[vertex.name] = _replaced_vertex(
            vertex,
            identity=identity,
            blank=blank,
            assigned=assigned,
            hash_properties=hash_properties,
            secondary_identities=secondary_identities,
        )
        retired_indexes[vertex.name] = old_identity

    if not replacements:
        return

    vertex_config.vertices = [
        replacements.get(vertex.name, vertex) for vertex in vertex_config.vertices
    ]

    for vertex_name, old_identity in retired_indexes.items():
        _drop_old_identity_indexes(manifest, vertex_name, old_identity)

    schema.finish_init()

    if pinned_selectors:
        _pin_endpoints(manifest, pinned_selectors)

    if dropped_fields:
        _drop_retired_properties(manifest, dropped_fields)


def _selectors_in_use(manifest: GraphManifest) -> list[tuple[str, str | list[str]]]:
    """Every ``(vertex, selector)`` an edge step depends on across all resources."""
    from .rewrite import collect_endpoint_selectors

    if manifest.ingestion_model is None:
        return []
    out: list[tuple[str, str | list[str]]] = []
    for resource in manifest.ingestion_model.resources:
        out.extend(collect_endpoint_selectors(resource.pipeline))
    return out


def _matches_selector(
    entry: SecondaryIdentity, selector: str | list[str], declared: int
) -> bool:
    """Whether *selector* on an edge step resolves to *entry*.

    Mirrors :meth:`Vertex.secondary_identity`: by name, by field-set in any order, or
    via the bare ``secondary`` shorthand when the vertex declares exactly one.
    """
    if isinstance(selector, list):
        return frozenset(selector) == entry.field_set
    if selector == SECONDARY_IDENTITY_SUGAR:
        return declared == 1
    return selector == entry.name


def apply_add_secondary_identities(
    manifest: GraphManifest, op: AddSecondaryIdentitiesOp
) -> None:
    """Declare alternate lookup keys on existing vertices.

    The derived non-unique index for each new field-set is registered by
    ``Schema.finish_init``; this op does not author indexes directly.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("add_secondary_identities requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.additions) - vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"add_secondary_identities: unknown vertices: {unknown}")

    replacements: dict[str, Vertex] = {}
    for vertex in vertex_config.vertices:
        additions = op.additions.get(vertex.name)
        if not additions:
            continue
        if vertex.blank:
            raise ValueError(
                f"add_secondary_identities: vertex '{vertex.name}' is blank and has no "
                "source-visible key to match on; secondary identities are not allowed"
            )

        declared = {field.name for field in vertex.properties}
        combined = list(vertex.secondary_identities)
        known_field_sets = {entry.field_set for entry in combined}
        known_names = {entry.name for entry in combined if entry.name}

        for entry in additions:
            missing = [name for name in entry.fields if name not in declared]
            if missing:
                raise ValueError(
                    f"add_secondary_identities: vertex '{vertex.name}' does not "
                    f"declare {missing}; add them with AddVertexPropertiesOp first"
                )
            if entry.field_set in known_field_sets:
                raise ValueError(
                    f"add_secondary_identities: vertex '{vertex.name}' already "
                    f"declares a secondary identity on {entry.fields}"
                )
            if entry.name is not None and entry.name in known_names:
                raise ValueError(
                    f"add_secondary_identities: vertex '{vertex.name}' already "
                    f"declares a secondary identity named '{entry.name}'"
                )
            known_field_sets.add(entry.field_set)
            if entry.name is not None:
                known_names.add(entry.name)
            combined.append(entry)

        replacements[vertex.name] = _replaced_vertex(
            vertex,
            identity=list(vertex.identity),
            blank=vertex.blank,
            assigned=vertex.assigned,
            hash_properties=list(vertex.hash_identity_properties),
            secondary_identities=combined,
        )

    if not replacements:
        return

    vertex_config.vertices = [
        replacements.get(vertex.name, vertex) for vertex in vertex_config.vertices
    ]
    schema.finish_init()


def apply_remove_secondary_identities(
    manifest: GraphManifest, op: RemoveSecondaryIdentitiesOp
) -> None:
    """Withdraw alternate lookup keys and drop the indexes derived from them."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("remove_secondary_identities requires graph_schema")

    vertex_config = schema.core_schema.vertex_config
    unknown = sorted(set(op.removals) - vertex_config.vertex_set)
    if unknown:
        raise ValueError(f"remove_secondary_identities: unknown vertices: {unknown}")

    in_use = _selectors_in_use(manifest)
    replacements: dict[str, Vertex] = {}
    removed_field_sets: dict[str, list[frozenset[str]]] = {}

    for vertex in vertex_config.vertices:
        selectors = op.removals.get(vertex.name)
        if not selectors:
            continue

        declared_count = len(vertex.secondary_identities)
        doomed: list[SecondaryIdentity] = []
        for selector in selectors:
            matched = [
                entry
                for entry in vertex.secondary_identities
                if _matches_selector(entry, selector, declared_count)
            ]
            if not matched:
                raise ValueError(
                    f"remove_secondary_identities: vertex '{vertex.name}' has no "
                    f"secondary identity matching {selector!r}; declared: "
                    f"{vertex.secondary_identity_names}"
                )
            doomed.extend(matched)

        doomed_field_sets = {entry.field_set for entry in doomed}
        for used_vertex, used_selector in in_use:
            if used_vertex != vertex.name:
                continue
            if any(
                _matches_selector(entry, used_selector, declared_count)
                for entry in doomed
            ):
                raise ValueError(
                    f"remove_secondary_identities: vertex '{vertex.name}' secondary "
                    f"identity {used_selector!r} is still selected by an edge step; "
                    "repoint that endpoint before removing the lookup key"
                )

        replacements[vertex.name] = _replaced_vertex(
            vertex,
            identity=list(vertex.identity),
            blank=vertex.blank,
            assigned=vertex.assigned,
            hash_properties=list(vertex.hash_identity_properties),
            secondary_identities=[
                entry
                for entry in vertex.secondary_identities
                if entry.field_set not in doomed_field_sets
            ],
        )
        removed_field_sets[vertex.name] = list(doomed_field_sets)

    if not replacements:
        return

    vertex_config.vertices = [
        replacements.get(vertex.name, vertex) for vertex in vertex_config.vertices
    ]

    # Derived indexes are only ever added by finish_init, never withdrawn.
    for vertex_name, field_sets in removed_field_sets.items():
        indexes = schema.db_profile.vertex_indexes.get(vertex_name)
        if not indexes:
            continue
        schema.db_profile.vertex_indexes[vertex_name] = [
            index for index in indexes if frozenset(index.fields) not in field_sets
        ]

    schema.finish_init()


def apply_replace_edge_identities(
    manifest: GraphManifest, op: ReplaceEdgeIdentitiesOp
) -> None:
    """Replace the uniqueness keys of logical edges."""
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("replace_edge_identities requires graph_schema")

    edges = schema.core_schema.edge_config.edges
    by_edge_id = {edge.edge_id: edge for edge in edges}

    unknown = sorted(
        str(entry.edge_id()) for entry in op.edges if entry.edge_id() not in by_edge_id
    )
    if unknown:
        raise ValueError(f"replace_edge_identities: unknown edges: {unknown}")

    for entry in op.edges:
        edge = by_edge_id[entry.edge_id()]
        for key in entry.identities:
            if not key:
                raise ValueError(
                    f"replace_edge_identities: edge {entry.edge_id()} declares an "
                    "empty uniqueness key"
                )
        edge.identities = [list(key) for key in entry.identities]
        logger.debug(
            "replace_edge_identities: edge %s identities -> %s",
            entry.edge_id(),
            edge.identities,
        )

    schema.finish_init()


def _pin_endpoints(manifest: GraphManifest, selectors: dict[str, str]) -> None:
    from .apply import _rebuild_ingestion_with_pipeline_rewrite
    from .rewrite import rewrite_endpoint_selectors_in_pipeline

    if manifest.ingestion_model is None:
        return
    _rebuild_ingestion_with_pipeline_rewrite(
        manifest,
        lambda pipeline: rewrite_endpoint_selectors_in_pipeline(pipeline, selectors),
    )


def _drop_retired_properties(
    manifest: GraphManifest, dropped: dict[str, list[str]]
) -> None:
    """Remove retired identity fields, reusing the property-removal cascade."""
    from .apply import apply_remove_vertex_properties
    from .ops import RemoveVertexPropertiesOp

    apply_remove_vertex_properties(manifest, RemoveVertexPropertiesOp(removals=dropped))
