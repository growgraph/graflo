"""Binary compose of two :class:`~graflo.architecture.contract.manifest.GraphManifest`s."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from itertools import pairwise
from typing import Any, Literal

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel
from graflo.architecture.schema.naming import canonical_slug
from graflo.architecture.schema.vertex import SecondaryIdentity, Vertex, VertexConfig

from .apply import (
    _bump_schema_version,
    _revalidate_db_profile,
    apply_manifest_ops_inplace,
    apply_rename_relations,
    apply_rename_resources,
    apply_rename_vertices,
)
from .canonical import (
    CanonicalMap,
    SideMaps,
    canonical_map_to_ops,
    validate_and_complete_canonical_map,
)
from .equivalence import Cluster, ClusterIndex, Side, index_clusters
from .merge_core import (
    edge_config_from_edges,
    merge_edge_pair,
    merge_vertex_models,
)
from .ops import (
    AddSecondaryIdentitiesOp,
    ComposeManifestsOp,
    IdentityBranchSpec,
    ManifestOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVerticesOp,
    SideIdentity,
)

_RIGHT_PREFIX = "r_"


def _prefixed(name: str) -> str:
    if name.startswith(_RIGHT_PREFIX):
        return name
    return f"{_RIGHT_PREFIX}{name}"


def _resolve_name_collisions(
    occupied: set[str],
    candidates: list[str],
    *,
    name_conflict: Literal["error", "prefix_right", "fuse_right"],
    kind: str,
) -> dict[str, str]:
    """Return rename map for *candidates* that collide with *occupied*.

    Exact matching only, and deliberately so: resources and connectors are
    *addresses*, not concepts. Two whose names key alike cause no split -- each
    keeps its own name, each is looked up by that name, each targets its own
    vertex -- so canonical matching here would be pure false positive.
    ``fuse_right`` is meaningless for the same reason and behaves as ``error``.
    """
    renames: dict[str, str] = {}
    taken = set(occupied)
    for name in candidates:
        if name not in taken:
            taken.add(name)
            continue
        if name_conflict in ("error", "fuse_right"):
            raise ValueError(
                f"compose_manifests: {kind} name collision on {name!r}; "
                "provide an equivalence / resource_renames, or set "
                "name_conflict='prefix_right'"
            )
        new_name = _prefixed(name)
        while new_name in taken:
            new_name = _prefixed(new_name)
        renames[name] = new_name
        taken.add(new_name)
    return renames


def _require_schema(manifest: GraphManifest, side: str) -> Schema:
    if manifest.graph_schema is None:
        raise ValueError(
            f"compose_manifests requires graph_schema on the {side} manifest"
        )
    return manifest.graph_schema


def _relation_names(schema: Schema) -> set[str]:
    return {
        edge.relation
        for edge in schema.core_schema.edge_config.edges
        if edge.relation is not None
    }


def _apply_right_resource_policy(
    right: GraphManifest,
    op: ComposeManifestsOp,
    left_resource_names: set[str],
) -> None:
    if op.resource_renames:
        apply_rename_resources(
            right, RenameResourcesOp(resources=dict(op.resource_renames))
        )

    if right.ingestion_model is None:
        return

    right_names = [r.name for r in right.ingestion_model.resources]
    collisions = _resolve_name_collisions(
        left_resource_names,
        right_names,
        name_conflict=op.name_conflict,
        kind="resource",
    )
    if collisions:
        apply_rename_resources(right, RenameResourcesOp(resources=collisions))


class ComposeNameConflictError(ValueError):
    """Two names denote one concept under different naming conventions.

    Distinct from ``ComposeCanonicalConflictError`` in ``canonical.py``, which
    reports a *declared* CanonicalMap contradicting the op. This one fires on
    the residue neither side declared -- the undeclared path, where compose
    would otherwise produce two unrelated types with the data split between
    them and nothing raising.
    """


class ComposeIdentityError(ValueError):
    """A composed vertex's identity is ambiguous and nothing resolves it.

    Two or more cluster members disagree on their (canonical-name) identity
    field-set and none of the three ways to resolve it were declared: an
    explicit ``identity`` on the ``VertexEquivalence``, a
    ``PropertyEquivalence(identity=True)`` flag, or an ``identity_alignments``
    entry for the composed class. The alternative -- silently taking the
    union of both field-sets as the new identity -- produces a natural key no
    record fully carries.
    """


def _canonical_near_collisions(
    left_names: Iterable[str],
    right_names: Iterable[str],
    *,
    exempt: frozenset[str],
) -> list[tuple[str, str]]:
    """``(left, right)`` pairs that key alike but are spelled differently.

    Exact matches are excluded: those are the pre-existing collision path, so
    the two checks can never report the same pair twice.
    """
    by_key: dict[str, list[str]] = {}
    for name in left_names:
        by_key.setdefault(canonical_slug(name), []).append(name)

    pairs: list[tuple[str, str]] = []
    for right_name in right_names:
        if right_name in exempt:
            continue
        for left_name in by_key.get(canonical_slug(right_name), []):
            if left_name != right_name:
                pairs.append((left_name, right_name))
    return sorted(set(pairs))


def _resolve_schema_collisions(
    *,
    left_names: set[str],
    right_names: list[str],
    exempt: frozenset[str],
    name_conflict: str,
    kind: str,
    equivalence_hint: str,
) -> dict[str, str]:
    """The rename map to apply to the right side, or raise under ``error``.

    Two kinds of collision are handled together because they are the same
    question asked with different confidence. An **exact** collision
    (``Customer`` on both sides) has always raised by default. A **canonical**
    one (``Customer`` / ``customer``, or ``OrderLine`` / ``order_line``) is the
    weaker signal, so it cannot trigger a *more* aggressive action than the
    stronger one -- if exact equality refuses to fuse silently, a naming-
    convention guess certainly must not.

    Left the right names alone and they compose into two unrelated types with
    the source data split between them and nothing raising, which is the whole
    of ``CORE-MERGE-001``.
    """
    exact = [name for name in right_names if name in left_names and name not in exempt]
    near = _canonical_near_collisions(left_names, right_names, exempt=exempt)

    if name_conflict == "error":
        if exact:
            raise ValueError(
                f"compose_manifests: {kind} name collision on "
                f"{sorted(exact)!r}; provide a {equivalence_hint} or set "
                "name_conflict='prefix_right'"
            )
        if near:
            pairs = ", ".join(f"{left!r} / {right!r}" for left, right in near)
            raise ComposeNameConflictError(
                f"compose_manifests: {pairs} denote the same concept under "
                f"different naming conventions, so they would compose into two "
                f"unrelated {kind} types with the source data split between "
                f"them. Declare a {equivalence_hint} (or a CanonicalMap) "
                "to combine them, set name_conflict='fuse_right' to adopt the "
                "left spelling, or name_conflict='prefix_right' to keep them "
                "apart."
            )
        return {}

    if name_conflict == "fuse_right":
        if exact:
            raise ValueError(
                f"compose_manifests: {kind} name collision on "
                f"{sorted(exact)!r}; provide a {equivalence_hint} or set "
                "name_conflict='prefix_right'"
            )
        # Adopt the left spelling; the ordinary same-name union then merges
        # them. The left side keeps its name because it is the side the
        # canonical vocabulary is expected to live on.
        return {right: left for left, right in near}

    # prefix_right: keep both, explicitly, under distinguishable names.
    renames: dict[str, str] = {}
    taken = set(left_names) | set(right_names)
    for name in [*exact, *(right for _left, right in near)]:
        if name in renames:
            continue
        new_name = _prefixed(name)
        while new_name in taken:
            new_name = _prefixed(new_name)
        renames[name] = new_name
        taken.add(new_name)
    return renames


def _did_you_mean(name: str, candidates: Iterable[str]) -> str:
    """A suffix naming a candidate that denotes the same concept, if any.

    Authoring an equivalence in the wrong convention is the likeliest mistake
    at this boundary, and "not in left manifest" alone is a dead end when the
    vertex is right there under another spelling.
    """
    key = canonical_slug(name)
    near = sorted(c for c in candidates if c != name and canonical_slug(c) == key)
    if not near:
        return ""
    return (
        f"; it has {near[0]!r}, which denotes the same concept — author the "
        "equivalence in the manifest's own spelling"
    )


def _assert_no_canonical_split(schema: Schema) -> None:
    """No two composed vertex types or relations may denote one concept.

    The invariant ``CORE-MERGE-001`` is actually about, asserted on the result
    rather than only at the sites that could violate it -- so a future path
    into the union is covered without anyone remembering to add a check.
    """
    for kind, names in (
        ("vertex", [v.name for v in schema.core_schema.vertex_config.vertices]),
        (
            "relation",
            [
                e.relation
                for e in schema.core_schema.edge_config.edges
                if e.relation is not None
            ],
        ),
    ):
        by_key: dict[str, set[str]] = {}
        for name in names:
            by_key.setdefault(canonical_slug(name), set()).add(name)
        split = {key: sorted(group) for key, group in by_key.items() if len(group) > 1}
        if split:
            raise ComposeNameConflictError(
                f"compose_manifests produced {kind} types that denote the same "
                f"concept under different spellings: {split}. This is an "
                "unhandled compose path, not an authoring error -- the result "
                "would split data between them silently."
            )


def _apply_right_schema_collision_policy(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
) -> None:
    """Prefix, fuse or error on non-equivalent right vertex/relation names.

    Names are compared both exactly and by :func:`canonical_slug`, so two
    spellings of one concept are a collision rather than two types.

    Deliberately **not** applied to properties, resources, connectors or
    transforms. A property name binds to a key in the source document, so
    fusing ``customer_email`` with ``customerEmail`` would fuse two columns fed
    by different keys; the rest are addresses looked up by exact name, where a
    near-collision splits nothing and the check would be pure false positive.
    """
    left_schema = _require_schema(left, "left")
    right_schema = _require_schema(right, "right")

    v_renames = _resolve_schema_collisions(
        left_names=set(left_schema.core_schema.vertex_config.vertex_set),
        right_names=sorted(right_schema.core_schema.vertex_config.vertex_set),
        exempt=frozenset({veq.into for veq in op.vertices}),
        name_conflict=op.name_conflict,
        kind="vertex",
        equivalence_hint="VertexEquivalence",
    )
    if v_renames:
        apply_rename_vertices(right, RenameVerticesOp(vertices=v_renames))

    r_renames = _resolve_schema_collisions(
        left_names={
            e.relation
            for e in left_schema.core_schema.edge_config.edges
            if e.relation is not None
        },
        right_names=sorted(
            {
                e.relation
                for e in right_schema.core_schema.edge_config.edges
                if e.relation is not None
            }
        ),
        exempt=frozenset({req.into for req in op.relations}),
        name_conflict=op.name_conflict,
        kind="relation",
        equivalence_hint="RelationEquivalence",
    )
    if r_renames:
        apply_rename_relations(right, RenameRelationsOp(relations=r_renames))


def _member_state(
    schema: Schema,
    cluster: Cluster,
    side: Side,
    property_rename: dict[str, dict[str, str]],
) -> tuple[
    dict[tuple[Side, str], tuple[str, ...] | None],
    dict[tuple[Side, str], set[str]],
]:
    """Per-member canonical-name identity key (or ``None`` if not a plain
    natural key) and canonical-name property set, captured **before** the
    per-side property-rename / merge ops run.

    A plain natural key is the only identity mode this module has to
    reconcile across members on its own: ``blank`` / ``assigned`` /
    ``hash_identity_properties`` / ``identity_funnel`` disagreement is already
    handled — raised on, or carried through when consistent — by
    :func:`~graflo.architecture.evolution.merge_core.merge_vertex_models`.
    """
    vertex_config = schema.core_schema.vertex_config
    keys: dict[tuple[Side, str], tuple[str, ...] | None] = {}
    names: dict[tuple[Side, str], set[str]] = {}
    for member in cluster.members(side):
        vertex = vertex_config[member]
        rename = property_rename.get(member, {})
        names[(side, member)] = {rename.get(f, f) for f in vertex.property_names}
        if (
            vertex.blank
            or vertex.assigned
            or vertex.hash_identity_properties
            or vertex.identity_funnel is not None
        ):
            keys[(side, member)] = None
        else:
            keys[(side, member)] = tuple(
                sorted(rename.get(f, f) for f in vertex.identity)
            )
    return keys, names


def _capture_all_member_state(
    index: ClusterIndex, left_schema: Schema, right_schema: Schema, side_maps: SideMaps
) -> tuple[
    dict[tuple[Side, str], tuple[str, ...] | None],
    dict[tuple[Side, str], set[str]],
]:
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None] = {}
    member_property_names: dict[tuple[Side, str], set[str]] = {}
    for cluster in index.vertices:
        for side, schema in (("left", left_schema), ("right", right_schema)):
            k, n = _member_state(schema, cluster, side, side_maps[side].properties)
            member_keys.update(k)
            member_property_names.update(n)
    return member_keys, member_property_names


def _branch_tuple(spec: IdentityBranchSpec) -> tuple[str, ...]:
    return (spec,) if isinstance(spec, str) else tuple(spec)


def side_identity_to_funnel(
    cluster: Cluster,
    member_property_names: dict[tuple[Side, str], set[str]],
) -> IdentityFunnel:
    """Lower a :class:`~graflo.architecture.evolution.ops.SideIdentity` shorthand.

    Each member supplies an ordered branch chain (its own override, or the
    side default); the chains are merged into one global branch order by
    topological sort over the "comes before" relation each chain implies,
    breaking ties by first appearance across members (in declaration order:
    every left member, then every right member). Two members that disagree on
    the relative order of two branches have no consistent global order and
    raise :class:`ComposeIdentityError`.
    """
    side_identity = cluster.declaration.identity
    assert isinstance(side_identity, SideIdentity)

    chains: dict[tuple[Side, str], list[tuple[str, ...]]] = {}
    for side in ("left", "right"):
        default = side_identity.left if side == "left" else side_identity.right
        for member in cluster.members(side):
            raw = side_identity.members.get(member, default)
            if raw is None:
                raise ComposeIdentityError(
                    f"compose_manifests: cluster into {cluster.into!r} has no "
                    f"SideIdentity branch chain for {side}:{member} (no "
                    f"per-member override and no {side} default)"
                )
            chains[(side, member)] = [_branch_tuple(spec) for spec in raw]

    ordered_members: list[tuple[Side, str]] = [("left", m) for m in cluster.left] + [
        ("right", m) for m in cluster.right
    ]
    first_seen: dict[tuple[str, ...], int] = {}
    for key in ordered_members:
        for branch in chains[key]:
            first_seen.setdefault(branch, len(first_seen))

    indegree: dict[tuple[str, ...], int] = dict.fromkeys(first_seen, 0)
    successors: dict[tuple[str, ...], set[tuple[str, ...]]] = {
        b: set() for b in first_seen
    }
    for key in ordered_members:
        chain = chains[key]
        for a, b in pairwise(chain):
            if b not in successors[a]:
                successors[a].add(b)
                indegree[b] += 1

    remaining = dict(indegree)
    available = sorted(
        (b for b, deg in remaining.items() if deg == 0), key=first_seen.__getitem__
    )
    order: list[tuple[str, ...]] = []
    while available:
        node = available.pop(0)
        order.append(node)
        for succ in successors[node]:
            remaining[succ] -= 1
            if remaining[succ] == 0:
                available.append(succ)
        available.sort(key=first_seen.__getitem__)

    if len(order) != len(first_seen):
        raise ComposeIdentityError(
            f"compose_manifests: cluster into {cluster.into!r} has an "
            "inconsistent SideIdentity branch order — two members disagree "
            "on the relative order of two branches, so no single global "
            "funnel order satisfies both"
        )

    for (side, member), chain in chains.items():
        declared = member_property_names.get((side, member), set())
        for branch in chain:
            missing = sorted(set(branch) - declared)
            if missing:
                raise ComposeIdentityError(
                    f"compose_manifests: cluster into {cluster.into!r} "
                    f"SideIdentity branch {branch} on {side}:{member} "
                    f"references undeclared propert"
                    f"{'y' if len(missing) == 1 else 'ies'} {missing}"
                )

    return IdentityFunnel(
        branches=[
            IdentityBranch(id="_".join(fields), fields=list(fields)) for fields in order
        ]
    )


def _composed_identity(
    cluster: Cluster,
    merged: Vertex,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
    member_property_names: dict[tuple[Side, str], set[str]],
    *,
    has_alignment: bool,
) -> tuple[list[str] | IdentityFunnel, bool]:
    """The composed vertex's identity, and whether it was explicitly declared.

    Undeclared and every plain-natural-key member agrees (or fewer than two
    are plain): carries the merged composite through, same as an ordinary
    :func:`~graflo.architecture.evolution.merge_core.merge_vertex_models`
    merge. Undeclared, members disagree, and nothing resolves it (no
    ``identity``, no ``PropertyEquivalence.identity`` flag, no
    ``identity_alignments`` entry for this class): raises
    :class:`ComposeIdentityError` rather than silently keying on the union of
    both field-sets.
    """
    declared = cluster.declaration.identity
    if declared is not None:
        if isinstance(declared, IdentityFunnel):
            return declared, True
        if isinstance(declared, SideIdentity):
            return side_identity_to_funnel(cluster, member_property_names), True
        return list(declared), True

    identity_out = list(merged.identity)
    seen = set(identity_out)
    flagged = False
    for pe in cluster.declaration.properties:
        if pe.identity and pe.into not in seen:
            identity_out.append(pe.into)
            seen.add(pe.into)
            flagged = True

    if flagged or has_alignment:
        return identity_out, False

    plain_members: list[tuple[Side, str, tuple[str, ...]]] = [
        (side, member, fields)
        for side in ("left", "right")
        for member in cluster.members(side)
        if (fields := member_keys.get((side, member))) is not None
    ]
    plain_keys = {fields for _side, _member, fields in plain_members}
    if len(plain_keys) > 1:
        detail = "; ".join(
            f"{side}:{member}={list(fields)}" for side, member, fields in plain_members
        )
        raise ComposeIdentityError(
            f"compose_manifests: composed vertex {cluster.into!r} has members "
            f"that disagree on identity ({detail}) and nothing resolves it. "
            "Declare `identity` on the VertexEquivalence, flag a "
            "PropertyEquivalence(identity=True), or add an "
            "identity_alignments entry for this class."
        )
    return identity_out, False


def _apply_composed_identity(
    merged: Vertex, identity: list[str] | IdentityFunnel, *, declared: bool
) -> Vertex:
    if not declared:
        return merged.model_copy(update={"identity": identity})
    if isinstance(identity, IdentityFunnel):
        # A funnel-mode vertex carries the synthetic key under "id" -- the
        # same convention `apply_replace_identity` uses for a FunnelIdentityTarget.
        return merged.model_copy(
            update={
                "identity": ["id"],
                "identity_funnel": identity,
                "hash_identity_properties": [],
                "blank": False,
                "assigned": False,
            }
        )
    return merged.model_copy(
        update={
            "identity": identity,
            "identity_funnel": None,
            "hash_identity_properties": [],
            "blank": False,
            "assigned": False,
        }
    )


def _cluster_retire_ops(
    cluster: Cluster,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
    primary_fields: frozenset[str],
) -> list[ManifestOp]:
    """Demote each member's pre-merge plain identity key to a lookup-only secondary.

    Only meaningful when the cluster declared an ``identity`` (nothing to
    retire otherwise) and ``retire == "demote"`` (the default). A member key
    that happens to equal the new composed identity's field-set is skipped --
    demoting it would restate the primary as a secondary, which
    ``Vertex.set_identity`` rejects outright.
    """
    if cluster.declaration.identity is None or cluster.declaration.retire != "demote":
        return []
    secondary: dict[tuple[str, ...], SecondaryIdentity] = {}
    for side in ("left", "right"):
        for member in cluster.members(side):
            fields = member_keys.get((side, member))
            if not fields or frozenset(fields) == primary_fields:
                continue
            secondary.setdefault(
                fields,
                SecondaryIdentity(name=f"by_{'_'.join(fields)}", fields=list(fields)),
            )
    if not secondary:
        return []
    return [
        AddSecondaryIdentitiesOp(additions={cluster.into: list(secondary.values())})
    ]


def _merge_db_profiles(
    left: DatabaseProfile, right: DatabaseProfile
) -> DatabaseProfile:
    data = left.to_dict(skip_defaults=False)
    right_data = right.to_dict(skip_defaults=False)

    vs = dict(data.get("vertex_storage_names") or {})
    for k, v in (right_data.get("vertex_storage_names") or {}).items():
        if k in vs and vs[k] != v:
            raise ValueError(
                f"compose_manifests: conflicting vertex_storage_names for {k!r}: "
                f"{vs[k]!r} vs {v!r}"
            )
        vs[k] = v
    data["vertex_storage_names"] = vs

    vi = {k: list(v) for k, v in (data.get("vertex_indexes") or {}).items()}
    for k, vlist in (right_data.get("vertex_indexes") or {}).items():
        vi.setdefault(k, []).extend(list(vlist))
    data["vertex_indexes"] = vi

    edge_specs = list(data.get("edge_specs") or [])
    edge_specs.extend(list(right_data.get("edge_specs") or []))
    data["edge_specs"] = edge_specs

    return _revalidate_db_profile(DatabaseProfile.model_validate(data))


def _union_schema(
    left: Schema,
    right: Schema,
    index: ClusterIndex,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
    member_property_names: dict[tuple[Side, str], set[str]],
    alignment_labels: set[str],
) -> tuple[Schema, list[ManifestOp]]:
    left_vc = left.core_schema.vertex_config
    right_vc = right.core_schema.vertex_config
    left_by_name = {v.name: v for v in left_vc.vertices}
    right_by_name = {v.name: v for v in right_vc.vertices}

    out_vertices: list[Vertex] = []
    seen: set[str] = set()
    retire_ops: list[ManifestOp] = []

    for cluster in index.vertices:
        name = cluster.into
        if name not in left_by_name or name not in right_by_name:
            missing_side = "left" if name not in left_by_name else "right"
            raise ValueError(
                f"compose_manifests: composed vertex {name!r} missing on "
                f"{missing_side} after alignment (left={list(cluster.left)!r}, "
                f"right={list(cluster.right)!r})"
            )
        merged = merge_vertex_models([left_by_name[name], right_by_name[name]], name)
        identity, declared = _composed_identity(
            cluster,
            merged,
            member_keys,
            member_property_names,
            has_alignment=name in alignment_labels,
        )
        merged = _apply_composed_identity(merged, identity, declared=declared)
        if declared:
            primary_fields = (
                frozenset(identity.field_names)
                if isinstance(identity, IdentityFunnel)
                else frozenset(identity)
            )
            retire_ops.extend(_cluster_retire_ops(cluster, member_keys, primary_fields))
        out_vertices.append(merged)
        seen.add(name)

    for v in left_vc.vertices:
        if v.name in seen:
            continue
        out_vertices.append(v)
        seen.add(v.name)

    for v in right_vc.vertices:
        if v.name in seen:
            continue
        out_vertices.append(v)
        seen.add(v.name)

    force_types: dict[str, list] = dict(left_vc.force_types or {})
    for k, v in (right_vc.force_types or {}).items():
        if k in force_types and force_types[k] != v:
            raise ValueError(
                f"compose_manifests: conflicting force_types for vertex {k!r}"
            )
        force_types[k] = v

    by_id: dict[EdgeId, Edge] = {}
    for edge in list(left.core_schema.edge_config.edges) + list(
        right.core_schema.edge_config.edges
    ):
        eid = edge.edge_id
        if eid in by_id:
            by_id[eid] = merge_edge_pair(by_id[eid], edge)
        else:
            by_id[eid] = edge

    meta = left.metadata.model_copy(deep=True)
    if right.metadata.name and meta.name and right.metadata.name != meta.name:
        meta.name = f"{meta.name}+{right.metadata.name}"
    elif right.metadata.name and not meta.name:
        meta.name = right.metadata.name

    db_profile = _merge_db_profiles(left.db_profile, right.db_profile)

    schema = Schema(
        metadata=meta,
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=out_vertices, force_types=force_types),
            edge_config=edge_config_from_edges(list(by_id.values())),
        ),
        db_profile=db_profile,
    )
    return schema, retire_ops


def _union_transforms(
    left: IngestionModel | None, right: IngestionModel | None
) -> list:
    left_t = list(left.transforms) if left is not None else []
    right_t = list(right.transforms) if right is not None else []
    by_name: dict[str, Any] = {}
    out: list = []
    for t in left_t + right_t:
        name = t.name
        if name is None:
            out.append(t)
            continue
        if name in by_name:
            existing = by_name[name]
            if existing.to_dict(skip_defaults=False) != t.to_dict(skip_defaults=False):
                raise ValueError(
                    f"compose_manifests: incompatible transform definitions for {name!r}"
                )
            continue
        by_name[name] = t
        out.append(t)
    return out


def _union_ingestion(
    left: IngestionModel | None, right: IngestionModel | None
) -> IngestionModel | None:
    if left is None and right is None:
        return None
    if left is None:
        return right.model_copy(deep=True) if right is not None else None
    if right is None:
        return left.model_copy(deep=True)

    resources = list(left.resources) + list(right.resources)
    transforms = _union_transforms(left, right)
    # Model-level write policies follow the left manifest, as composition treats
    # it as the base being extended.
    edges_on_duplicate = left.edges_on_duplicate
    endpoints_on_ambiguous = left.endpoints_on_ambiguous
    return IngestionModel.model_validate(
        {
            "edges_on_duplicate": edges_on_duplicate,
            "endpoints_on_ambiguous": endpoints_on_ambiguous,
            "resources": [r.to_dict(skip_defaults=False) for r in resources],
            "transforms": [t.to_dict(skip_defaults=False) for t in transforms],
        }
    )


def _connector_name(connector: Any) -> str | None:
    if isinstance(connector, dict):
        name = connector.get("name")
        return name if isinstance(name, str) else None
    name = getattr(connector, "name", None)
    return name if isinstance(name, str) else None


def _union_bindings(
    left: Bindings | None,
    right: Bindings | None,
    *,
    name_conflict: Literal["error", "prefix_right", "fuse_right"],
) -> Bindings | None:
    if left is None and right is None:
        return None
    if left is None:
        return right.model_copy(deep=True) if right is not None else None
    if right is None:
        return left.model_copy(deep=True)

    left_data = left.to_dict(skip_defaults=False)
    right_data = right.to_dict(skip_defaults=False)

    left_connectors = list(left_data.get("connectors") or [])
    right_connectors = list(right_data.get("connectors") or [])
    left_names = {n for c in left_connectors if (n := _connector_name(c)) is not None}
    rename_connectors: dict[str, str] = {}
    for connector in right_connectors:
        name = _connector_name(connector)
        if name is None or name not in left_names:
            if name is not None:
                left_names.add(name)
            continue
        if name_conflict == "error":
            raise ValueError(
                f"compose_manifests: connector name collision on {name!r}; "
                "rename before compose or set name_conflict='prefix_right'"
            )
        new_name = _prefixed(name)
        while new_name in left_names:
            new_name = _prefixed(new_name)
        rename_connectors[name] = new_name
        left_names.add(new_name)
        if isinstance(connector, dict):
            connector["name"] = new_name
        else:
            # Re-serialize path: mutate via dict rebuild below
            pass

    if rename_connectors:
        # Rebuild right connectors/bindings with renamed connector names.
        rebuilt_right: list[Any] = []
        for connector in right_connectors:
            d = (
                dict(connector)
                if isinstance(connector, dict)
                else connector.to_dict(skip_defaults=False)
            )
            cname = d.get("name")
            if isinstance(cname, str) and cname in rename_connectors:
                d["name"] = rename_connectors[cname]
            rebuilt_right.append(d)
        right_connectors = rebuilt_right

        def _remap_connector_ref(entries: list[Any], key: str) -> list[Any]:
            out: list[Any] = []
            for entry in entries:
                d = (
                    dict(entry)
                    if isinstance(entry, dict)
                    else (
                        entry.to_dict(skip_defaults=False)
                        if hasattr(entry, "to_dict")
                        else dict(entry)
                    )
                )
                ref = d.get(key)
                if isinstance(ref, str) and ref in rename_connectors:
                    d[key] = rename_connectors[ref]
                out.append(d)
            return out

        right_data["resource_connector"] = _remap_connector_ref(
            list(right_data.get("resource_connector") or []), "connector"
        )
        right_data["connector_connection"] = _remap_connector_ref(
            list(right_data.get("connector_connection") or []), "connector"
        )

    merged = {
        "connector_templates": list(left_data.get("connector_templates") or [])
        + list(right_data.get("connector_templates") or []),
        "conn_proxy": left_data.get("conn_proxy") or right_data.get("conn_proxy"),
        "connectors": left_connectors + right_connectors,
        "resource_connector": list(left_data.get("resource_connector") or [])
        + list(right_data.get("resource_connector") or []),
        "connector_connection": list(left_data.get("connector_connection") or [])
        + list(right_data.get("connector_connection") or []),
        "staging_proxy": list(left_data.get("staging_proxy") or [])
        + list(right_data.get("staging_proxy") or []),
    }
    return Bindings.model_validate(merged)


def _coerce_side_maps(
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
) -> list[tuple[Side, CanonicalMap]]:
    coerced: list[tuple[Side, CanonicalMap]] = []
    for entry in canonical_maps:
        if (
            isinstance(entry, tuple)
            and len(entry) == 2
            and entry[0] in ("left", "right")
            and isinstance(entry[1], CanonicalMap)
        ):
            coerced.append(entry)
            continue
        raise TypeError(
            "compose_manifests: canonical_maps entries must be (side, "
            f"CanonicalMap) pairs with side in ('left', 'right'); got {entry!r}"
        )
    return coerced


def _check_member_existence(
    index: ClusterIndex,
    *,
    left_vertex_names: set[str],
    right_vertex_names: set[str],
    left_relation_names: set[str],
    right_relation_names: set[str],
) -> None:
    for cluster in index.vertices:
        for member in cluster.left:
            if member not in left_vertex_names:
                raise ValueError(
                    f"compose_manifests: left vertex {member!r} not in left "
                    f"manifest{_did_you_mean(member, left_vertex_names)}"
                )
        for member in cluster.right:
            if member not in right_vertex_names:
                raise ValueError(
                    f"compose_manifests: right vertex {member!r} not in right "
                    f"manifest{_did_you_mean(member, right_vertex_names)}"
                )
    for cluster in index.relations:
        for member in cluster.left:
            if member not in left_relation_names:
                raise ValueError(
                    f"compose_manifests: left relation {member!r} not in left manifest"
                )
        for member in cluster.right:
            if member not in right_relation_names:
                raise ValueError(
                    f"compose_manifests: right relation {member!r} not in "
                    "right manifest"
                )


def compose_manifests(
    left: GraphManifest,
    right: GraphManifest,
    op: ComposeManifestsOp,
    *,
    bump_version: bool | Literal["minor"] = "minor",
    finish_init: bool = True,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> GraphManifest:
    """Return a new manifest that is the deterministic compose of *left* and *right*.

    Each declared cluster (a :class:`~graflo.architecture.evolution.ops.VertexEquivalence`
    or :class:`~graflo.architecture.evolution.ops.RelationEquivalence`, possibly
    n-ary) is lowered to a per-side :class:`~graflo.architecture.evolution.canonical.CanonicalMap`
    and applied to that side standalone (property alignment, then merge/rename)
    before the two sides are unioned by name. Does not invent semantic matches.

    When ``op.identity_alignments`` is non-empty, the composed union is further
    rewritten by the fundamental ops emitted from each alignment (see
    :func:`~graflo.architecture.evolution.alignment.alignment_to_ops`). Pass
    *canonical_maps* — ``(side, CanonicalMap)`` pairs — so derivation inputs
    written in canonical vocabulary fail loudly rather than silently deriving
    nothing, and so a class already canonicalized on one side is checked
    against the other side's declared clusters.
    """
    if not isinstance(op, ComposeManifestsOp):
        raise TypeError(
            f"compose_manifests expects ComposeManifestsOp, got {type(op)!r}"
        )
    maps = _coerce_side_maps(canonical_maps)

    out_left = left.model_copy(deep=True)
    out_right = right.model_copy(deep=True)

    left_schema = _require_schema(out_left, "left")
    right_schema = _require_schema(out_right, "right")

    left_vertex_names = set(left_schema.core_schema.vertex_config.vertex_set)
    right_vertex_names = set(right_schema.core_schema.vertex_config.vertex_set)
    left_relation_names = _relation_names(left_schema)
    right_relation_names = _relation_names(right_schema)

    # Raw ClusterConflictError here (not wrapped): a compose op whose own
    # declarations conflict is broken regardless of any canonical map.
    index = index_clusters(
        op,
        left_vertices=left_vertex_names,
        right_vertices=right_vertex_names,
        left_relations=left_relation_names,
        right_relations=right_relation_names,
    )

    side_maps = validate_and_complete_canonical_map(
        op, left=out_left, right=out_right, canonical_maps=maps
    )
    _check_member_existence(
        index,
        left_vertex_names=left_vertex_names,
        right_vertex_names=right_vertex_names,
        left_relation_names=left_relation_names,
        right_relation_names=right_relation_names,
    )

    member_keys, member_property_names = _capture_all_member_state(
        index, left_schema, right_schema, side_maps
    )

    for manifest, side in ((out_left, "left"), (out_right, "right")):
        apply_manifest_ops_inplace(
            manifest,
            canonical_map_to_ops(
                side_maps[side],
                allow_self_relations=op.allow_self_relations,
                allow_row_fusion=op.allow_row_fusion,
            ),
        )

    left_resource_names: set[str] = set()
    if out_left.ingestion_model is not None:
        left_resource_names = {r.name for r in out_left.ingestion_model.resources}
    _apply_right_resource_policy(out_right, op, left_resource_names)
    _apply_right_schema_collision_policy(out_left, out_right, op)

    alignment_labels = {alignment.vertex for alignment in op.identity_alignments}
    composed_schema, retire_ops = _union_schema(
        _require_schema(out_left, "left"),
        _require_schema(out_right, "right"),
        index,
        member_keys,
        member_property_names,
        alignment_labels,
    )
    _assert_no_canonical_split(composed_schema)
    composed_ingestion = _union_ingestion(
        out_left.ingestion_model, out_right.ingestion_model
    )
    composed_bindings = _union_bindings(
        out_left.bindings, out_right.bindings, name_conflict=op.name_conflict
    )

    result = GraphManifest(
        graph_schema=composed_schema,
        ingestion_model=composed_ingestion,
        bindings=composed_bindings,
    )
    _bump_schema_version(result, bump_version)

    if retire_ops:
        apply_manifest_ops_inplace(result, retire_ops)

    if op.identity_alignments:
        result = _apply_identity_alignments(
            result,
            op,
            index=index,
            side_maps=side_maps,
            canonical_maps=maps,
            finish_init=False,
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )

    if finish_init:
        result.finish_init(
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
    return result


def _apply_identity_alignments(
    manifest: GraphManifest,
    op: ComposeManifestsOp,
    *,
    index: ClusterIndex,
    side_maps: SideMaps,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
    finish_init: bool,
    strict_references: bool,
    dynamic_edge_feedback: bool,
) -> GraphManifest:
    from .alignment import alignment_to_ops
    from .apply import apply_evolution

    cluster_labels = index.labels
    all_maps = [cm for _side, cm in canonical_maps] + [side_maps.left, side_maps.right]
    out = manifest
    for alignment in op.identity_alignments:
        if cluster_labels:
            if alignment.vertex not in cluster_labels:
                raise ValueError(
                    f"compose_manifests: identity alignment vertex "
                    f"{alignment.vertex!r} is not a declared cluster's `into` "
                    f"label {sorted(cluster_labels)}"
                )
        else:
            union_vertices = (
                out.graph_schema.core_schema.vertex_config.vertex_set
                if out.graph_schema is not None
                else set()
            )
            if alignment.vertex not in union_vertices:
                raise ValueError(
                    f"compose_manifests: identity alignment vertex "
                    f"{alignment.vertex!r} is not in the composed union"
                )
        ops = alignment_to_ops(alignment, manifest=out, canonical_maps=all_maps)
        out = apply_evolution(
            out,
            ops,
            bump_version=False,
            finish_init=finish_init,
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
    return out
