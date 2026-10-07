"""Binary merge of two :class:`~graflo.architecture.contract.manifest.GraphManifest`s."""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.ingestion.resource import (
    pipeline_has_pass_through_router,
    step_finds,
    step_looks_up,
    step_produces_vertices,
)
from graflo.architecture.contract.ingestion.steps.normalize import (
    normalize_actor_step,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import ManifestMetadata
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.refusal import Refusal
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.database_features import (
    DatabaseProfile,
    append_index,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig, union_inverses
from graflo.architecture.schema.identity_funnel import IdentityFunnel
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.namespace import validate_namespace
from graflo.architecture.schema.naming import NamingConvention, canonical_slug
from graflo.architecture.schema.semantics import merge_semantics
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
    ClusterResolution,
    SideMaps,
    canonical_near_collisions,
    canonicalize_ops,
    resolve_clusters,
)
from .db_profile import union_default_property_values
from .equivalence import Cluster, ClusterIndex, Side, subject
from .merge_core import (
    merge_edge_pair,
    merge_vertex_models,
)
from .merge_types import (
    MERGE_RETYPE_REMEDY,
    UnionNames,
    field_type_clashes,
    field_type_ops,
)
from .naming_graph import is_key_space_name
from .ops import (
    AddSecondaryIdentitiesOp,
    CanonicalizeOp,
    DerivedBranch,
    LocalKeyBranch,
    ManifestOp,
    MergeManifestsOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVerticesOp,
    VertexEquivalence,
    identity_branches_funnel,
)
from .version import semver_core

if TYPE_CHECKING:
    from .alignment import IdentityPlan

logger = logging.getLogger(__name__)

_RIGHT_PREFIX = "r_"


def _prefixed(name: str) -> str:
    if name.startswith(_RIGHT_PREFIX):
        return name
    return f"{_RIGHT_PREFIX}{name}"


def _free_prefixed_name(name: str, taken: set[str]) -> str:
    """``r_<name>``, disambiguated by an ordinal until it is free.

    The ordinal is not decoration. ``_prefixed`` is idempotent -- it refuses to
    build ``r_r_x`` -- so re-prefixing a taken name returns the same string,
    and a loop that re-prefixed until free would never terminate on a right
    side whose own names already start with ``r_``. Merging a manifest with
    itself is exactly that case.
    """
    candidate = _prefixed(name)
    if candidate not in taken:
        return candidate
    ordinal = 2
    while f"{candidate}_{ordinal}" in taken:
        ordinal += 1
    return f"{candidate}_{ordinal}"


def _resolve_name_collisions(
    occupied: set[str],
    candidates: list[str],
    *,
    name_conflict: Literal["error", "prefix_right", "union_right"],
    kind: str,
    hint: str = "rename one with `renames.<side>.resources`",
) -> dict[str, str]:
    """Return rename map for *candidates* that collide with *occupied*.

    Exact matching only, and deliberately so: resources and connectors are
    *addresses*, not concepts. Two whose names key alike cause no split -- each
    keeps its own name, each is looked up by that name, each targets its own
    vertex -- so canonical matching here would be pure false positive.
    ``union_right`` is meaningless for the same reason and behaves as ``error``.
    """
    renames: dict[str, str] = {}
    taken = set(occupied)
    for name in candidates:
        if name not in taken:
            taken.add(name)
            continue
        if name_conflict in ("error", "union_right"):
            raise ValueError(
                f"merge_manifests: {kind} name collision on {name!r}; "
                f"{hint}, or set name_conflict='prefix_right'"
            )
        new_name = _free_prefixed_name(name, taken)
        renames[name] = new_name
        taken.add(new_name)
    return renames


def _require_schema(manifest: GraphManifest, side: str) -> Schema:
    if manifest.graph_schema is None:
        raise ValueError(
            f"merge_manifests requires graph_schema on the {side} manifest"
        )
    return manifest.graph_schema


def _schema_of(manifest: GraphManifest) -> Schema | None:
    """The manifest's schema, or ``None`` when it carries no schema block.

    The counterpart to :func:`_require_schema`, which stays for the paths that
    genuinely cannot proceed without one. A manifest with only an
    ``ingestion_model`` and/or ``bindings`` -- a new source wired onto an
    existing type vocabulary -- is a legitimate merge input, so the union
    treats a missing side as contributing nothing rather than as an error.
    """
    return manifest.graph_schema


def _relation_names(schema: Schema | None) -> set[str]:
    if schema is None:
        return set()
    return {
        edge.relation
        for edge in schema.core_schema.edge_config.edges
        if edge.relation is not None
    }


def _vertex_names(schema: Schema | None) -> set[str]:
    if schema is None:
        return set()
    return set(schema.core_schema.vertex_config.vertex_set)


def _apply_resource_renames(
    manifest: GraphManifest, renames: Mapping[str, str]
) -> None:
    """Apply one side's ``renames.resources``."""
    effective = {old: new for old, new in renames.items() if old != new}
    if effective and manifest.ingestion_model is not None:
        apply_rename_resources(manifest, RenameResourcesOp(renames=effective))


def _apply_right_resource_policy(
    right: GraphManifest,
    op: MergeManifestsOp,
    left_resource_names: set[str],
) -> None:
    _apply_resource_renames(right, op.renames.right.resources)

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
        apply_rename_resources(right, RenameResourcesOp(renames=collisions))


class MergeNameConflictError(Refusal):
    """Two names denote one concept under different naming conventions.

    Distinct from ``MergeCanonicalConflictError`` in ``canonical.py``, which
    reports a *declared* CanonicalMap contradicting the op. This one fires on
    the residue neither side declared -- the undeclared path, where merge
    would otherwise produce two unrelated types with the data split between
    them and nothing raising.

    ``check`` names the rule that refused and ``subjects`` the names it is
    about, as :func:`~graflo.architecture.evolution.equivalence.subject` ids;
    see :class:`.Refusal`.
    """


class MergeIdentityError(Refusal):
    """A merged vertex's identity is ambiguous and nothing resolves it.

    Two or more cluster members disagree on their (canonical-name) identity
    field-set and the ``VertexEquivalence`` declares no ``identity``. The
    alternative -- silently taking the union of both field-sets as the new
    identity -- produces a natural key no record fully carries. Also raised
    for a declared ``identity`` some member cannot complete, or one whose
    funnel's synthetic ``id`` a member already declares as a property.

    ``subjects`` names the merged class and its disagreeing members, as
    :func:`~graflo.architecture.evolution.equivalence.subject` ids; it does not
    appear in the message.
    """

    def __init__(
        self, message: str, *, check: str = "", subjects: tuple[str, ...] = ()
    ) -> None:
        # The only refusal carrying a default check: every raise site here is
        # the same rule, and three of them pass no subjects either.
        super().__init__(
            message, check=check or "identity disagreement", subjects=subjects
        )


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

    The residue after cluster resolution. An **exact** collision (``Customer``
    on both sides) is a name no cluster merges: under ``error`` it has
    already been refused as incomplete by
    :func:`~graflo.architecture.evolution.canonical.resolve_clusters`, and
    under ``union_right`` it has already become a synthesized cluster -- so
    reaching one here is an invariant breach, not an authoring error. A
    **canonical** collision (``Customer`` / ``customer``, ``OrderLine`` /
    ``order_line``) is the same question with less confidence: ``error``
    refuses it naming both spellings, ``union_right`` has synthesized it under
    the left spelling upstream, and ``prefix_right`` keeps both apart here.

    Left alone the right names merge into two unrelated types with the
    source data split between them and nothing raising.
    """
    exact = [name for name in right_names if name in left_names and name not in exempt]
    near = canonical_near_collisions(left_names, right_names, exempt=exempt)

    if name_conflict == "error":
        if exact:
            raise ValueError(
                f"merge_manifests: {kind} name collision on "
                f"{sorted(exact)!r}; provide a {equivalence_hint} or set "
                "name_conflict='prefix_right'"
            )
        if near:
            pairs = ", ".join(f"{left!r} / {right!r}" for left, right in near)
            raise MergeNameConflictError(
                f"merge_manifests: {pairs} denote the same concept under "
                f"different naming conventions, so they would merge into two "
                f"unrelated {kind} types with the source data split between "
                f"them. Declare a {equivalence_hint} (or a CanonicalMap) "
                "to combine them, set name_conflict='union_right' to adopt the "
                "left spelling, or name_conflict='prefix_right' to keep them "
                "apart.",
                check=f"{kind} near collision",
                subjects=tuple(
                    name
                    for left, right in near
                    for name in (subject("left", left), subject("right", right))
                ),
            )
        return {}

    if name_conflict == "union_right":
        if exact or near:
            raise ValueError(
                f"merge_manifests: unreachable -- {kind} names "
                f"{sorted(exact) + [right for _left, right in near]!r} survived "
                "cluster resolution under union_right; every same-name pair "
                "should have been synthesized into a cluster"
            )
        return {}

    # prefix_right: keep both, explicitly, under distinguishable names.
    renames: dict[str, str] = {}
    taken = set(left_names) | set(right_names)
    for name in [*exact, *(right for _left, right in near)]:
        if name in renames:
            continue
        new_name = _free_prefixed_name(name, taken)
        renames[name] = new_name
        taken.add(new_name)
    return renames


def _assert_no_canonical_split(schema: Schema) -> None:
    """No two merged vertex types or relations may denote one concept.

    The invariant canonical name matching exists for, asserted on the result
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
            raise MergeNameConflictError(
                f"merge_manifests produced {kind} types that denote the same "
                f"concept under different spellings: {split}. This is an "
                "unhandled merge path, not an authoring error -- the result "
                "would split data between them silently.",
                check=f"{kind} canonical split",
                subjects=tuple(
                    subject("merged", name)
                    for group in split.values()
                    for name in group
                ),
            )


def _apply_right_schema_collision_policy(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp,
    index: ClusterIndex,
) -> None:
    """Prefix, union or error on non-equivalent right vertex/relation names.

    Names are compared both exactly and by :func:`canonical_slug`, so two
    spellings of one concept are a collision rather than two types.

    Deliberately **not** applied to properties, resources, connectors or
    transforms. A property name binds to a key in the source document, so
    fusing ``customer_email`` with ``customerEmail`` would fuse two columns fed
    by different keys; the rest are addresses looked up by exact name, where a
    near-collision splits nothing and the check would be pure false positive.

    A side with no schema block declares no types, so nothing can collide:
    return without renaming anything.
    """
    left_schema = _schema_of(left)
    right_schema = _schema_of(right)
    if left_schema is None or right_schema is None:
        return

    v_renames = _resolve_schema_collisions(
        left_names=set(left_schema.core_schema.vertex_config.vertex_set),
        right_names=sorted(right_schema.core_schema.vertex_config.vertex_set),
        exempt=index.labels,
        name_conflict=op.name_conflict,
        kind="vertex",
        equivalence_hint="VertexEquivalence",
    )
    if v_renames:
        apply_rename_vertices(right, RenameVerticesOp(renames=v_renames))

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
        exempt=index.relation_labels,
        name_conflict=op.name_conflict,
        kind="relation",
        equivalence_hint="RelationEquivalence",
    )
    if r_renames:
        apply_rename_relations(right, RenameRelationsOp(renames=r_renames))


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
            # Declared order, not sorted: a demoted key becomes a compound
            # index whose column order is the author's. Comparisons across
            # members are order-insensitive at the comparison site.
            keys[(side, member)] = tuple(rename.get(f, f) for f in vertex.identity)
    return keys, names


def _member_identity_fields(
    index: ClusterIndex,
    left_schema: Schema | None,
    right_schema: Schema | None,
    side_maps: SideMaps,
    demoted_keys: Mapping[tuple[Side, str], tuple[str, ...]],
) -> dict[str, set[str]]:
    """Per merged name, the identity fields its members' records carry.

    Canonical names, captured before the sides are rewritten, for every
    identity mode (a funnel member contributes its synthetic key field). A
    derived identity checks its attribute names against these rather than
    the merged class's intermediate identity, which it replaces. A demoted
    key counts under both names: the origin-prefixed property still reads
    the raw field a derivation named like it would overwrite.
    """
    fields: dict[str, set[str]] = {}
    for cluster in index.vertices:
        carried = fields.setdefault(cluster.into, set())
        for side, schema in (("left", left_schema), ("right", right_schema)):
            if schema is None:
                continue
            vertex_config = schema.core_schema.vertex_config
            for member in cluster.members(side):
                rename = side_maps[side].properties.get(member, {})
                carried.update(rename.get(f, f) for f in vertex_config[member].identity)
                carried.update(demoted_keys.get((side, member), ()))
    return fields


def _capture_all_member_state(
    index: ClusterIndex,
    left_schema: Schema | None,
    right_schema: Schema | None,
    side_maps: SideMaps,
) -> tuple[
    dict[tuple[Side, str], tuple[str, ...] | None],
    dict[tuple[Side, str], set[str]],
]:
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None] = {}
    member_property_names: dict[tuple[Side, str], set[str]] = {}
    for cluster in index.vertices:
        for side, schema in (("left", left_schema), ("right", right_schema)):
            if schema is None:
                continue
            k, n = _member_state(schema, cluster, side, side_maps[side].properties)
            member_keys.update(k)
            member_property_names.update(n)
    return member_keys, member_property_names


def _composed_identity(
    cluster: Cluster,
    merged: Vertex,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
) -> tuple[list[str] | IdentityFunnel, bool]:
    """The merged vertex's identity, and whether the cluster is re-keyed.

    *Re-keyed* means the merged class no longer upserts on its members' own
    keys: the equivalence declares ``identity``. Branches over properties the
    members carry are decided here -- one as a natural key, several as a
    funnel -- and must be ones every member can complete
    (:func:`_check_identity_coverage`). A declaration with a derived or
    ``local_key`` branch keeps the members' merged key for now: its steps
    need the assembled manifest, and :func:`_apply_derived_identities`
    replaces it. Either way the members' pre-merge keys are demoted afterwards.

    Undeclared, the merged key is carried through when every plain-natural-key
    member agrees (or fewer than two are plain), as in an ordinary
    :func:`~graflo.architecture.evolution.merge_core.merge_vertex_models`
    merge. Disagreement raises :class:`MergeIdentityError`: the union of the
    disagreeing keys would be one no record carries.
    """
    declaration = cluster.declaration
    assert isinstance(declaration, VertexEquivalence)
    if declaration.identity is not None:
        if declaration.has_derivation:
            return list(merged.identity), True
        raw = declaration.raw_branches()
        if len(raw) == 1:
            return list(raw[0]), True
        return identity_branches_funnel(declaration.identity), True

    plain_members: list[tuple[Side, str, tuple[str, ...]]] = [
        (side, member, fields)
        for side in ("left", "right")
        for member in cluster.members(side)
        if (fields := member_keys.get((side, member))) is not None
    ]
    plain_keys = {frozenset(fields) for _side, _member, fields in plain_members}
    if len(plain_keys) <= 1:
        return list(merged.identity), False
    detail = "; ".join(
        f"{side}:{member}={list(fields)}" for side, member, fields in plain_members
    )
    raise MergeIdentityError(
        f"merge_manifests: merged vertex {cluster.into!r} has members "
        f"that disagree on identity ({detail}) and nothing resolves it. "
        "Declare `identity` on the VertexEquivalence: a property every member "
        "carries, one branch per member's own key, or a derived branch every "
        "source computes.",
        subjects=(
            subject("merged", cluster.into),
            *(subject(side, member) for side, member, _f in plain_members),
        ),
    )


def _check_digest_field_free(
    cluster: Cluster,
    digest_field: str,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
    member_property_names: dict[tuple[Side, str], set[str]],
) -> None:
    """Refuse a funnel over a merged class whose members declare *digest_field*.

    A funnel keys a record on a synthetic digest stored in *digest_field*, and
    the cast discards a record's own value there for it. A member that
    declares the field as a real property would lose its values silently.
    """
    for side in ("left", "right"):
        for member in cluster.members(side):
            if member_keys.get((side, member)) is None:
                continue  # its own identity is synthetic; its key is no column
            if digest_field in member_property_names.get((side, member), set()):
                raise MergeIdentityError(
                    f"merge_manifests: merged vertex {cluster.into!r} is keyed "
                    f"on a funnel whose digest is stored in `{digest_field}`, "
                    f"but {side}:{member} declares a property `{digest_field}`, "
                    "whose values the digest would replace. Rename the property "
                    "with a PropertyEquivalence, or store the digest in another "
                    "field with `digest_field` on the VertexEquivalence.",
                    check="identity collision",
                    subjects=(subject("merged", cluster.into), subject(side, member)),
                )


def _check_identity_coverage(
    cluster: Cluster,
    identity: list[str] | IdentityFunnel,
    member_property_names: dict[tuple[Side, str], set[str]],
) -> None:
    """Refuse a re-keyed identity some member cannot complete.

    A record keys on the merged identity only if it carries every field of it
    (a natural key) or of one branch (a funnel). A member that declares none
    of them contributes records that complete no key and are dropped at
    ingestion -- the whole member, silently. Checked in canonical names, after
    the cluster's and the canonical maps' property renames.
    """
    branches = (
        [set(branch.required_fields) for branch in identity.branches]
        if isinstance(identity, IdentityFunnel)
        else [set(identity)]
    )
    if isinstance(identity, IdentityFunnel):
        declared_anywhere: set[str] = set()
        for side in ("left", "right"):
            for member in cluster.members(side):
                declared_anywhere |= member_property_names.get((side, member), set())
        for branch in branches:
            if not branch <= declared_anywhere:
                raise MergeIdentityError(
                    f"merge_manifests: merged vertex {cluster.into!r} is keyed on "
                    f"{_describe_identity(identity)}, but no member declares "
                    f"{sorted(branch - declared_anywhere)}, so no record can "
                    "complete that branch. Name a property the members carry, "
                    "under its canonical name.",
                    check="identity coverage",
                    subjects=(subject("merged", cluster.into),),
                )
    for side in ("left", "right"):
        for member in cluster.members(side):
            declared = member_property_names.get((side, member))
            if declared is None or any(branch <= declared for branch in branches):
                continue
            if isinstance(identity, IdentityFunnel):
                detail = (
                    "completes none of the funnel branches "
                    f"{[sorted(branch) for branch in branches]}"
                )
            else:
                detail = f"does not carry {sorted(branches[0] - declared)}"
            raise MergeIdentityError(
                f"merge_manifests: merged vertex {cluster.into!r} is keyed on "
                f"{_describe_identity(identity)}, but {side}:{member} {detail}, "
                "so every one of its records would complete no key and be "
                "dropped. Map the field onto the member with a "
                "PropertyEquivalence, or key each member on what it carries "
                "(one identity branch per member's own key).",
                check="identity coverage",
                subjects=(subject("merged", cluster.into), subject(side, member)),
            )


def _describe_identity(identity: list[str] | IdentityFunnel) -> str:
    if isinstance(identity, IdentityFunnel):
        return f"a funnel over {[b.required_fields for b in identity.branches]}"
    return repr(list(identity))


def _apply_composed_identity(
    merged: Vertex,
    identity: list[str] | IdentityFunnel,
    *,
    declared: bool,
    digest_field: str,
) -> Vertex:
    if not declared:
        return merged.model_copy(update={"identity": identity})
    if isinstance(identity, IdentityFunnel):
        # A funnel-mode vertex carries its digest under *digest_field* -- as
        # `apply_replace_identity` does for a FunnelIdentityTarget.
        return merged.model_copy(
            update={
                "identity": [digest_field],
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


def _retire_member_keys(
    manifest: GraphManifest,
    index: ClusterIndex,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
    rekeyed: Collection[str],
    demoted_keys: Mapping[tuple[Side, str], tuple[str, ...]],
    origins: Mapping[Side, str],
    key_spaces: Mapping[tuple[Side, str], str] | None = None,
) -> dict[tuple[Side, str], str]:
    """Demote each re-keyed member's pre-merge key to a lookup-only secondary.

    Runs once the merged class has its **final** identity -- after any
    derived identity is lowered -- because that is the primary a demoted key
    must not restate.

    Only for the members the naming pass demoted (*demoted_keys*), in
    clusters in *rekeyed* whose ``retire`` is ``demote`` (the default). The
    secondary is named by the member's key space (*key_spaces*: its
    ``local_key`` tag, else its side's origin), one per key space per class;
    a key space contributing two key field-sets to one class names each
    ``<space>__<fields joined by __>``. A field-set already declared as a
    secondary keeps that declaration and its name. The first member to
    declare a field-set fixes its column order.

    Returns the secondary name each demoted member's key now answers to, keyed
    by ``(side, member)``.
    """
    schema = manifest.graph_schema
    if schema is None:
        return {}
    vertex_config = schema.core_schema.vertex_config
    spaces = key_spaces or {}
    ops: list[ManifestOp] = []
    demoted: dict[tuple[Side, str], str] = {}
    for cluster in index.vertices:
        if cluster.into not in rekeyed or cluster.declaration.retire != "demote":
            continue
        vertex = vertex_config[cluster.into]
        primary = frozenset(
            vertex.identity_funnel.field_names
            if vertex.identity_funnel is not None
            else vertex.identity
        )
        # A funnel keys on its digest field; a member key spelled like it
        # cannot be demoted beside it without restating the primary.
        restated = {primary, frozenset(vertex.identity)}
        authored = {
            entry.name: frozenset(entry.fields) for entry in vertex.secondary_identities
        }
        by_fields = {fields: name for name, fields in authored.items()}
        keys: list[tuple[Side, str, tuple[str, ...]]] = []
        for side in ("left", "right"):
            for member in cluster.members(side):
                fields = member_keys.get((side, member))
                if (
                    (side, member) not in demoted_keys
                    or not fields
                    or frozenset(fields) in restated
                ):
                    continue
                keys.append((side, member, fields))
        field_sets: dict[str, set[frozenset[str]]] = {}
        for side, member, fields in keys:
            if frozenset(fields) not in by_fields:
                field_sets.setdefault(
                    spaces.get((side, member), origins[side]), set()
                ).add(frozenset(fields))
        additions: list[SecondaryIdentity] = []
        for side, member, fields in keys:
            if frozenset(fields) not in by_fields:
                name = spaces.get((side, member), origins[side])
                if len(field_sets[name]) > 1:
                    name = "__".join((name, *demoted_keys[(side, member)]))
                if name in authored:
                    raise MergeIdentityError(
                        f"merge_manifests: the key {side}:{member} demotes on "
                        f"{cluster.into!r} would be the secondary identity "
                        f"{name!r}, which a member already declares over "
                        f"{sorted(authored[name])}; rename that secondary, or "
                        f"set `origins.{side}` on the op, or tag the member's "
                        "`local_key` otherwise",
                        check="origin",
                        subjects=(subject("merged", cluster.into),),
                    )
                entry = SecondaryIdentity(name=name, fields=list(fields))
                by_fields[frozenset(fields)] = entry.name
                additions.append(entry)
            demoted[(side, member)] = by_fields[frozenset(fields)]
        if additions:
            ops.append(AddSecondaryIdentitiesOp(additions={cluster.into: additions}))
    if ops:
        apply_manifest_ops_inplace(manifest, ops)
    return demoted


def origin_refusals(
    op: MergeManifestsOp,
    manifests: Mapping[Side, GraphManifest],
    resolution: ClusterResolution,
) -> list[MergeIdentityError]:
    """Every problem with the origins *resolution* names something by.

    Only a side whose origin names a demoted key or a defaulted ``local_key``
    tag is checked. Its origin must be a letter then letters, digits or
    underscores, without ``__`` (it is joined to field names with ``__``),
    and not a selector word (``identity``, ``secondary``); two named origins
    must differ; and no member of a re-keyed cluster may declare a secondary
    identity of that name, which the demoted key takes.
    """
    sides: tuple[Side, ...] = ("left", "right")
    used = [side for side in sides if side in resolution.origin_sides]
    if not used:
        return []
    origins = resolution.origins
    rekeyed = [
        cluster
        for cluster in resolution.index.vertices
        if isinstance(cluster.declaration, VertexEquivalence)
        and cluster.declaration.identity is not None
    ]
    subjects = tuple(subject("merged", cluster.into) for cluster in rekeyed)
    declared = op.origins or {}

    def source(side: Side) -> str:
        if side in declared:
            return f"`origins.{side}`"
        return f"the {side} schema name"

    out: list[MergeIdentityError] = []
    for side in used:
        origin = origins[side]
        if not is_key_space_name(origin):
            out.append(
                MergeIdentityError(
                    f"merge_manifests: origin {origin!r} ({source(side)}) names "
                    f"the {side} side's demoted keys, but is not a letter "
                    "followed by letters, digits or single underscores, or is a "
                    f"selector word; set `origins.{side}` on the op",
                    check="origin",
                    subjects=subjects,
                )
            )
        authors = sorted(
            f"{member_side}:{member}"
            for cluster in rekeyed
            for member_side in sides
            if (schema := manifests[member_side].graph_schema) is not None
            for member in cluster.members(member_side)
            if member in schema.core_schema.vertex_config.vertex_set
            and origin
            in {
                entry.name
                for entry in schema.core_schema.vertex_config[
                    member
                ].secondary_identities
            }
        )
        if authors:
            out.append(
                MergeIdentityError(
                    f"merge_manifests: origin {origin!r} ({source(side)}) names "
                    f"the {side} side's demoted keys, but {', '.join(authors)} "
                    f"declare a secondary identity named {origin!r}; set "
                    f"`origins.{side}` on the op",
                    check="origin",
                    subjects=subjects,
                )
            )
    if len(used) == 2 and origins["left"] == origins["right"]:
        out.append(
            MergeIdentityError(
                f"merge_manifests: both sides' origin is {origins['left']!r}, "
                "so their demoted keys would share one name; set `origins` on "
                "the op",
                check="origin",
                subjects=subjects,
            )
        )
    return out


def _steps_producing(
    steps: Sequence[Any], vertex: str, *, known_vertices: Collection[str]
) -> list[dict[str, Any]]:
    """Every step at any level of *steps* that produces *vertex*, normalized."""
    out: list[dict[str, Any]] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        normalized = normalize_actor_step(dict(step))
        if vertex in step_produces_vertices(normalized, known_vertices=known_vertices):
            out.append(normalized)
        if normalized.get("type") == "descend":
            nested = normalized.get("pipeline")
            if isinstance(nested, list):
                out.extend(
                    _steps_producing(nested, vertex, known_vertices=known_vertices)
                )
    return out


def _members_produced(
    resource: str, cluster: Cluster, sides: Mapping[str, GraphManifest]
) -> tuple[Side, list[str]] | None:
    """The side *resource* comes from, and the members of *cluster* it produced there.

    Judged on the side manifest, before the relabel: an unbounded router
    produces every class of its side by pass-through, so it produces every
    member there; a closed or bounded one, the members in its reach.
    """
    for side in ("left", "right"):
        side_manifest = sides[side]
        side_ingestion = side_manifest.ingestion_model
        side_schema = side_manifest.graph_schema
        if side_ingestion is None or side_schema is None:
            continue
        pipeline = next(
            (r.pipeline for r in side_ingestion.resources if r.name == resource), None
        )
        if pipeline is None:
            continue
        side_vertices = side_schema.core_schema.vertex_config.vertex_set
        return side, sorted(
            member
            for member in cluster.members(side)
            if _steps_producing(pipeline, member, known_vertices=side_vertices)
        )
    return None


def _rebuild_pipelines(
    manifest: GraphManifest,
    rewrites: Mapping[str, Callable[[list[Any]], list[Any]]],
) -> None:
    """Apply each resource's pipeline rewrite in *rewrites*, revalidating the model."""
    ingestion = manifest.ingestion_model
    if ingestion is None or not rewrites:
        return
    from graflo.architecture.contract.ingestion.resource import Resource

    resources: list[Resource] = []
    for resource in ingestion.resources:
        rewrite = rewrites.get(resource.name)
        if rewrite is None:
            resources.append(resource)
            continue
        payload = resource.to_dict(skip_defaults=False)
        payload["pipeline"] = rewrite(resource.pipeline)
        resources.append(Resource.model_validate(payload))
    ingestion.resources = resources
    manifest.ingestion_model = IngestionModel.model_validate(
        ingestion.to_dict(skip_defaults=False)
    )


@dataclass(frozen=True)
class KeyOwner:
    """A resource a derived identity names that produces the merged class.

    It computes the class's key, so it creates and fuses its nodes.
    """

    resource: str
    vertex: str
    side: Side
    members: tuple[str, ...]


@dataclass(frozen=True)
class AttachedProducer:
    """A resource merge turned from creating a merged class into attaching to it.

    Its steps producing the class now ``find`` it by *key*, write their
    properties onto the node found, and create none.
    """

    resource: str
    vertex: str
    side: Side
    members: tuple[str, ...]
    key: str
    """The secondary identity its records find the class by."""


@dataclass(frozen=True)
class PinnedReference:
    """A resource that only references a re-keyed member, pointed at its key.

    Its edge endpoints for the class now select *key*.
    """

    resource: str
    vertex: str
    side: Side
    members: tuple[str, ...]
    key: str


@dataclass(frozen=True)
class SharedKeySpace:
    """Members of one side whose demoted keys are one secondary identity.

    Their key values are taken to be one space: an id of one member finds a
    node of another.
    """

    vertex: str
    side: Side
    members: tuple[str, ...]
    key: str


@dataclass(frozen=True)
class OrderCycle:
    """A resource order constraint the union's resource order leaves unmet.

    *resource* depends on *before* for *vertex* -- it attaches to or
    references what *before* writes -- but runs first, because the
    constraints formed a cycle and the declared order was kept.
    """

    resource: str
    before: str
    vertex: str


@dataclass(frozen=True)
class DemotedKey:
    """A member's pre-merge key the merge demoted to a lookup-only secondary."""

    vertex: str
    side: Side
    member: str
    fields: tuple[str, ...]
    secondary: str
    branches: tuple[str, ...] = ()
    """Funnel branches the member's records can complete, in order, up to the
    one over its own key. Two or more mean the key no longer deduplicates."""


@dataclass(frozen=True)
class MergeReport:
    """What a merge did to the sources, beyond the manifest it returns."""

    demoted: list[DemotedKey] = field(default_factory=list)
    owners: list[KeyOwner] = field(default_factory=list)
    attached: list[AttachedProducer] = field(default_factory=list)
    references: list[PinnedReference] = field(default_factory=list)
    shared_key_spaces: list[SharedKeySpace] = field(default_factory=list)
    order_cycles: list[OrderCycle] = field(default_factory=list)


def _completes_property_branch(
    plan: IdentityPlan,
    cluster: Cluster,
    sides: Mapping[str, GraphManifest],
    member_property_names: Mapping[tuple[Side, str], set[str]],
) -> Callable[[str], bool]:
    """Whether a resource's records can complete one of *plan*'s property branches.

    They can when every member of *cluster* the resource produces on its side
    declares every field of some property branch, in canonical names.
    """

    def completes(resource: str) -> bool:
        origin = _members_produced(resource, cluster, sides)
        if origin is None:
            return False
        side, members = origin
        return bool(members) and all(
            any(
                set(fields) <= member_property_names.get((side, member), set())
                for fields in plan.raw
            )
            for member in members
        )

    return completes


def _attach_uncovered_producers(
    manifest: GraphManifest,
    plans: Sequence[tuple[Cluster, IdentityPlan]],
    sides: Mapping[str, GraphManifest],
    demoted: Mapping[tuple[Side, str], str],
    member_property_names: Mapping[tuple[Side, str], set[str]],
) -> list[AttachedProducer]:
    """Attach each resource a derived identity leaves unable to key its class.

    A resource that creates the class, derives none of its branches and
    completes none of its property branches
    (:func:`~graflo.architecture.evolution.alignment.uncovered_producers`)
    carries the member's own key and nothing of the new one: every record it
    created would complete no funnel branch and be dropped, with its edges.
    It still carries the key :func:`_retire_member_keys` has just demoted to
    a secondary identity, so its steps producing the class ``find`` the node
    by that key -- ``find: <key>`` on a vertex step, ``{class: key}`` in the
    ``find`` map of every router that can reach it, in any role -- write
    their properties onto it and create none. Edges built from those records
    follow the selector without one of their own.

    Refused, with
    :class:`~graflo.architecture.evolution.alignment.AlignmentConflictError`,
    when no member key was demoted for the resource to find the class by
    (``retire: keep``), and with :class:`MergeIdentityError` (``ambiguous
    reference``) when the members it produces were demoted to different
    secondaries, since a resource finds a class by one.
    """
    from .alignment import AlignmentConflictError, uncovered_producers
    from .rewrite import mark_find_in_pipeline

    marks: dict[str, list[tuple[str, str]]] = {}
    attached: list[AttachedProducer] = []
    for cluster, plan in plans:
        completes = _completes_property_branch(
            plan, cluster, sides, member_property_names
        )
        for resource in uncovered_producers(
            plan, manifest, completes_property_branch=completes
        ):
            origin = _members_produced(resource, cluster, sides)
            side, members = origin if origin is not None else (None, [])
            keys = sorted(
                {demoted.get((side, member)) or "" for member in members}
                if side is not None
                else set()
            )
            if not members or "" in keys:
                if cluster.declaration.retire == "keep":
                    why = (
                        "the cluster sets `retire: keep`, so no member key became "
                        "a secondary identity to find it by"
                    )
                    remedy = "Drop `retire: keep`, or add"
                else:
                    why = (
                        "no member key it carries became a secondary identity to "
                        "find it by"
                    )
                    remedy = "Add"
                raise AlignmentConflictError(
                    "derived identity conflict (uncovered producer): resource "
                    f"{resource!r} upserts {plan.vertex!r} but derives none of "
                    f"its key, and {why}. {remedy} the resource to a derived "
                    "branch's sources if its rows carry the inputs."
                )
            assert side is not None
            if len(keys) > 1:
                raise MergeIdentityError(
                    f"merge_manifests: resource {resource!r} upserts {side} "
                    f"members {members} of {plan.vertex!r}, whose keys were "
                    f"demoted to different secondary identities {keys}; it "
                    "cannot find the class by one of them. Tag the members' "
                    "`local_key` alike (one key space), produce each member from "
                    "its own resource, or add the resource to a derived "
                    "branch's sources.",
                    check="ambiguous reference",
                    subjects=(
                        subject("merged", plan.vertex),
                        *(subject(side, member) for member in members),
                    ),
                )
            marks.setdefault(resource, []).append((plan.vertex, keys[0]))
            attached.append(
                AttachedProducer(
                    resource=resource,
                    vertex=plan.vertex,
                    side=side,
                    members=tuple(members),
                    key=keys[0],
                )
            )
            logger.info(
                "merge_manifests: resource %r upserts %r but no derived identity "
                "branch names it; it now finds %r by %r, writes its properties "
                "onto the node found, and creates none",
                resource,
                plan.vertex,
                plan.vertex,
                keys[0],
            )

    def _marking(found: list[tuple[str, str]]) -> Callable[[list[Any]], list[Any]]:
        def rewrite(pipeline: list[Any]) -> list[Any]:
            for vertex, key in found:
                pipeline = mark_find_in_pipeline(pipeline, vertex, key)
            return pipeline

        return rewrite

    _rebuild_pipelines(
        manifest,
        {resource: _marking(found) for resource, found in marks.items()},
    )
    return attached


def _key_owners(
    manifest: GraphManifest,
    plans: Sequence[tuple[Cluster, IdentityPlan]],
    sides: Mapping[str, GraphManifest],
) -> list[KeyOwner]:
    """The resources each derived identity names that produce its class."""
    from .alignment import _referenced_resources

    ingestion = manifest.ingestion_model
    schema = manifest.graph_schema
    if ingestion is None or schema is None:
        return []
    known = schema.core_schema.vertex_config.vertex_set
    pipelines = {resource.name: resource.pipeline for resource in ingestion.resources}
    owners: list[KeyOwner] = []
    for cluster, plan in plans:
        for resource in sorted(_referenced_resources(plan)):
            pipeline = pipelines.get(resource)
            if pipeline is None or not _steps_producing(
                pipeline, plan.vertex, known_vertices=known
            ):
                continue
            origin = _members_produced(resource, cluster, sides)
            if origin is None:
                continue
            side, members = origin
            owners.append(
                KeyOwner(
                    resource=resource,
                    vertex=plan.vertex,
                    side=side,
                    members=tuple(members),
                )
            )
    return owners


def _shared_key_spaces(
    index: ClusterIndex, demoted: Mapping[tuple[Side, str], str]
) -> list[SharedKeySpace]:
    """Each group of same-side members of a cluster demoted to one key."""
    out: list[SharedKeySpace] = []
    for cluster in index.vertices:
        for side in ("left", "right"):
            by_key: dict[str, list[str]] = {}
            for member in cluster.members(side):
                key = demoted.get((side, member))
                if key is not None:
                    by_key.setdefault(key, []).append(member)
            out.extend(
                SharedKeySpace(
                    vertex=cluster.into,
                    side=side,
                    members=tuple(sorted(members)),
                    key=key,
                )
                for key, members in sorted(by_key.items())
                if len(members) > 1
            )
    return out


def key_space_refusals(
    resolution: ClusterResolution, manifests: Mapping[Side, GraphManifest]
) -> list[MergeIdentityError]:
    """Every problem with the key spaces *resolution* names demoted keys by.

    A ``local_key`` tag names the key space of a member's own ids, and its
    demoted key is named by it. Refused: a member two of its resources tag
    differently (its ids would be in two spaces at once), a tag that cannot
    name a key space (see :func:`~graflo.architecture.evolution.naming_graph.is_key_space_name`),
    a tag a member of the cluster already declares as a secondary identity
    (the demoted key would take that name), and a key space both sides name
    (two sides' ids in one space). An origin colliding with a secondary is
    :func:`origin_refusals`' to report.
    """
    origins = resolution.origins
    clusters = {
        (side, member): cluster.into
        for cluster in resolution.index.vertices
        for side in ("left", "right")
        for member in cluster.members(side)
    }
    out: list[MergeIdentityError] = []
    authored: dict[str, dict[str, list[str]]] = {}
    for cluster in resolution.index.vertices:
        for member_side in ("left", "right"):
            schema = manifests[member_side].graph_schema
            if schema is None:
                continue
            vertex_config = schema.core_schema.vertex_config
            for member in cluster.members(member_side):
                if member not in vertex_config.vertex_set:
                    continue
                for entry in vertex_config[member].secondary_identities:
                    authored.setdefault(cluster.into, {}).setdefault(
                        entry.name, []
                    ).append(f"{member_side}:{member}")
    for (side, member), space in sorted(resolution.key_spaces.items()):
        into = clusters.get((side, member))
        authors = authored.get(into or "", {}).get(space)
        if space == origins[side] or not authors:
            continue
        out.append(
            MergeIdentityError(
                f"merge_manifests: key space: {side}:{member} is tagged "
                f"{space!r}, which names its demoted key on {into!r}, but "
                f"{', '.join(sorted(authors))} declare a secondary identity "
                f"named {space!r}; give the member another tag, or rename that "
                "secondary",
                check="key space",
                subjects=(subject("merged", into or member), subject(side, member)),
            )
        )
    for (side, member), tags in sorted(resolution.key_tags.items()):
        nodes = (
            subject("merged", clusters.get((side, member), member)),
            subject(side, member),
        )
        if len(tags) > 1:
            out.append(
                MergeIdentityError(
                    f"merge_manifests: key space: {side}:{member} is tagged "
                    f"{list(tags)} by its resources' local_key entries, so its "
                    "ids would be in several key spaces; give the member one tag",
                    check="key space",
                    subjects=nodes,
                )
            )
        elif tags[0] != origins[side] and not is_key_space_name(tags[0]):
            out.append(
                MergeIdentityError(
                    f"merge_manifests: key space: {side}:{member} is tagged "
                    f"{tags[0]!r}, which names its demoted key but is not a "
                    "letter followed by letters, digits or single underscores, "
                    "or is a selector word; give the member another tag",
                    check="key space",
                    subjects=nodes,
                )
            )
    named: dict[str, set[Side]] = {}
    for (side, _member), space in resolution.key_spaces.items():
        named.setdefault(space, set()).add(side)
    for space, sides in sorted(named.items()):
        # Two equal origins are the origin rule's refusal.
        if len(sides) < 2 or space == origins["left"] == origins["right"]:
            continue
        members = sorted(
            (side, member)
            for (side, member), name in resolution.key_spaces.items()
            if name == space
        )
        named_members = ", ".join(f"{side}:{member}" for side, member in members)
        out.append(
            MergeIdentityError(
                f"merge_manifests: key space: {named_members} would share "
                f"the key space {space!r}, though they come from different "
                "sides; tag one side's members otherwise, or set `origins`",
                check="key space",
                subjects=tuple(subject(side, member) for side, member in members),
            )
        )
    return out


def _demoted_key_names(resolution: ClusterResolution) -> dict[tuple[Side, str], str]:
    """The secondary each demoted member key will be, as the naming pass sees it.

    Its key space, joined with its fields when that space demotes several
    field sets onto one class -- the rule of :func:`_retire_member_keys`,
    short of reusing a secondary a member already declares over the fields.
    """
    origins = resolution.origins
    into_of = {
        (side, member): cluster.into
        for cluster in resolution.index.vertices
        for side in ("left", "right")
        for member in cluster.members(side)
    }

    def space(side: Side, member: str) -> str:
        return resolution.key_spaces.get((side, member), origins[side])

    field_sets: dict[tuple[str, str], set[frozenset[str]]] = {}
    for (side, member), fields in resolution.demoted_keys.items():
        into = into_of.get((side, member))
        if into is not None:
            field_sets.setdefault((into, space(side, member)), set()).add(
                frozenset(fields)
            )
    out: dict[tuple[Side, str], str] = {}
    for (side, member), fields in resolution.demoted_keys.items():
        into = into_of.get((side, member))
        if into is None:
            continue
        name = space(side, member)
        if len(field_sets[(into, name)]) > 1:
            name = "__".join((name, *fields))
        out[(side, member)] = name
    return out


def ambiguous_reference_refusals(
    resolution: ClusterResolution,
    sides: Mapping[str, GraphManifest],
    member_properties: Mapping[tuple[Side, str], set[str]],
) -> list[MergeIdentityError]:
    """Each resource that would have to find one class by two demoted keys.

    The rule :func:`_attach_uncovered_producers` and
    :func:`_pin_member_references` refuse on, read from the sides before the
    union: a resource producing members of one side whose keys are demoted to
    different secondaries -- two key spaces, or two field sets -- when it
    only references them (every step ``lookup_only``), or when it would be
    attached: it writes them, the derived identity names it nowhere, and its
    records complete no property branch. *sides* carry the union's resource
    names; *member_properties* are each member's properties in canonical
    names.
    """
    names = _demoted_key_names(resolution)
    out: list[MergeIdentityError] = []
    for cluster in resolution.index.vertices:
        declaration = cluster.declaration
        if (
            not isinstance(declaration, VertexEquivalence)
            or declaration.identity is None
            or declaration.retire != "demote"
        ):
            continue
        local_key = declaration.local_key_branch()
        owners = {
            *declaration.derive_at,
            *(r for branch in declaration.derivations() for r in branch.sources),
            *(local_key.local_key if local_key is not None else ()),
        }
        raw = declaration.raw_branches()
        for side in ("left", "right"):
            manifest = sides[side]
            ingestion, schema = manifest.ingestion_model, manifest.graph_schema
            if ingestion is None or schema is None:
                continue
            known = schema.core_schema.vertex_config.vertex_set
            for resource in ingestion.resources:
                produced = {
                    member: steps
                    for member in cluster.members(side)
                    if (
                        steps := _steps_producing(
                            resource.pipeline, member, known_vertices=known
                        )
                    )
                }
                keys = {names.get((side, member)) for member in produced}
                named = sorted(key for key in keys if key is not None)
                if len(named) < 2 or any(
                    step_finds(step, member) is not None
                    for member, steps in produced.items()
                    for step in steps
                ):
                    continue
                references = all(
                    step_looks_up(step, member)
                    for member, steps in produced.items()
                    for step in steps
                )
                attached = (
                    not references
                    and declaration.has_derivation
                    and resource.name not in owners
                    and None not in keys
                    and not (
                        raw
                        and all(
                            any(
                                set(fields)
                                <= member_properties.get((side, member), set())
                                for fields in raw
                            )
                            for member in produced
                        )
                    )
                )
                if not references and not attached:
                    continue
                members = sorted(produced)
                out.append(
                    MergeIdentityError(
                        f"merge_manifests: resource {resource.name!r} "
                        f"{'references' if references else 'upserts'} {side} "
                        f"members {members} of {cluster.into!r}, whose keys are "
                        f"demoted to different secondary identities {named}; it "
                        "cannot find the class by one of them. Tag the members' "
                        "`local_key` alike (one key space), or produce each "
                        "member from its own resource.",
                        check="ambiguous reference",
                        subjects=(
                            subject("merged", cluster.into),
                            *(subject(side, member) for member in members),
                        ),
                    )
                )
    return out


def _close_side_routers(
    manifest: GraphManifest, sides: Mapping[str, GraphManifest]
) -> None:
    """Close each resource's routers over the classes of the side it came from.

    An open router passes a value its ``type_map`` does not name through as
    the class name -- after the union, any class of the merged schema, the
    other side's included, where the value used to be skipped. Each class the
    side's relabel renamed already has its ``{old: new}`` entry: the cluster
    and the canonical map, written into the table. Listing the side's other
    classes as themselves and setting ``type_map_only`` completes it, so the
    router routes exactly what it did before the merge. The self-entries are
    load-bearing: a closed router skips any value its table does not name,
    and the static analyses read the table as the classes the router
    produces. A router with ``vertex_types`` is closed too -- a listed class
    renamed onto the other side's name would otherwise pass that name through
    -- with self-entries for its listed classes only. This runs last, so the
    derived-identity lowering and the attaching of producers see routers as
    they always have. ``router_scope: union`` skips it; a relabel that merges
    a listed class with an unlisted one closes the router regardless (see
    ``evolve_router``).
    """
    from .rewrite import close_routers_in_pipeline, pass_through_classes

    ingestion = manifest.ingestion_model
    if ingestion is None:
        return
    open_routers = {
        resource.name
        for resource in ingestion.resources
        if pipeline_has_pass_through_router(resource.pipeline)
    }
    vocabulary_of: dict[str, frozenset[str]] = {}
    pass_through_of: dict[str, set[str]] = {}
    for side in ("left", "right"):
        side_ingestion = sides[side].ingestion_model
        side_schema = sides[side].graph_schema
        if side_ingestion is None or side_schema is None:
            continue
        vocabulary = frozenset(side_schema.core_schema.vertex_config.vertex_set)
        for resource in side_ingestion.resources:
            if resource.name in open_routers:
                vocabulary_of.setdefault(resource.name, vocabulary)
                pass_through_of.setdefault(
                    resource.name,
                    pass_through_classes(list(resource.pipeline), vocabulary),
                )

    # A class the router reached by pass-through keeps the part of the
    # router-level `from` it declares in the union, as it did when open --
    # under whatever name the relabel gave it.
    union_schema = manifest.graph_schema
    declared: dict[str, list[str]] | None = None
    if union_schema is not None:
        union_vertices = union_schema.core_schema.vertex_config
        declared = {
            name: list(union_vertices.property_names(name))
            for name in union_vertices.vertex_set
        }

    def _closing(
        vocabulary: frozenset[str], pass_through: set[str]
    ) -> Callable[[list[Any]], list[Any]]:
        return lambda pipeline: close_routers_in_pipeline(
            pipeline,
            vocabulary,
            declared_properties=declared,
            pass_through=pass_through,
        )

    _rebuild_pipelines(
        manifest,
        {
            name: _closing(vocabulary, pass_through_of[name])
            for name, vocabulary in vocabulary_of.items()
        },
    )


def _pin_member_references(
    manifest: GraphManifest,
    index: ClusterIndex,
    sides: Mapping[str, GraphManifest],
    demoted: Mapping[tuple[Side, str], str],
    rekeyed: Collection[str],
) -> list[PinnedReference]:
    """Point each reference to a re-keyed member at that member's demoted key.

    A resource that only *references* a member -- every step producing the
    merged class only looks it up (``lookup_only`` on a vertex step or a
    router), the edge-only source shape -- carries the member's own key and
    nothing of the merged identity. Left on the primary, its edges would look
    the endpoint up by a key its rows cannot compute and resolve nothing. Its
    edge steps are rewritten to select the secondary the member's key was
    demoted to, which is what the key it carries still finds; an endpoint a
    router role fills is pinned for the merged class only.

    A router produces every member of its side, so a resource may reference
    several; that is one reference when their keys were demoted to the same
    secondary, and refused when they were not. A resource that upserts the
    class is not touched here, nor one that finds it by a secondary identity
    (``find``): its edges follow that selector. Nor is a reference to a member that kept its
    key as the primary, or whose key was not demoted (``retire: keep``) -- the
    latter is logged, since its edges will not resolve.
    """
    ingestion = manifest.ingestion_model
    schema = manifest.graph_schema
    if ingestion is None or schema is None or not rekeyed:
        return []
    union_vertices = schema.core_schema.vertex_config.vertex_set

    pinned: list[PinnedReference] = []
    selectors_by_resource: dict[str, dict[str, str]] = {}
    for resource in ingestion.resources:
        for cluster in index.vertices:
            if cluster.into not in rekeyed:
                continue
            steps = _steps_producing(
                resource.pipeline, cluster.into, known_vertices=union_vertices
            )
            if (
                not steps
                or not all(step_looks_up(step, cluster.into) for step in steps)
                or any(step_finds(step, cluster.into) is not None for step in steps)
            ):
                continue
            origin = _members_produced(resource.name, cluster, sides)
            if origin is None or not origin[1]:
                continue
            side, members = origin
            keys = {demoted.get((side, member)) for member in members}
            named = sorted(key for key in keys if key is not None)
            if len(named) > 1:
                raise MergeIdentityError(
                    f"merge_manifests: resource {resource.name!r} references "
                    f"{side} members {members} of {cluster.into!r}, whose keys were "
                    f"demoted to different secondary identities {named}; its edges "
                    "cannot be pointed at one of them. Tag the members' "
                    "`local_key` alike (one key space), or reference each member "
                    "from its own resource.",
                    check="ambiguous reference",
                    subjects=(
                        subject("merged", cluster.into),
                        *(subject(side, member) for member in members),
                    ),
                )
            if None in keys:
                if cluster.declaration.retire == "keep":
                    logger.warning(
                        "merge_manifests: resource %r references %s:%s by its "
                        "pre-merge key, which `retire: keep` did not demote; its "
                        "edges to %r will not resolve",
                        resource.name,
                        side,
                        ", ".join(members),
                        cluster.into,
                    )
                continue
            selectors_by_resource.setdefault(resource.name, {})[cluster.into] = named[0]
            pinned.append(
                PinnedReference(
                    resource=resource.name,
                    vertex=cluster.into,
                    side=side,
                    members=tuple(members),
                    key=named[0],
                )
            )

    from .rewrite import rewrite_endpoint_selectors_in_pipeline

    def _pinning(selectors: dict[str, str]) -> Callable[[list[Any]], list[Any]]:
        return lambda pipeline: rewrite_endpoint_selectors_in_pipeline(
            pipeline, selectors
        )

    _rebuild_pipelines(
        manifest,
        {
            resource: _pinning(selectors)
            for resource, selectors in selectors_by_resource.items()
        },
    )
    return pinned


def _order_after_merge(
    manifest: GraphManifest,
    owners: Sequence[KeyOwner],
    attached: Sequence[AttachedProducer],
) -> list[OrderCycle]:
    """Run each resource after the resources whose nodes it needs.

    A resource attached to a merged class runs after the class's key owners;
    one whose every step producing a class only looks it up or finds it runs
    after every resource writing that class -- otherwise, in one ingest, it
    finds nothing. The declared order is moved only as far as that requires
    (:func:`~graflo.architecture.evolution.ordering.order_resources`). When
    the constraints form a cycle the declared order is kept, and each
    constraint it leaves unmet is returned.
    """
    from .ordering import order_resources

    ingestion = manifest.ingestion_model
    schema = manifest.graph_schema
    if ingestion is None or schema is None:
        return []
    known = schema.core_schema.vertex_config.vertex_set
    because: dict[tuple[str, str], set[str]] = {}
    for producer in attached:
        for owner in owners:
            if owner.vertex == producer.vertex:
                because.setdefault((owner.resource, producer.resource), set()).add(
                    producer.vertex
                )
    for vertex in sorted(known):
        writers: list[str] = []
        referrers: list[str] = []
        for resource in ingestion.resources:
            steps = _steps_producing(resource.pipeline, vertex, known_vertices=known)
            refers = [
                step_looks_up(step, vertex) or step_finds(step, vertex) is not None
                for step in steps
            ]
            if refers and all(refers):
                referrers.append(resource.name)
            elif refers:
                writers.append(resource.name)
        for writer in writers:
            for referrer in referrers:
                because.setdefault((writer, referrer), set()).add(vertex)

    declared = [resource.name for resource in ingestion.resources]
    order, unmet = order_resources(declared, because)
    if order != declared:
        by_name = {resource.name: resource for resource in ingestion.resources}
        ingestion.resources = [by_name[name] for name in order]
        manifest.ingestion_model = IngestionModel.model_validate(
            ingestion.to_dict(skip_defaults=False)
        )
    return [
        OrderCycle(resource=after, before=before, vertex=vertex)
        for before, after in unmet
        for vertex in sorted(because[(before, after)])
    ]


#: Separator :func:`_fold_name` joins two differing labels with.
_NAME_FOLD_SEP = "+"


def _fold_name(left: str | None, right: str | None) -> str | None:
    """The shared name when the two sides agree, ``left+right`` when they differ.

    The fold is flat: a side that is itself a fold contributes its parts, and
    a part already present is not repeated, so re-merging a source into a
    union keeps the union's name (``a+b`` with ``a`` stays ``a+b``) instead of
    growing on every round. The result is a label, not an identifier -- the
    namespace it deploys into is derived by
    :meth:`~graflo.architecture.schema.document.Schema.effective_namespace`.
    """
    if not (left and right) or left == right:
        return left or right
    parts: list[str] = []
    for part in (*left.split(_NAME_FOLD_SEP), *right.split(_NAME_FOLD_SEP)):
        if part not in parts:
            parts.append(part)
    return _NAME_FOLD_SEP.join(parts)


def _fold_version(left: str | None, right: str | None) -> str | None:
    """The higher of the two sides' versions, by ``MAJOR.MINOR.PATCH``.

    The merged schema supersedes both inputs, so the base a bump starts from
    is whichever side is further along; taking the left's alone would version
    a merge with a ``3.0.0`` right side below that side. Ties and unparsable
    versions keep the left's.
    """
    if left is None or right is None:
        return left if right is None else right
    left_key, right_key = semver_core(left), semver_core(right)
    if left_key is not None and right_key is not None and right_key > left_key:
        return right
    return left


def _fold_description(left: str | None, right: str | None) -> str | None:
    """Both descriptions, in side order — neither side's prose is authoritative."""
    if left and right and left != right:
        return f"{left}\n\n{right}"
    return left or right


def _merge_naming(
    left: NamingConvention | None, right: NamingConvention | None
) -> NamingConvention | None:
    """Keep a declared convention only while both sides declare the same one.

    Unlike the other descriptive blocks this one has consequences —
    :meth:`NamingConvention.rename_map` is computed from it — so asserting the
    left side's style over a union that also contains the right side's names
    would be a false claim about identifiers that are demonstrably not in it.
    Two conventions merge to no declared convention.
    """
    if left is None or right is None:
        source = left if right is None else right
        return source.model_copy(deep=True) if source is not None else None
    return left.model_copy(deep=True) if left == right else None


def _merge_graph_metadata(left: GraphMetadata, right: GraphMetadata) -> GraphMetadata:
    """Fold both sides' schema metadata into the merged schema's.

    Every descriptive field is folded rather than inherited from the left: the
    merged schema contains both sides' types, so describing it with only one
    side's prose, anchors and convention is wrong in the same way carrying only
    one side's vertices would be.

    ``version`` is the higher of the two (:func:`_fold_version`) because it is
    the base :func:`_bump_schema_version` bumps from. ``provenance`` is dropped because the merged schema is a new artifact
    with a new content address — stamping it is a commit point's job, not
    merge's.
    """
    return GraphMetadata(
        name=_fold_name(left.name, right.name) or left.name,
        version=_fold_version(left.version, right.version),
        description=_fold_description(left.description, right.description),
        semantics=merge_semantics(left.semantics, right.semantics),
        naming=_merge_naming(left.naming, right.naming),
        provenance=None,
    )


def _merge_manifest_metadata(
    left: ManifestMetadata | None, right: ManifestMetadata | None
) -> ManifestMetadata | None:
    """Fold both manifests' own name and description into the merged one.

    Manifest-level identity is separate from the schema's: a manifest carrying
    only bindings has no schema to borrow a name from. Provenance is dropped
    for the reason given in :func:`_merge_graph_metadata`, so a fold that would
    carry nothing but provenance yields no metadata block at all.
    """
    name = _fold_name(
        left.name if left else None,
        right.name if right else None,
    )
    description = _fold_description(
        left.description if left else None,
        right.description if right else None,
    )
    if name is None and description is None:
        return None
    return ManifestMetadata(name=name, description=description)


def _merge_declared_scalar(
    left: DatabaseProfile,
    right: DatabaseProfile,
    field: str,
) -> Any:
    """The declared value of a single-valued profile key, refusing two of them.

    Presence is read from ``skip_defaults=True`` rather than from the value:
    ``db_flavor`` defaults to Arango, so a value-based fold cannot tell a side
    that *declared* Arango from one that never spoke, and would let an
    undeclared left silently retarget a right that named its backend.
    """
    left_declared = left.to_dict(skip_defaults=True)
    right_declared = right.to_dict(skip_defaults=True)
    if field not in left_declared:
        return right_declared.get(field, getattr(left, field))
    if field not in right_declared:
        return left_declared[field]
    if left_declared[field] != right_declared[field]:
        hint = (
            " (set MergeManifestsOp.target_namespace to choose one)"
            if field == "target_namespace"
            else ""
        )
        raise ValueError(
            f"merge_manifests: conflicting {field}: "
            f"{left_declared[field]!r} vs {right_declared[field]!r}{hint}"
        )
    return left_declared[field]


def _merge_db_profiles(
    left: DatabaseProfile, right: DatabaseProfile
) -> DatabaseProfile:
    """Fold both sides' physical profile, electing neither.

    Every key is folded on its own terms. The single-valued ones
    (``db_flavor``, ``target_namespace``) refuse a declared disagreement rather
    than inheriting the left's, because both decide what DDL is emitted against
    which backend -- the merged manifest cannot target two.
    """
    data = left.to_dict(skip_defaults=False)
    right_data = right.to_dict(skip_defaults=False)

    data["db_flavor"] = _merge_declared_scalar(left, right, "db_flavor")
    data["target_namespace"] = _merge_declared_scalar(left, right, "target_namespace")

    vs = dict(data.get("vertex_storage_names") or {})
    for k, v in (right_data.get("vertex_storage_names") or {}).items():
        if k in vs and vs[k] != v:
            raise ValueError(
                f"merge_manifests: conflicting vertex_storage_names for {k!r}: "
                f"{vs[k]!r} vs {v!r}"
            )
        vs[k] = v
    data["vertex_storage_names"] = vs

    vi = {k: list(v) for k, v in (left.vertex_indexes or {}).items()}
    for k, indexes in (right.vertex_indexes or {}).items():
        merged_indexes = vi.setdefault(k, [])
        for index in indexes:
            append_index(merged_indexes, index, owner=f"vertex {k!r}")
    data["vertex_indexes"] = {
        k: [ix.to_dict(skip_defaults=False) for ix in v] for k, v in vi.items()
    }

    edge_specs = list(data.get("edge_specs") or [])
    edge_specs.extend(list(right_data.get("edge_specs") or []))
    data["edge_specs"] = edge_specs
    data["native_inverses"] = sorted(
        set(left.native_inverses) | set(right.native_inverses)
    )

    defaults = union_default_property_values(
        left.default_property_values, right.default_property_values
    )
    data["default_property_values"] = (
        None if defaults is None else defaults.to_dict(skip_defaults=False)
    )

    return _revalidate_db_profile(DatabaseProfile.model_validate(data))


def _union_schema(
    left: Schema,
    right: Schema,
    index: ClusterIndex,
    member_keys: dict[tuple[Side, str], tuple[str, ...] | None],
    member_property_names: dict[tuple[Side, str], set[str]],
) -> tuple[Schema, set[str], set[str]]:
    """Assemble both schemas by name, merging at every name they share.

    Both levels of the operation in one pass: the walk over names is the
    *union*, and ``merge_vertex_models`` / ``merge_edge_pair`` at a shared name
    is the *merge*. Cluster members have already arrived at their merged name
    by the time this runs, so a cluster reads here as an ordinary shared name.

    Returns the merged schema, the merged names whose ``identity`` is
    declared -- whose members' pre-merge keys :func:`_retire_member_keys`
    demotes once the manifest is complete -- and, among them, the ones with a
    derived or ``local_key`` branch, which :func:`_apply_derived_identities`
    lowers onto the assembled manifest.
    """
    left_vc = left.core_schema.vertex_config
    right_vc = right.core_schema.vertex_config
    left_by_name = {v.name: v for v in left_vc.vertices}
    right_by_name = {v.name: v for v in right_vc.vertices}

    out_vertices: list[Vertex] = []
    seen: set[str] = set()
    rekeyed: set[str] = set()
    derived: set[str] = set()

    for cluster in index.vertices:
        name = cluster.into
        if name not in left_by_name or name not in right_by_name:
            missing_side = "left" if name not in left_by_name else "right"
            raise ValueError(
                f"merge_manifests: merged vertex {name!r} missing on "
                f"{missing_side} after alignment (left={list(cluster.left)!r}, "
                f"right={list(cluster.right)!r})"
            )
        merged = merge_vertex_models(
            [left_by_name[name], right_by_name[name]],
            name,
            retype_remedy=MERGE_RETYPE_REMEDY,
        )
        identity, declared = _composed_identity(cluster, merged, member_keys)
        declaration = cluster.declaration
        assert isinstance(declaration, VertexEquivalence)
        if declared:
            rekeyed.add(name)
            if declaration.has_derivation or isinstance(identity, IdentityFunnel):
                _check_digest_field_free(
                    cluster,
                    declaration.digest_field,
                    member_keys,
                    member_property_names,
                )
            if declaration.has_derivation:
                derived.add(name)
            else:
                _check_identity_coverage(cluster, identity, member_property_names)
        merged = _apply_composed_identity(
            merged,
            identity,
            declared=declared and not declaration.has_derivation,
            digest_field=declaration.digest_field,
        )
        out_vertices.append(merged)
        seen.add(name)

    for v in left_vc.vertices:
        if v.name in seen:
            continue
        out_vertices.append(v)
        seen.add(v.name)

    for v in right_vc.vertices:
        if v.name in seen:
            if v.name in index.labels:
                continue  # a cluster member, merged above under its merged name
            # Anything else sharing a name with the union is a collision the
            # policy should have refused or prefixed. Skipping it would drop
            # its model silently.
            raise ValueError(
                f"merge_manifests: unreachable -- right vertex {v.name!r} "
                "shares a name with the union but no cluster merges it"
            )
        out_vertices.append(v)
        seen.add(v.name)

    force_types: dict[str, list] = dict(left_vc.force_types or {})
    for k, v in (right_vc.force_types or {}).items():
        if k in force_types and force_types[k] != v:
            raise ValueError(
                f"merge_manifests: conflicting force_types for vertex {k!r}"
            )
        force_types[k] = v

    by_id: dict[EdgeId, Edge] = {}
    for edge in list(left.core_schema.edge_config.edges) + list(
        right.core_schema.edge_config.edges
    ):
        eid = edge.edge_id
        if eid in by_id:
            by_id[eid] = merge_edge_pair(
                by_id[eid], edge, retype_remedy=MERGE_RETYPE_REMEDY
            )
        else:
            by_id[eid] = edge

    meta = _merge_graph_metadata(left.metadata, right.metadata)

    db_profile = _merge_db_profiles(left.db_profile, right.db_profile)
    inverses, symmetric = union_inverses(
        left.core_schema.edge_config, right.core_schema.edge_config
    )

    schema = Schema(
        metadata=meta,
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=out_vertices, force_types=force_types),
            edge_config=EdgeConfig(
                edges=list(by_id.values()),
                inverses=inverses,
                symmetric=symmetric,
            ),
        ),
        db_profile=db_profile,
    )
    return schema, rekeyed, derived


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
                    f"merge_manifests: incompatible transform definitions for {name!r}"
                )
            continue
        by_name[name] = t
        out.append(t)
    return out


def _concat_ingestion(
    left: IngestionModel | None, right: IngestionModel | None
) -> IngestionModel | None:
    """Concatenate both resource lists; union the transform registries by name.

    Not a union by name on the resources: colliding resource names were already
    resolved by ``_apply_right_resource_policy`` before the relabel, so nothing
    is left to fold and the lists simply join. ``transforms`` is a real by-name
    union -- see ``_union_transforms``.
    """
    if left is None and right is None:
        return None
    if left is None:
        return right.model_copy(deep=True) if right is not None else None
    if right is None:
        return left.model_copy(deep=True)

    resources = list(left.resources) + list(right.resources)
    transforms = _union_transforms(left, right)
    # Model-level write policies follow the left manifest, as merge treats
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
    name_conflict: Literal["error", "prefix_right", "union_right"],
) -> Bindings | None:
    """Assemble both bindings registries by name, renaming right-side collisions.

    Connectors are *addresses*, not concepts, so they collide only on an exact
    name and ``union_right`` is meaningless for them -- it behaves as ``error``.
    A renamed right connector is propagated into the references that name it.
    """
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
    right_names = [n for c in right_connectors if (n := _connector_name(c)) is not None]
    # Connectors are addresses, like resources: the same exact-match policy,
    # the same ordinal disambiguation, and ``union_right`` behaves as ``error``.
    rename_connectors = _resolve_name_collisions(
        left_names,
        right_names,
        name_conflict=name_conflict,
        kind="connector",
        hint="rename before merge",
    )

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
            "merge_manifests: canonical_maps entries must be (side, "
            f"CanonicalMap) pairs with side in ('left', 'right'); got {entry!r}"
        )
    return coerced


def merge_manifests(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp,
    *,
    bump_version: bool | Literal["minor"] = "minor",
    finish_init: bool = True,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> GraphManifest:
    """Return a new manifest that is the deterministic merge of *left* and *right*.

    The declared clusters (each a :class:`~graflo.architecture.evolution.ops.VertexEquivalence`
    or :class:`~graflo.architecture.evolution.ops.RelationEquivalence`, possibly
    n-ary) and ``op.canonical_maps`` are resolved together into one composite
    :class:`~graflo.architecture.evolution.ops.CanonicalizeOp` per side
    (see :func:`~graflo.architecture.evolution.canonical.resolve_clusters`),
    applied to that side in one step, before the two sides are unioned by
    name. Does not invent semantic matches: a name both sides carry and no
    cluster merges is refused under ``name_conflict="error"`` (naming the
    equivalences to declare), synthesized into a 1-1 cluster under
    ``union_right`` so it reconciles exactly as a declared one, and kept apart
    under ``prefix_right``.

    A vertex equivalence whose ``identity`` has a derived or ``local_key``
    branch further rewrites the merged union with the fundamental ops that
    identity lowers to (see
    :func:`~graflo.architecture.evolution.alignment.identity_to_ops`);
    member-keyed sources are resolved against the sides as handed in.
    *canonical_maps* — ``(side, CanonicalMap)`` pairs — are folded into
    ``op.canonical_maps``; putting the maps on the op itself keeps the whole
    recipe in one document.
    """
    manifest, _report = _merge_manifests(
        left,
        right,
        op,
        bump_version=bump_version,
        finish_init=finish_init,
        strict_references=strict_references,
        dynamic_edge_feedback=dynamic_edge_feedback,
        canonical_maps=canonical_maps,
    )
    return manifest


def merge_manifests_with_report(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp,
    *,
    bump_version: bool | Literal["minor"] = "minor",
    finish_init: bool = True,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> tuple[GraphManifest, MergeReport]:
    """:func:`merge_manifests`, and the :class:`MergeReport` of what it did.

    The report lists the key owners, the resources attached by their own key,
    the references pinned to a demoted key, the shared key spaces, the
    demoted keys and any resource order constraint a cycle left unmet.
    """
    return _merge_manifests(
        left,
        right,
        op,
        bump_version=bump_version,
        finish_init=finish_init,
        strict_references=strict_references,
        dynamic_edge_feedback=dynamic_edge_feedback,
        canonical_maps=canonical_maps,
    )


def _merge_manifests(
    left: GraphManifest,
    right: GraphManifest,
    op: MergeManifestsOp,
    *,
    bump_version: bool | Literal["minor"] = "minor",
    finish_init: bool = True,
    strict_references: bool = False,
    dynamic_edge_feedback: bool = False,
    canonical_maps: Sequence[tuple[Side, CanonicalMap]] = (),
) -> tuple[GraphManifest, MergeReport]:
    """:func:`merge_manifests`, and what it did to the sources on the way."""
    if not isinstance(op, MergeManifestsOp):
        raise TypeError(f"merge_manifests expects MergeManifestsOp, got {type(op)!r}")
    maps = _coerce_side_maps(canonical_maps)

    out_left = left.model_copy(deep=True)
    out_right = right.model_copy(deep=True)

    left_schema = _schema_of(out_left)
    right_schema = _schema_of(out_right)
    if op.target_namespace is not None:
        # The op's choice supersedes both sides' declarations, so a
        # disagreement between them is resolved rather than refused.
        for schema in (left_schema, right_schema):
            if schema is not None:
                schema.db_profile.target_namespace = None
    for side, schema in (("left", left_schema), ("right", right_schema)):
        if schema is None:
            logger.info(
                "merge_manifests: %s manifest carries no schema block; "
                "the merged schema comes from the other side alone",
                side,
            )

    # Every naming problem at once, as one MergeNamingError.
    resolution = resolve_clusters(
        op, left=out_left, right=out_right, canonical_maps=maps
    )
    index = resolution.index
    side_maps = resolution.side_maps
    # An origin names demoted keys and default local-key tags: refuse one
    # that cannot before anything is renamed by it.
    refusals = [
        *origin_refusals(op, {"left": out_left, "right": out_right}, resolution),
        *key_space_refusals(resolution, {"left": out_left, "right": out_right}),
    ]
    if refusals:
        raise refusals[0]

    # Declared merged types, lowered onto each side's own names, and every
    # clash no declaration settles -- both conflict points (one side's fold,
    # the union) at once, before either runs.
    union_names = UnionNames.of(
        {"left": out_left, "right": out_right},
        side_maps,
        index=index,
        name_conflict=op.name_conflict,
    )
    retypes = field_type_ops(
        op.field_types, {"left": out_left, "right": out_right}, union_names
    )
    clash = field_type_clashes(
        op.field_types, {"left": out_left, "right": out_right}, union_names
    )
    if clash is not None:
        raise clash

    member_keys, member_property_names = _capture_all_member_state(
        index, left_schema, right_schema, side_maps
    )
    member_identity = _member_identity_fields(
        index, left_schema, right_schema, side_maps, resolution.demoted_keys
    )

    _apply_resource_renames(out_left, op.renames.left.resources)
    left_resource_names: set[str] = set()
    if out_left.ingestion_model is not None:
        left_resource_names = {r.name for r in out_left.ingestion_model.resources}
    _apply_right_resource_policy(out_right, op, left_resource_names)

    # The sides as the derived identities see them: resources carry the names
    # the union will use, and every cluster member still exists as its own
    # class. The per-side lowering below merges the members in place, after
    # which no manifest can say which router key produced which member.
    sides = {
        "left": out_left.model_copy(deep=True),
        "right": out_right.model_copy(deep=True),
    }

    for manifest, side in ((out_left, "left"), (out_right, "right")):
        apply_manifest_ops_inplace(
            manifest, [*retypes[side], *canonicalize_ops(side_maps[side])]
        )

    _apply_right_schema_collision_policy(out_left, out_right, op, index)

    # Three-way, deliberately: a fabricated empty Schema would not be neutral.
    # Its `DatabaseProfile` declares nothing, but it would still fold a
    # fabricated label into the merged name, and a side that declared the
    # default flavor could not be told from one that never spoke.
    post_left = _schema_of(out_left)
    post_right = _schema_of(out_right)
    rekeyed: set[str] = set()
    derived: set[str] = set()
    composed_schema: Schema | None
    if post_left is not None and post_right is not None:
        composed_schema, rekeyed, derived = _union_schema(
            post_left,
            post_right,
            index,
            member_keys,
            member_property_names,
        )
    elif post_left is not None:
        composed_schema = post_left.model_copy(deep=True)
    elif post_right is not None:
        composed_schema = post_right.model_copy(deep=True)
    else:
        composed_schema = None
    if composed_schema is not None:
        _assert_no_canonical_split(composed_schema)
    composed_ingestion = _concat_ingestion(
        out_left.ingestion_model, out_right.ingestion_model
    )
    composed_bindings = _union_bindings(
        out_left.bindings, out_right.bindings, name_conflict=op.name_conflict
    )

    result = GraphManifest(
        graph_schema=composed_schema,
        ingestion_model=composed_ingestion,
        bindings=composed_bindings,
        metadata=_merge_manifest_metadata(left.metadata, right.metadata),
    )
    _bump_schema_version(result, bump_version)

    plans: list[tuple[Cluster, IdentityPlan]] = []
    if derived:
        result, plans = _apply_derived_identities(
            result,
            index=index,
            derived=derived,
            sides=sides,
            side_maps=side_maps,
            member_identity=member_identity,
            origins=resolution.origins,
            canonical_maps=[
                ("left", resolution.declared.left),
                ("right", resolution.declared.right),
            ],
            finish_init=False,
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )

    # Every re-keyed class now has its final identity: demote the members'
    # pre-merge keys against it, then point the resources that only reference
    # a member at the key they still carry.
    demoted = _retire_member_keys(
        result,
        index,
        member_keys,
        rekeyed,
        resolution.demoted_keys,
        resolution.origins,
        resolution.key_spaces,
    )
    shared_key_spaces = _shared_key_spaces(index, demoted)
    # Owners are read before any resource is attached: attaching marks only
    # resources no derived branch names, so the two never overlap.
    owners = _key_owners(result, plans, sides)
    attached = _attach_uncovered_producers(
        result, plans, sides, demoted, member_property_names
    )
    references = _pin_member_references(result, index, sides, demoted, rekeyed)
    if op.router_scope == "side":
        _close_side_routers(result, sides)
    # Last among the pipeline rewrites: the order is read from the steps as
    # attached, pinned and closed.
    order_cycles = _order_after_merge(result, owners, attached)

    _apply_merge_naming(result, op)

    if finish_init:
        result.finish_init(
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
    demotions = [
        DemotedKey(
            vertex=cluster.into,
            side=side,
            member=member,
            fields=tuple(member_keys.get((side, member)) or ()),
            secondary=demoted[(side, member)],
            branches=_completable_branches(
                result,
                cluster,
                side,
                member,
                member_keys.get((side, member)) or (),
                member_property_names.get((side, member), set()),
                sides,
            ),
        )
        for cluster in index.vertices
        for side in ("left", "right")
        for member in cluster.members(side)
        if (side, member) in demoted
    ]
    return result, MergeReport(
        demoted=demotions,
        owners=owners,
        attached=attached,
        references=references,
        shared_key_spaces=shared_key_spaces,
        order_cycles=order_cycles,
    )


def _completable_branches(
    manifest: GraphManifest,
    cluster: Cluster,
    side: Side,
    member: str,
    key: Sequence[str],
    carried: Collection[str],
    sides: Mapping[str, GraphManifest],
) -> tuple[str, ...]:
    """Funnel branches a member's records can complete, up to its own key's.

    A branch is completable when each field it requires is one the member
    carries or one a derived or ``local_key`` branch adds *for this member*:
    some resource producing it has an entry for the branch that applies to it.
    The walk stops at the branch over the member's own key: a record carrying
    the key always completes it, so nothing after it is reachable.
    """
    schema = manifest.graph_schema
    if (
        schema is None
        or cluster.into not in schema.core_schema.vertex_config.vertex_set
    ):
        return ()
    funnel = schema.core_schema.vertex_config[cluster.into].identity_funnel
    if funnel is None:
        return ()
    declaration = cluster.declaration
    derived: set[str] = set()
    if isinstance(declaration, VertexEquivalence):
        stepped: list[DerivedBranch | LocalKeyBranch] = [*declaration.derivations()]
        local_key = declaration.local_key_branch()
        if local_key is not None:
            stepped.append(local_key)
        derived = {
            branch.name
            for branch in stepped
            if _branch_reaches(branch, cluster, side, member, sides)
        }
    reachable = set(carried) | derived
    out: list[str] = []
    for branch in funnel.branches:
        required = set(branch.required_fields)
        if required <= reachable:
            out.append(branch.id)
        if required == set(key):
            break
    return tuple(out)


def _branch_reaches(
    branch: DerivedBranch | LocalKeyBranch,
    cluster: Cluster,
    side: Side,
    member: str,
    sides: Mapping[str, GraphManifest],
) -> bool:
    """Whether a resource producing *member* on *side* derives *branch* for it.

    An unkeyed entry applies to every member the resource produces; a
    member-keyed one only to the members it names, by own or canonical name.
    """
    for resource in branch.sources:
        produced = _members_produced(resource, cluster, sides)
        if produced is None or produced[0] != side or member not in produced[1]:
            continue
        keyed = branch.members_for(resource)
        if keyed is None or member in {cluster.resolved(side, k) for k in keyed}:
            return True
    return False


def _apply_merge_naming(manifest: GraphManifest, op: MergeManifestsOp) -> None:
    """Apply the op's explicit ``name`` / ``target_namespace`` to the merged manifest.

    ``name`` replaces the folded label on both the manifest and its schema.
    ``target_namespace`` is validated against the merged flavor here, at the
    merge, rather than at the first deploy that would reject it.
    """
    schema = manifest.graph_schema
    if op.name is not None:
        if manifest.metadata is None:
            manifest.metadata = ManifestMetadata(name=op.name)
        else:
            manifest.metadata.name = op.name
        if schema is not None:
            schema.metadata.name = op.name
    if op.target_namespace is not None:
        if schema is None:
            raise ValueError(
                "merge_manifests: target_namespace was given but neither side "
                "carries a schema to deploy"
            )
        validate_namespace(op.target_namespace, schema.db_profile.db_flavor)
        schema.db_profile.target_namespace = op.target_namespace


def _apply_derived_identities(
    manifest: GraphManifest,
    *,
    index: ClusterIndex,
    derived: Collection[str],
    sides: Mapping[str, GraphManifest],
    side_maps: SideMaps,
    member_identity: Mapping[str, set[str]],
    origins: Mapping[Side, str],
    canonical_maps: Sequence[tuple[Side, CanonicalMap]],
    finish_init: bool,
    strict_references: bool,
    dynamic_edge_feedback: bool,
) -> tuple[GraphManifest, list[tuple[Cluster, IdentityPlan]]]:
    """Lower each derived identity onto the union, and return the plans lowered.

    *sides* are the manifests as handed in, after the resource rename policy
    and before the per-side relabel: member-keyed sources resolve against
    them, since the relabel rewrites router ``type_map`` values to the
    merged name. Member keys are first re-keyed through the cluster, so a
    member may be keyed by its own name or its canonical one. A ``local_key``
    source with no tag takes its side's entry in *origins*.
    """
    from .alignment import IdentityPlan, identity_to_ops, rekey_members
    from .apply import apply_evolution

    all_maps: list[CanonicalMap | CanonicalizeOp] = [cm for _side, cm in canonical_maps]
    all_maps.extend((side_maps.left, side_maps.right))
    out = manifest
    plans: list[tuple[Cluster, IdentityPlan]] = []
    for cluster in index.vertices:
        if cluster.into not in derived:
            continue
        declaration = cluster.declaration
        assert isinstance(declaration, VertexEquivalence)
        assert declaration.identity is not None
        plan = rekey_members(
            IdentityPlan(
                vertex=cluster.into,
                branches=tuple(declaration.identity),
                at=dict(declaration.derive_at),
                digest_field=declaration.digest_field,
                derive=tuple(declaration.derive_attributes()),
            ),
            sides=sides,
            resolve=cluster.resolved,
        )
        ops = identity_to_ops(
            plan,
            manifest=out,
            canonical_maps=all_maps,
            sides=sides,
            cluster_members={"left": set(cluster.left), "right": set(cluster.right)},
            member_identity=member_identity.get(cluster.into),
            origins=origins,
            # Attached by their own key once the member keys are demoted:
            # see _attach_uncovered_producers.
            uncovered_producers="allow",
        )
        out = apply_evolution(
            out,
            ops,
            bump_version=False,
            finish_init=finish_init,
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
        )
        plans.append((cluster, plan))
    return out, plans
