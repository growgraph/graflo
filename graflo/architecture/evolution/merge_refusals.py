"""The structural refusals of a merge, read from the sides before the union.

Pure: each reads the sides and their
:class:`~graflo.architecture.evolution.canonical.ClusterResolution` and
returns its refusals, so the preview and the merge refuse alike.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

from graflo.architecture.contract.ingestion.resource import (
    _steps_producing,
    step_finds,
    step_looks_up,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.vertex import SecondaryIdentity
from graflo.onto import DBType

from .canonical import ClusterResolution, SideMaps
from .db_profile import _merge_declared_scalar
from .equivalence import Cluster, ClusterIndex, Side, subject
from .merge_errors import MergeIdentityError
from .naming_graph import key_space_name_problem
from .ops import MergeManifestsOp, VertexEquivalence


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
        if member not in vertex_config.vertex_set:
            continue  # a naming problem the resolution reports
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


@dataclass(frozen=True)
class DemotedKeyPlan:
    """How one merged class names its members' demoted keys.

    ``names`` is the secondary identity each demoted member's key answers to,
    by ``(side, member)``; ``additions`` the secondaries to declare for them,
    in order; ``refusals`` what makes the naming impossible.
    """

    names: dict[tuple[Side, str], str]
    additions: list[SecondaryIdentity]
    refusals: list[MergeIdentityError]


def plan_demoted_key_names(
    into: str,
    members: Sequence[tuple[Side, str, tuple[str, ...], tuple[str, ...]]],
    key_spaces: Mapping[tuple[Side, str], str],
    origins: Mapping[Side, str],
    authored: Mapping[str, Collection[str]],
) -> DemotedKeyPlan:
    """Name the demoted keys of the merged class *into*; pure.

    *members* are its demoted members in order, as ``(side, member, fields,
    key)``: *fields* in the union's names (after the key-space prefix), *key*
    the canonical fields before it. A member's key space is its entry in
    *key_spaces*, else its side's origin. *authored* are the secondaries the
    members declare, by name, in the union's field names.

    A field set *authored* declares answers to that declaration and its name.
    Otherwise the secondary is named by its key space, one per key space; a
    key space contributing two field sets names each ``<space>__<key fields
    joined by __>``. A field set is shared by the members of one key space
    and by members of different sides; the first member to declare it fixes
    its column order. Refused: two key spaces of one side on one field set,
    authored or not -- their ids would share a lookup column -- and a
    ``<space>__<fields>`` name an authored secondary takes.
    A key space named like an authored secondary is
    :func:`key_space_refusals`' and :func:`origin_refusals`' to report.
    """
    authored_by_fields = {frozenset(fields): name for name, fields in authored.items()}

    def space(side: Side, member: str) -> str:
        return key_spaces.get((side, member), origins[side])

    refusals: list[MergeIdentityError] = []
    claims: dict[tuple[Side, frozenset[str]], dict[str, list[str]]] = {}
    field_sets: dict[str, set[frozenset[str]]] = {}
    for side, member, fields, _key in members:
        claims.setdefault((side, frozenset(fields)), {}).setdefault(
            space(side, member), []
        ).append(member)
        if frozenset(fields) not in authored_by_fields:
            field_sets.setdefault(space(side, member), set()).add(frozenset(fields))
    for (side, fields), by_space in claims.items():
        if len(by_space) < 2:
            continue
        detail = ", ".join(
            f"{side}:{member} ({name!r})"
            for name, claimants in by_space.items()
            for member in claimants
        )
        refusals.append(
            MergeIdentityError(
                f"merge_manifests: key space: {detail} demote the key "
                f"{sorted(fields)} onto {into!r} from different key spaces, so "
                "their ids would share one lookup column and an id of one would "
                "find a node of another; tag the members' `local_key` alike "
                "(one key space), or do not list the key as a property branch "
                "of `identity`",
                check="key space",
                subjects=(subject("merged", into),),
            )
        )

    by_fields = dict(authored_by_fields)
    names: dict[tuple[Side, str], str] = {}
    additions: list[SecondaryIdentity] = []
    for side, member, fields, key in members:
        if frozenset(fields) not in by_fields:
            name = space(side, member)
            if len(field_sets[name]) > 1:
                name = "__".join((name, *key))
                if name in authored:
                    refusals.append(
                        MergeIdentityError(
                            f"merge_manifests: key space: the key {side}:{member} "
                            f"demotes on {into!r} would be the secondary identity "
                            f"{name!r}, which a member already declares over "
                            f"{sorted(authored[name])}; rename that secondary, or "
                            f"set `origins.{side}` on the op, or tag the member's "
                            "`local_key` otherwise",
                            check="key space",
                            subjects=(subject("merged", into),),
                        )
                    )
            entry = SecondaryIdentity(name=name, fields=list(fields))
            by_fields[frozenset(fields)] = name
            additions.append(entry)
        names[(side, member)] = by_fields[frozenset(fields)]
    return DemotedKeyPlan(names=names, additions=additions, refusals=refusals)


def _demoted_key_plans(
    resolution: ClusterResolution, manifests: Mapping[Side, GraphManifest]
) -> list[DemotedKeyPlan]:
    """:func:`plan_demoted_key_names` for every class, read from the sides before the union.

    *manifests* are the sides as *resolution* was read from them; member keys
    and authored secondaries are taken in the union's names, as the merge
    takes them from the merged class.
    """
    left = manifests["left"].graph_schema
    right = manifests["right"].graph_schema
    member_keys, _names = _capture_all_member_state(
        resolution.index, left, right, resolution.side_maps
    )
    plans: list[DemotedKeyPlan] = []
    for cluster in resolution.index.vertices:
        members: list[tuple[Side, str, tuple[str, ...], tuple[str, ...]]] = []
        authored: dict[str, tuple[str, ...]] = {}
        for side, schema in (("left", left), ("right", right)):
            if schema is None:
                continue
            vertex_config = schema.core_schema.vertex_config
            renames = resolution.side_maps[side].properties
            for member in cluster.members(side):
                key = resolution.demoted_keys.get((side, member))
                fields = member_keys.get((side, member))
                if key is not None and fields:
                    members.append((side, member, fields, key))
                if member not in vertex_config.vertex_set:
                    continue
                rename = renames.get(member, {})
                for entry in vertex_config[member].secondary_identities:
                    if entry.name is not None:
                        authored.setdefault(
                            entry.name, tuple(rename.get(f, f) for f in entry.fields)
                        )
        if members:
            plans.append(
                plan_demoted_key_names(
                    cluster.into,
                    members,
                    resolution.key_spaces,
                    resolution.origins,
                    authored,
                )
            )
    return plans


def demoted_key_refusals(
    resolution: ClusterResolution, manifests: Mapping[Side, GraphManifest]
) -> list[MergeIdentityError]:
    """Every demoted key :func:`plan_demoted_key_names` cannot name.

    Read from the sides before the union, so the preview and the merge refuse
    alike before anything is renamed.
    """
    return [
        refusal
        for plan in _demoted_key_plans(resolution, manifests)
        for refusal in plan.refusals
    ]


#: The longest identifier PostgreSQL keeps, in bytes (``NAMEDATALEN - 1``);
#: a longer one is truncated without an error.
_PG_IDENTIFIER_BYTES = 63


def _union_db_flavor(manifests: Mapping[Side, GraphManifest]) -> DBType | None:
    """The backend the union targets, as :func:`_merge_db_profiles` folds it.

    ``None`` when no side has a schema, or the sides declare two backends --
    a disagreement the union refuses itself.
    """
    left = manifests["left"].graph_schema
    right = manifests["right"].graph_schema
    if left is not None and right is not None:
        try:
            return _merge_declared_scalar(
                left.db_profile, right.db_profile, "db_flavor"
            )
        except ValueError:
            return None
    schema = left if left is not None else right
    return schema.db_profile.db_flavor if schema is not None else None


def identifier_refusals(
    resolution: ClusterResolution, manifests: Mapping[Side, GraphManifest]
) -> list[MergeIdentityError]:
    """Every demoted-key property name a PostgreSQL target would truncate.

    A demoted key field is renamed ``<key space>__<field>``; PostgreSQL keeps
    only the first 63 bytes of a column name, so two such names could
    become one column. Checked only when the union targets PostgreSQL. Read
    from the sides before the union, so the preview and the merge refuse
    alike.
    """
    if _union_db_flavor(manifests) != DBType.POSTGRES:
        return []
    into_of = {
        (side, member): cluster.into
        for cluster in resolution.index.vertices
        for side in ("left", "right")
        for member in cluster.members(side)
    }
    out: list[MergeIdentityError] = []
    for (side, member), space in sorted(resolution.key_spaces.items()):
        renames = resolution.side_maps[side].properties.get(member, {})
        for own, name in renames.items():
            size = len(name.encode("utf-8"))
            if name != f"{space}__{own}" or size <= _PG_IDENTIFIER_BYTES:
                continue
            into = into_of.get((side, member), member)
            tag = (
                f"`origins.{side}`"
                if space == resolution.origins[side]
                else "the member's `local_key` tag"
            )
            out.append(
                MergeIdentityError(
                    f"merge_manifests: identifier: the key {side}:{member}.{own} "
                    f"demotes on {into!r} would be the property {name!r}, "
                    f"{size} bytes, but PostgreSQL keeps only "
                    f"{_PG_IDENTIFIER_BYTES}; set a shorter {tag}",
                    check="identifier",
                    subjects=(subject("merged", into),),
                )
            )
    return out


def origin_refusals(
    op: MergeManifestsOp,
    manifests: Mapping[Side, GraphManifest],
    resolution: ClusterResolution,
) -> list[MergeIdentityError]:
    """Every problem with the origins *resolution* names something by.

    Only a side whose origin names something is checked: a demoted key whose
    key space is the origin, or a local key the origin tags (see
    :attr:`~graflo.architecture.evolution.canonical.ClusterResolution.origin_tagged`).
    Its origin must be a valid key space name (see
    :func:`~graflo.architecture.evolution.naming_graph.key_space_name_problem`;
    it is joined to field names with ``__``); two named origins must differ;
    and no member of a re-keyed cluster may declare a secondary identity of
    that name, which the demoted key takes. Each refusal is about the merged
    classes that name something by the origin, or that declare the secondary.
    """
    sides: tuple[Side, ...] = ("left", "right")
    origins = resolution.origins
    into_of = {
        (side, member): cluster.into
        for cluster in resolution.index.vertices
        for side in sides
        for member in cluster.members(side)
    }
    demoting: dict[Side, set[str]] = {side: set() for side in sides}
    for (side, member), space in resolution.key_spaces.items():
        into = into_of.get((side, member))
        if into is not None and space == origins[side]:
            demoting[side].add(into)
    tagged = {side: set(resolution.origin_tagged.get(side, ())) for side in sides}
    used = [side for side in sides if demoting[side] or tagged[side]]
    if not used:
        return []
    rekeyed = [
        cluster
        for cluster in resolution.index.vertices
        if isinstance(cluster.declaration, VertexEquivalence)
        and cluster.declaration.identity is not None
    ]
    declared = op.origins or {}

    def source(side: Side) -> str:
        if side in declared:
            return f"`origins.{side}`"
        return f"the {side} schema name"

    def names(side: Side) -> str:
        demoted = f"the {side} side's demoted keys"
        untagged = "local keys the op leaves untagged"
        if demoting[side] and tagged[side]:
            return f"names {demoted} and tags its {untagged}"
        if demoting[side]:
            return f"names {demoted}"
        return f"tags the {side} side's {untagged}"

    def classes(*intos: str) -> tuple[str, ...]:
        """The merged classes *intos*, in the index's order."""
        return tuple(
            subject("merged", cluster.into)
            for cluster in resolution.index.vertices
            if cluster.into in intos
        )

    out: list[MergeIdentityError] = []
    for side in used:
        origin = origins[side]
        problem = key_space_name_problem(origin)
        if problem is not None:
            out.append(
                MergeIdentityError(
                    f"merge_manifests: origin {origin!r} ({source(side)}) "
                    f"{names(side)}, but {problem}; set `origins.{side}` on the op",
                    check="origin",
                    subjects=classes(*demoting[side], *tagged[side]),
                )
            )
        authors = sorted(
            (f"{member_side}:{member}", cluster.into)
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
                    f"merge_manifests: origin {origin!r} ({source(side)}) "
                    f"{names(side)}, but "
                    f"{', '.join(author for author, _into in authors)} declare a "
                    f"secondary identity named {origin!r}; set `origins.{side}` "
                    "on the op",
                    check="origin",
                    subjects=classes(*(into for _author, into in authors)),
                )
            )
    if len(used) == 2 and origins["left"] == origins["right"]:
        out.append(
            MergeIdentityError(
                f"merge_manifests: both sides' origin is {origins['left']!r}, "
                "so the keys it names or tags on each side would share one "
                "name; set `origins` on the op",
                check="origin",
                subjects=classes(
                    *(into for side in sides for into in demoting[side] | tagged[side])
                ),
            )
        )
    return out


def key_space_refusals(
    resolution: ClusterResolution, manifests: Mapping[Side, GraphManifest]
) -> list[MergeIdentityError]:
    """Every problem with the key spaces *resolution* names demoted keys by.

    A ``local_key`` tag names the key space of a member's own ids, and its
    demoted key is named by it. Refused: a member two of its resources tag
    differently (its ids would be in two spaces at once), a tag that cannot
    name a key space (see :func:`~graflo.architecture.evolution.naming_graph.key_space_name_problem`),
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
        elif tags[0] != origins[side] and (problem := key_space_name_problem(tags[0])):
            out.append(
                MergeIdentityError(
                    f"merge_manifests: key space: {side}:{member} is tagged "
                    f"{tags[0]!r}, which names its demoted key but {problem}; "
                    "give the member another tag",
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
    names = {
        member: name
        for plan in _demoted_key_plans(
            resolution, {"left": sides["left"], "right": sides["right"]}
        )
        for member, name in plan.names.items()
    }
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
                    step_finds(step, member, known_vertices=known) is not None
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
