"""Merged property types: declared on a merge, refused when they disagree undeclared.

A merge folds every member of a group into one class, and every edge that
lands on one ``edge_id`` into one edge, so their property types must agree.
:attr:`~graflo.architecture.evolution.ops.MergeManifestsOp.field_types` states
the merged type once, by merged names. This module lowers it per side onto the
members' own names (a ``change_field_types`` applied ahead of the side's
``canonicalize``), and reports every undeclared disagreement at once, naming
the declarations that carry each type.

Merge, the merge commit and the preview all call it, so the three cannot
disagree on what a declaration retypes.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.refusal import Refusal
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import (
    Field,
    FieldMergeConflict,
    FieldMergeError,
    FieldType,
    format_field_type_label,
)

from .canonical import SideNames
from .equivalence import ClusterIndex, Kind, Side
from .ops import (
    CanonicalizeOp,
    ChangeFieldTypesOp,
    FieldTypeSpec,
    ManifestOp,
    MergeFieldTypes,
)

_SIDES: tuple[Side, Side] = ("left", "right")

#: The fix a type clash suggests once the clash is past the attributed check.
MERGE_RETYPE_REMEDY = "Declare the merged type in the merge's field_types."


class FieldTypeDeclarationError(Refusal):
    """A ``field_types`` entry that names no merged class, relation or property."""


class Composite(Protocol):
    """The composite relabel per side (``SideMaps``, or a dict of both sides)."""

    def __getitem__(self, side: Side, /) -> CanonicalizeOp: ...


# ── where names end up ───────────────────────────────────────────────────────


def union_name_map(
    composite: Composite,
    names: Mapping[Side, SideNames],
    *,
    index: ClusterIndex,
    name_conflict: str,
    side: Side,
    kind: Kind,
) -> dict[str, str]:
    """Where a name on *side* ends up, as the union sees it.

    Merge applies the composite relabel first and the right side's
    name-conflict policy second, so the map the union sees is the policy
    merged onto the relabel. The policy comes from the function merge calls,
    over the post-relabel names, so ``prefix_right`` cannot drift out of sync.
    Under ``error`` and ``union_right`` the policy is empty by construction;
    a refusal it raises is merge's to report, so it reads as no renames here.
    """
    from .merge import _resolve_schema_collisions

    own = _kind_mapping(composite[side], kind)
    if side != "right":
        return dict(own)

    left_own = _kind_mapping(composite["left"], kind)
    left_after = {left_own.get(name, name) for name in names["left"].of_kind(kind)}
    right_after = sorted({own.get(name, name) for name in names[side].of_kind(kind)})
    try:
        policy = _resolve_schema_collisions(
            left_names=left_after,
            right_names=right_after,
            exempt=index.labels if kind == "vertex" else index.relation_labels,
            name_conflict=name_conflict,
            kind=kind,
            equivalence_hint=(
                "VertexEquivalence" if kind == "vertex" else "RelationEquivalence"
            ),
        )
    except ValueError:
        policy = {}
    if not policy:
        return dict(own)
    out = {name: policy.get(target, target) for name, target in own.items()}
    for name in names[side].of_kind(kind):
        if name not in out and name in policy:
            out[name] = policy[name]
    return out


def _kind_mapping(relabel: CanonicalizeOp, kind: Kind) -> dict[str, str]:
    return relabel.vertices if kind == "vertex" else relabel.relations


@dataclass(frozen=True)
class UnionNames:
    """Each side's class and relation names, and the property renames per class."""

    maps: Mapping[tuple[Side, Kind], Mapping[str, str]]
    properties: Mapping[Side, Mapping[str, Mapping[str, str]]]

    @classmethod
    def of(
        cls,
        manifests: Mapping[Side, GraphManifest],
        composite: Composite,
        *,
        index: ClusterIndex,
        name_conflict: str,
    ) -> UnionNames:
        names = {side: SideNames.of(manifests[side]) for side in _SIDES}
        kinds: tuple[Kind, Kind] = ("vertex", "relation")
        return cls(
            maps={
                (side, kind): union_name_map(
                    composite,
                    names,
                    index=index,
                    name_conflict=name_conflict,
                    side=side,
                    kind=kind,
                )
                for side in _SIDES
                for kind in kinds
            },
            properties={side: composite[side].properties for side in _SIDES},
        )

    def name(self, side: Side, kind: Kind, own: str) -> str:
        return self.maps[(side, kind)].get(own, own)

    def prop(self, side: Side, vertex: str, own: str) -> str:
        return self.properties[side].get(vertex, {}).get(own, own)

    def edge_id(self, side: Side, edge: Edge) -> tuple[str, str, str | None]:
        return (
            self.name(side, "vertex", edge.source),
            self.name(side, "vertex", edge.target),
            self.name(side, "relation", edge.relation) if edge.relation else None,
        )


# ── lowering a declaration ───────────────────────────────────────────────────


def field_type_ops(
    declared: MergeFieldTypes | None,
    manifests: Mapping[Side, GraphManifest],
    union: UnionNames,
) -> dict[Side, list[ManifestOp]]:
    """The retype each side needs ahead of its ``canonicalize``, by its own names.

    Every member whose merged name is declared, and that carries a declared
    property under any spelling renamed onto it, is retyped; a member that
    already has the type, or does not carry the property, is left alone.

    Raises:
        FieldTypeDeclarationError: An entry no member on either side reaches,
            all listed at once.
    """
    out: dict[Side, list[ManifestOp]] = {side: [] for side in _SIDES}
    if declared is None:
        return out
    seen: set[tuple[Kind, str]] = set()
    reached: set[tuple[Kind, str, str]] = set()

    for side in _SIDES:
        schema = manifests[side].graph_schema
        if schema is None:
            continue
        vertices: dict[str, dict[str, FieldTypeSpec]] = {}
        for vertex in schema.core_schema.vertex_config.vertices:
            merged = union.name(side, "vertex", vertex.name)
            specs = declared.vertices.get(merged)
            if specs is None:
                continue
            seen.add(("vertex", merged))
            for prop in vertex.properties:
                target = union.prop(side, vertex.name, prop.name)
                spec = specs.get(target)
                if spec is None:
                    continue
                reached.add(("vertex", merged, target))
                if not _has_type(prop, spec):
                    vertices.setdefault(vertex.name, {})[prop.name] = spec

        edges: dict[str, dict[str, FieldTypeSpec]] = {}
        for edge in schema.core_schema.edge_config.edges:
            if edge.relation is None:
                continue
            merged = union.name(side, "relation", edge.relation)
            specs = declared.edges.get(merged)
            if specs is None:
                continue
            seen.add(("relation", merged))
            for prop in edge.properties:
                spec = specs.get(prop.name)
                if spec is None:
                    continue
                reached.add(("relation", merged, prop.name))
                if not _has_type(prop, spec):
                    edges.setdefault(edge.relation, {})[prop.name] = spec

        if vertices or edges:
            out[side].append(ChangeFieldTypesOp(vertices=vertices, edges=edges))

    misses = [
        *_misses("vertex", declared.vertices, seen, reached),
        *_misses("relation", declared.edges, seen, reached),
    ]
    if misses:
        raise FieldTypeDeclarationError(
            "merge_manifests: field_types names nothing to retype: "
            + "; ".join(misses),
            check="field type declaration",
        )
    return out


def _has_type(prop: Field, spec: FieldTypeSpec) -> bool:
    return (prop.type, prop.item_type) == (spec.type, spec.item_type)


def _misses(
    kind: Kind,
    declared: Mapping[str, Mapping[str, FieldTypeSpec]],
    seen: set[tuple[Kind, str]],
    reached: set[tuple[Kind, str, str]],
) -> list[str]:
    out: list[str] = []
    for name, specs in declared.items():
        if (kind, name) not in seen:
            out.append(f"no {kind} of either side merges into {name!r}")
            continue
        out.extend(
            f"no member of {kind} {name!r} carries property {prop!r}"
            for prop in specs
            if (kind, name, prop) not in reached
        )
    return out


def with_declared_types(
    fields: Iterable[Field], specs: Mapping[str, FieldTypeSpec] | None
) -> list[Field]:
    """*fields*, already under their merged names, with the declared types applied."""
    if not specs:
        return list(fields)
    return [
        field.model_copy(
            update={
                "type": specs[field.name].type,
                "item_type": specs[field.name].item_type,
            }
        )
        if field.name in specs
        else field
        for field in fields
    ]


# ── refusing an undeclared clash ─────────────────────────────────────────────


@dataclass
class _Clash:
    """Every typed declaration of one merged property, grouped by type."""

    owner: str
    remedy_path: tuple[str, str] | None  # ("vertices" | "edges", merged name)
    by_label: dict[str, list[str]]
    fields: dict[str, Field]


def field_type_clashes(
    declared: MergeFieldTypes | None,
    manifests: Mapping[Side, GraphManifest],
    union: UnionNames,
) -> FieldMergeError | None:
    """Every merged property whose members disagree on a type no declaration settles.

    Groups each typed property of each side by the merged class (or merged
    ``edge_id``) and merged property it lands on, so a disagreement among one
    side's members and one between the two sides are reported alike, with the
    declarations carrying each type. Untyped properties take the other side's
    type and never clash; units are left to the fold.
    """
    groups: dict[tuple[str, str], _Clash] = {}
    vertex_specs = declared.vertices if declared is not None else {}
    edge_specs = declared.edges if declared is not None else {}

    for side in _SIDES:
        schema = manifests[side].graph_schema
        if schema is None:
            continue
        for vertex in schema.core_schema.vertex_config.vertices:
            merged = union.name(side, "vertex", vertex.name)
            for prop in vertex.properties:
                target = union.prop(side, vertex.name, prop.name)
                if prop.type is None or target in vertex_specs.get(merged, {}):
                    continue
                clash = groups.setdefault(
                    (f"vertex {merged!r}", target),
                    _Clash(f"vertex {merged!r}", ("vertices", merged), {}, {}),
                )
                label = format_field_type_label(prop)
                clash.by_label.setdefault(label, []).append(
                    f"{side} {vertex.name}.{prop.name}"
                )
                clash.fields.setdefault(label, prop)
        for edge in schema.core_schema.edge_config.edges:
            eid = union.edge_id(side, edge)
            relation = eid[2]
            for prop in edge.properties:
                if prop.type is None or (
                    relation is not None and prop.name in edge_specs.get(relation, {})
                ):
                    continue
                clash = groups.setdefault(
                    (f"edge {eid!r}", prop.name),
                    _Clash(
                        f"edge {eid!r}",
                        ("edges", relation) if relation is not None else None,
                        {},
                        {},
                    ),
                )
                label = format_field_type_label(prop)
                clash.by_label.setdefault(label, []).append(
                    f"{side} {_edge_label(edge)}.{prop.name}"
                )
                clash.fields.setdefault(label, prop)

    conflicts: list[FieldMergeConflict] = []
    blocks: list[str] = []
    for (_owner, prop_name), clash in groups.items():
        if len(clash.by_label) < 2:
            continue
        reason = f"property {prop_name!r}: " + " vs ".join(
            repr(label) for label in clash.by_label
        )
        remedy = _remedy(clash, prop_name)
        conflicts.append(
            FieldMergeConflict(
                clash.owner, reason, remedy, field=prop_name, origins=clash.by_label
            )
        )
        listed = "\n".join(
            f"  {label!r}: {', '.join(origins)}"
            for label, origins in clash.by_label.items()
        )
        blocks.append(
            f"Conflicting field types for {clash.owner}, {reason}\n{listed}\n{remedy}"
        )
    if not conflicts:
        return None
    return FieldMergeError(
        "\n".join(blocks),
        owner="; ".join(dict.fromkeys(c.owner for c in conflicts)),
        conflicts=tuple(conflicts),
    )


def _edge_label(edge: Edge) -> str:
    if edge.relation is None:
        return f"{edge.source}->{edge.target}"
    return f"{edge.source}-{edge.relation}->{edge.target}"


def _remedy(clash: _Clash, prop_name: str) -> str:
    if clash.remedy_path is None:
        return "Give the edge a relation to declare its merged type in field_types."
    block, name = clash.remedy_path
    fields = list(clash.fields.values())
    if all(f.type == FieldType.LIST for f in fields):
        items = "|".join(dict.fromkeys(str(f.item_type) for f in fields))
        spec = f"{{type: LIST, item_type: {items}}}"
    else:
        spec = f"{{type: {'|'.join(dict.fromkeys(str(f.type) for f in fields))}}}"
    return (
        "Declare the merged type on the merge: "
        f"field_types: {{{block}: {{{name}: {{{prop_name}: {spec}}}}}}}"
    )
