"""Two versions of a schema, side by side.

:func:`compare_schemas` lines up the vertex types, relations and fields of two
manifests as rows -- one row per element, renamed or not -- with each side's
values. Given their common ancestor, rows are anchored on it and each side's
recorded renames (:class:`RenameHints`, ancestor → side) say where an ancestor
element went; without one, rows pair by name, then by canonical name.

Every row carries the slots :func:`merge_three_way` addresses it by, so
:meth:`SchemaComparison.locate` finds the row a merge conflict is about.

A derived view of the schema, not part of the contract: nothing here is hashed
or stored.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.naming import canonical_key
from graflo.architecture.schema.vertex import Field, Vertex

from .autogenerate import RenameHints, _edges, _identity_target, _vertices
from .canonicalize import canonical_payload
from .merge3 import (
    Slot,
    _covers,
    _edge_slot,
    _field_slot,
    _relation_slot,
    _vertex_slot,
    describe_slot,
)

CompareStatus = Literal["unchanged", "added", "removed", "renamed", "changed"]
"""A row read left → right: ``added`` is only on the right, ``removed`` only on the left."""

Side = Literal["left", "right"]


class FieldSide(ConfigBaseModel):
    """One side of a field row."""

    name: str
    type: str | None = PydanticField(
        default=None, description='``"INT"``, ``"LIST[STRING]"``; None when untyped.'
    )
    description: str | None = None
    in_identity: bool = PydanticField(
        default=False, description="Whether the field is part of the primary key."
    )


class FieldRow(ConfigBaseModel):
    """A field (vertex property or edge property) on both sides."""

    id: str
    left: FieldSide | None = None
    right: FieldSide | None = None
    status: CompareStatus
    aspects: list[str] = PydanticField(
        default_factory=list,
        description="What differs between the sides: name, type, description, identity.",
    )
    changed_on: list[Side] = PydanticField(
        default_factory=list,
        description="The sides that changed it since the common ancestor.",
    )
    slots: list[list[str]] = PydanticField(default_factory=list)


class VertexSide(ConfigBaseModel):
    """One side of a vertex row."""

    name: str
    description: str | None = None
    identity: list[str] = PydanticField(default_factory=list)
    identity_mode: str = PydanticField(
        description="natural, hash, funnel, assigned or blank."
    )
    secondary_identities: list[list[str]] = PydanticField(default_factory=list)


class VertexRow(ConfigBaseModel):
    """A vertex type on both sides, with its fields."""

    id: str
    left: VertexSide | None = None
    right: VertexSide | None = None
    status: CompareStatus
    aspects: list[str] = PydanticField(
        default_factory=list,
        description="What differs: name, description, identity, secondary, fields.",
    )
    fields: list[FieldRow] = PydanticField(default_factory=list)
    changed_on: list[Side] = PydanticField(default_factory=list)
    slots: list[list[str]] = PydanticField(default_factory=list)


class RelationSide(ConfigBaseModel):
    """One side of a relation row: one edge."""

    relation: str | None = None
    source: str
    target: str
    source_row: str = PydanticField(description="The id of the source's vertex row.")
    target_row: str = PydanticField(description="The id of the target's vertex row.")
    description: str | None = None
    directed: bool = True
    identities: list[list[str]] = PydanticField(default_factory=list)


class RelationRow(ConfigBaseModel):
    """An edge on both sides, with its properties.

    Rows are keyed by their endpoint rows, so an edge whose endpoint was renamed
    stays on one row; an edge moved to other endpoints is a removed and an added row.
    """

    id: str
    left: RelationSide | None = None
    right: RelationSide | None = None
    status: CompareStatus
    aspects: list[str] = PydanticField(
        default_factory=list,
        description="What differs: name, description, directed, identity, fields.",
    )
    fields: list[FieldRow] = PydanticField(default_factory=list)
    changed_on: list[Side] = PydanticField(default_factory=list)
    slots: list[list[str]] = PydanticField(default_factory=list)


class CompareFocus(ConfigBaseModel):
    """Where a merge slot lands among the rows."""

    row_id: str
    field_row_id: str | None = None
    aspect: str | None = PydanticField(
        default=None,
        description="The part of the row the slot names: description, identity, fields, ...",
    )


class SchemaComparison(ConfigBaseModel):
    """Two schemas as aligned rows."""

    left_label: str | None = None
    right_label: str | None = None
    base_label: str | None = None
    aligned_on_base: bool = PydanticField(
        description="Rows are anchored on the common ancestor (else paired by name)."
    )
    vertices: list[VertexRow] = PydanticField(default_factory=list)
    relations: list[RelationRow] = PydanticField(default_factory=list)
    counts: dict[str, int] = PydanticField(
        default_factory=dict, description="Vertex and relation rows per status."
    )
    warnings: list[str] = PydanticField(default_factory=list)

    def locate(self, slot: Sequence[str]) -> CompareFocus | None:
        """The row (and field row) a merge slot addresses, or None.

        The deepest row slot that covers *slot* wins; what *slot* says beyond it
        is the aspect (``("vertex", "company", "description")`` → the Company row,
        aspect ``description``). Match on the slot, never on its label: a
        canonical name may itself contain ``/``.
        """
        target: Slot = tuple(slot)
        best: tuple[int, CompareFocus] | None = None

        def consider(row_id: str, field_id: str | None, slots: list[list[str]]):
            nonlocal best
            for candidate in slots:
                outer = tuple(candidate)
                if not _covers(outer, target):
                    continue
                if best is not None and len(outer) <= best[0]:
                    continue
                rest = target[len(outer) :]
                aspect = rest[0] if rest else None
                if aspect == "field":
                    aspect = "fields"
                best = (
                    len(outer),
                    CompareFocus(row_id=row_id, field_row_id=field_id, aspect=aspect),
                )

        for row in [*self.vertices, *self.relations]:
            consider(row.id, None, row.slots)
            for field_row in row.fields:
                consider(row.id, field_row.id, field_row.slots)
        return None if best is None else best[1]


# ── alignment ───────────────────────────────────────────────────────────────


class _Pair:
    """One aligned element: its name in the ancestor, on the left, on the right."""

    __slots__ = ("base", "left", "right")

    def __init__(self, base: str | None, left: str | None, right: str | None):
        self.base = base
        self.left = left
        self.right = right

    @property
    def anchor(self) -> str:
        name = self.base or self.left or self.right
        assert name is not None
        return name


def _canon(name: str) -> str:
    return "/".join(canonical_key(name)) or name.lower()


def _mapped(name: str, renames: dict[str, str], present: Sequence[str]) -> str | None:
    """Where *name* went on a side, or None when the side no longer has it.

    An explicit rename wins; a name another element was renamed *to* is taken,
    so the unrenamed element of that name is gone.
    """
    if name in renames:
        target = renames[name]
    elif name in renames.values():
        return None
    else:
        target = name
    return target if target in present else None


def _align(
    left: Sequence[str],
    right: Sequence[str],
    *,
    base: Sequence[str] | None = None,
    left_renames: dict[str, str] | None = None,
    right_renames: dict[str, str] | None = None,
    cross_renames: dict[str, str] | None = None,
) -> list[_Pair]:
    """Pair names across the sides: through the ancestor (or *cross_renames*),
    then by canonical name. Unpaired names get a row of their own."""
    pairs: list[_Pair] = []
    used_left: set[str] = set()
    used_right: set[str] = set()
    if base is not None:
        for name in base:
            on_left = _mapped(name, left_renames or {}, left)
            on_right = _mapped(name, right_renames or {}, right)
            if on_left in used_left:
                on_left = None
            if on_right in used_right:
                on_right = None
            if on_left is not None:
                used_left.add(on_left)
            if on_right is not None:
                used_right.add(on_right)
            pairs.append(_Pair(name, on_left, on_right))
    else:
        for name in left:
            on_right = _mapped(name, cross_renames or {}, right)
            if on_right is not None and on_right not in used_right:
                used_left.add(name)
                used_right.add(on_right)
                pairs.append(_Pair(None, name, on_right))
    rest_right: dict[str, list[str]] = {}
    for name in right:
        if name not in used_right:
            rest_right.setdefault(_canon(name), []).append(name)
    for name in left:
        if name in used_left:
            continue
        candidates = rest_right.get(_canon(name))
        on_right = candidates.pop(0) if candidates else None
        if on_right is not None:
            used_right.add(on_right)
        pairs.append(_Pair(None, name, on_right))
    pairs.extend(_Pair(None, None, name) for name in right if name not in used_right)
    return pairs


def _row_ids(slots: Sequence[Slot]) -> list[str]:
    """Row ids from anchor slots, ``#2``-suffixed where two collide."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for slot in slots:
        plain = describe_slot(slot)
        count = seen.get(plain, 0) + 1
        seen[plain] = count
        out.append(plain if count == 1 else f"{plain}#{count}")
    return out


def _status(on_left: bool, on_right: bool, aspects: list[str]) -> CompareStatus:
    if not on_left:
        return "added"
    if not on_right:
        return "removed"
    if "name" in aspects:
        return "renamed"
    return "changed" if aspects else "unchanged"


def _sides(left: bool, right: bool) -> list[Side]:
    out: list[Side] = []
    if left:
        out.append("left")
    if right:
        out.append("right")
    return out


def _slot_lists(slots: set[Slot]) -> list[list[str]]:
    return [list(s) for s in sorted(slots)]


# ── fields ──────────────────────────────────────────────────────────────────


def _type_label(prop: Field) -> str | None:
    if prop.type is None:
        return None
    if prop.item_type is not None:
        return f"{prop.type}[{prop.item_type}]"
    return str(prop.type)


class _Owner:
    """A vertex or an edge on one side, as far as its fields go.

    *renames* carries the ancestor's field names to this side's.
    """

    __slots__ = ("fields", "key_fields", "renames")

    def __init__(
        self, fields: list[Field], key_fields: set[str], renames: dict[str, str]
    ):
        self.fields = {f.name: f for f in fields}
        self.key_fields = key_fields
        self.renames = renames

    def side(self, name: str | None) -> FieldSide | None:
        if name is None:
            return None
        prop = self.fields[name]
        return FieldSide(
            name=prop.name,
            type=_type_label(prop),
            description=prop.description,
            in_identity=prop.name in self.key_fields,
        )


def _field_aspects(a: FieldSide, b: FieldSide) -> list[str]:
    aspects: list[str] = []
    if a.name != b.name:
        aspects.append("name")
    if a.type != b.type:
        aspects.append("type")
    if a.description != b.description:
        aspects.append("description")
    if a.in_identity != b.in_identity:
        aspects.append("identity")
    return aspects


def _field_rows(
    *,
    base: _Owner | None,
    left: _Owner | None,
    right: _Owner | None,
    has_base: bool,
    cross_renames: dict[str, str],
    row_slot: Slot,
    slot_for: Callable[[str, str], Slot | None],
) -> list[FieldRow]:
    """The field rows of one vertex or edge row.

    ``slot_for(side, field)`` is a field's merge slot on that side, or None
    where fields are not addressed one by one (edge properties).
    """
    left_names = list(left.fields) if left else []
    right_names = list(right.fields) if right else []
    if base is not None:
        pairs = _align(
            left_names,
            right_names,
            base=list(base.fields),
            left_renames=left.renames if left else {},
            right_renames=right.renames if right else {},
        )
    else:
        pairs = _align(left_names, right_names, cross_renames=cross_renames)
    ids = _row_ids([(*row_slot, "field", _canon(p.anchor)) for p in pairs])

    rows: list[FieldRow] = []
    for pair, row_id in zip(pairs, ids):
        anc = base.side(pair.base) if base else None
        lhs = left.side(pair.left) if left else None
        rhs = right.side(pair.right) if right else None
        aspects = _field_aspects(lhs, rhs) if lhs and rhs else []
        if anc is not None:
            changed = _sides(
                lhs is None or bool(_field_aspects(anc, lhs)),
                rhs is None or bool(_field_aspects(anc, rhs)),
            )
        elif has_base:
            changed = _sides(lhs is not None, rhs is not None)
        else:
            changed = []
        slots: set[Slot] = set()
        for side, name in (
            ("base", pair.base),
            ("left", pair.left),
            ("right", pair.right),
        ):
            if name is not None and (slot := slot_for(side, name)) is not None:
                slots.add(slot)
        rows.append(
            FieldRow(
                id=row_id,
                left=lhs,
                right=rhs,
                status=_status(lhs is not None, rhs is not None, aspects),
                aspects=aspects,
                changed_on=changed,
                slots=_slot_lists(slots),
            )
        )
    return rows


def _field_map(rows: list[FieldRow]) -> dict[str, str]:
    """Left field name → right field name, over the paired rows."""
    return {r.left.name: r.right.name for r in rows if r.left and r.right}


# ── vertices ────────────────────────────────────────────────────────────────


def _identity_signature(vertex: Vertex, renames: dict[str, str]) -> tuple:
    """A vertex's primary key, field names carried through *renames*."""
    target = _identity_target(vertex)
    mode = target["mode"]
    if mode == "funnel":
        return (mode, str(canonical_payload(vertex.identity_funnel)))
    if mode == "hash":
        return (mode, tuple(sorted(renames.get(f, f) for f in target["hash_from"])))
    if mode == "natural":
        return (mode, tuple(renames.get(f, f) for f in target["identity"]))
    return (mode,)


def _secondary_signature(vertex: Vertex, renames: dict[str, str]) -> tuple:
    return tuple(
        sorted(
            tuple(sorted(renames.get(f, f) for f in s.fields))
            for s in vertex.secondary_identities
        )
    )


def _vertex_aspects(
    a: Vertex, b: Vertex, renames: dict[str, str], fields_differ: bool
) -> list[str]:
    """What differs from *a* to *b*; *renames* carries a's field names to b's."""
    aspects: list[str] = []
    if a.name != b.name:
        aspects.append("name")
    if a.description != b.description:
        aspects.append("description")
    if _identity_signature(a, renames) != _identity_signature(b, {}):
        aspects.append("identity")
    if _secondary_signature(a, renames) != _secondary_signature(b, {}):
        aspects.append("secondary")
    if fields_differ:
        aspects.append("fields")
    return aspects


def _vertex_changed(
    base: Vertex,
    vertex: Vertex | None,
    renames: dict[str, str],
    fields: list[FieldRow],
    side: Side,
) -> bool:
    """Whether *side* removed or changed *base* (its fields included)."""
    if vertex is None:
        return True
    fields_changed = any(side in f.changed_on for f in fields)
    return bool(_vertex_aspects(base, vertex, renames, fields_changed))


def _vertex_side(vertex: Vertex) -> VertexSide:
    return VertexSide(
        name=vertex.name,
        description=vertex.description,
        identity=list(vertex.identity),
        identity_mode=_identity_target(vertex)["mode"],
        secondary_identities=[sorted(s.fields) for s in vertex.secondary_identities],
    )


def _vertex_owner(vertex: Vertex | None, renames: dict[str, str]) -> _Owner | None:
    if vertex is None:
        return None
    keys = set(vertex.identity) | set(vertex.hash_identity_properties)
    return _Owner(vertex.properties, keys, renames)


class _Context:
    """What every row builder reads: the three schemas and the renames."""

    def __init__(
        self,
        left: GraphManifest,
        right: GraphManifest,
        base: GraphManifest | None,
        lh: RenameHints,
        rh: RenameHints,
        xh: RenameHints,
    ):
        self.has_base = base is not None
        self.hints = {"left": lh, "right": rh}
        self.cross = xh
        self.vertices: dict[str, dict[str, Vertex]] = {
            "base": _vertices(base) if base is not None else {},
            "left": _vertices(left),
            "right": _vertices(right),
        }
        self.edges: dict[str, dict[tuple[str, str, str | None], Edge]] = {
            "base": _edges(base) if base is not None else {},
            "left": _edges(left),
            "right": _edges(right),
        }
        # vertex name on a side → its row id; row id → the name it is anchored on
        self.row_of: dict[str, dict[str, str]] = {"base": {}, "left": {}, "right": {}}
        self.anchor_of: dict[str, str] = {}


def _vertex_rows(ctx: _Context) -> list[VertexRow]:
    lh, rh = ctx.hints["left"], ctx.hints["right"]
    bv, lv, rv = ctx.vertices["base"], ctx.vertices["left"], ctx.vertices["right"]
    pairs = _align(
        list(lv),
        list(rv),
        base=list(bv) if ctx.has_base else None,
        left_renames=lh.vertices,
        right_renames=rh.vertices,
        cross_renames=ctx.cross.vertices,
    )
    anchor_slots = [_vertex_slot(p.anchor) for p in pairs]
    ids = _row_ids(anchor_slots)

    rows: list[VertexRow] = []
    for pair, row_id, anchor_slot in zip(pairs, ids, anchor_slots):
        names = {"base": pair.base, "left": pair.left, "right": pair.right}
        ctx.anchor_of[row_id] = pair.anchor
        for side, name in names.items():
            if name is not None:
                ctx.row_of[side][name] = row_id
        b = bv[pair.base] if pair.base else None
        lvx = lv[pair.left] if pair.left else None
        rvx = rv[pair.right] if pair.right else None
        left_renames = lh.vertex_properties.get(pair.left or "", {})
        right_renames = rh.vertex_properties.get(pair.right or "", {})

        def slot_for(side: str, field: str, names: dict = names) -> Slot | None:
            return _field_slot(names[side], field) if names[side] else None

        fields = _field_rows(
            base=_vertex_owner(b, {}),
            left=_vertex_owner(lvx, left_renames),
            right=_vertex_owner(rvx, right_renames),
            has_base=ctx.has_base,
            cross_renames=ctx.cross.vertex_properties.get(pair.right or "", {}),
            row_slot=anchor_slot,
            slot_for=slot_for,
        )
        aspects = (
            _vertex_aspects(
                lvx,
                rvx,
                _field_map(fields),
                any(f.status != "unchanged" for f in fields),
            )
            if lvx is not None and rvx is not None
            else []
        )
        if b is not None:
            changed_on = _sides(
                _vertex_changed(b, lvx, left_renames, fields, "left"),
                _vertex_changed(b, rvx, right_renames, fields, "right"),
            )
        elif ctx.has_base:
            changed_on = _sides(lvx is not None, rvx is not None)
        else:
            changed_on = []
        rows.append(
            VertexRow(
                id=row_id,
                left=_vertex_side(lvx) if lvx is not None else None,
                right=_vertex_side(rvx) if rvx is not None else None,
                status=_status(lvx is not None, rvx is not None, aspects),
                aspects=aspects,
                fields=fields,
                changed_on=changed_on,
                slots=_slot_lists({_vertex_slot(n) for n in names.values() if n}),
            )
        )
    return rows


# ── relations ───────────────────────────────────────────────────────────────


EdgeKey = tuple[str, str, str | None]


def _identities_signature(edge: Edge, renames: dict[str, str]) -> tuple:
    return tuple(
        sorted(tuple(renames.get(f, f) for f in key) for key in edge.identities)
    )


def _edge_aspects(
    a: Edge, b: Edge, renames: dict[str, str], fields_differ: bool
) -> list[str]:
    """What differs from *a* to *b*; *renames* carries a's property names to b's."""
    aspects: list[str] = []
    if a.relation != b.relation:
        aspects.append("name")
    if a.description != b.description:
        aspects.append("description")
    if a.directed != b.directed:
        aspects.append("directed")
    if _identities_signature(a, renames) != _identities_signature(b, {}):
        aspects.append("identity")
    if fields_differ:
        aspects.append("fields")
    return aspects


def _edge_changed(
    base: Edge,
    edge: Edge | None,
    renames: dict[str, str],
    fields: list[FieldRow],
    side: Side,
) -> bool:
    """Whether *side* removed or changed *base* (its properties included)."""
    if edge is None:
        return True
    fields_changed = any(side in f.changed_on for f in fields)
    return bool(_edge_aspects(base, edge, renames, fields_changed))


def _edge_owner(edge: Edge | None, renames: dict[str, str]) -> _Owner | None:
    if edge is None:
        return None
    keys = {f for key in edge.identities for f in key}
    return _Owner(edge.properties, keys, renames)


def _relation_rows(ctx: _Context) -> list[RelationRow]:
    """Edges keyed by their endpoint rows and their relation's ancestor name."""
    origin = {
        side: {new: old for old, new in ctx.hints[side].relations.items()}
        for side in ("left", "right")
    }

    def key_of(side: str, triple: EdgeKey) -> EdgeKey:
        source, target, relation = triple
        if relation is not None:
            if ctx.has_base and side != "base":
                relation = origin[side].get(relation, relation)
            elif not ctx.has_base and side == "left":
                relation = ctx.cross.relations.get(relation, relation)
            relation = _canon(relation)
        rows = ctx.row_of[side]
        return (rows.get(source, source), rows.get(target, target), relation)

    grouped: dict[EdgeKey, dict[str, EdgeKey]] = {}
    for side in ("base", "left", "right"):
        for triple in ctx.edges[side]:
            slot = grouped.setdefault(key_of(side, triple), {})
            if side in slot:
                # Two edges of one side on one key: give the second its own row.
                slot = grouped.setdefault((*triple[:2], f"{triple[2]}#{side}"), {})
            slot[side] = triple

    entries = list(grouped.items())
    anchor_slots: list[Slot] = []
    for (source_row, target_row, _), triples in entries:
        named = triples.get("base") or triples.get("left") or triples.get("right")
        assert named is not None
        anchor_slots.append(
            _edge_slot(
                ctx.anchor_of.get(source_row, source_row),
                ctx.anchor_of.get(target_row, target_row),
                named[2],
            )
        )
    ids = _row_ids(anchor_slots)

    rows: list[RelationRow] = []
    for (_key, triples), row_id, anchor_slot in zip(entries, ids, anchor_slots):
        edge = {side: ctx.edges[side][t] for side, t in triples.items()}
        b, le, re = edge.get("base"), edge.get("left"), edge.get("right")

        def renames(side: Side, e: Edge | None) -> dict[str, str]:
            if e is None or e.relation is None:
                return {}
            return ctx.hints[side].edge_properties.get(e.relation, {})

        fields = _field_rows(
            base=_edge_owner(b, {}),
            left=_edge_owner(le, renames("left", le)),
            right=_edge_owner(re, renames("right", re)),
            has_base=ctx.has_base,
            cross_renames=(
                ctx.cross.edge_properties.get(re.relation, {})
                if re is not None and re.relation is not None
                else {}
            ),
            row_slot=anchor_slot,
            slot_for=lambda side, field: None,
        )
        aspects = (
            _edge_aspects(
                le, re, _field_map(fields), any(f.status != "unchanged" for f in fields)
            )
            if le is not None and re is not None
            else []
        )
        if b is not None:
            changed_on = _sides(
                _edge_changed(b, le, renames("left", le), fields, "left"),
                _edge_changed(b, re, renames("right", re), fields, "right"),
            )
        elif ctx.has_base:
            changed_on = _sides(le is not None, re is not None)
        else:
            changed_on = []

        slots: set[Slot] = set()
        for e in edge.values():
            slots.add(_edge_slot(e.source, e.target, e.relation))
            if e.relation is not None:
                slots.add(_relation_slot(e.relation))

        def side_of(side: str, e: Edge | None) -> RelationSide | None:
            if e is None:
                return None
            rows_of = ctx.row_of[side]
            return RelationSide(
                relation=e.relation,
                source=e.source,
                target=e.target,
                source_row=rows_of.get(e.source, e.source),
                target_row=rows_of.get(e.target, e.target),
                description=e.description,
                directed=e.directed,
                identities=[list(key) for key in e.identities],
            )

        rows.append(
            RelationRow(
                id=row_id,
                left=side_of("left", le),
                right=side_of("right", re),
                status=_status(le is not None, re is not None, aspects),
                aspects=aspects,
                fields=fields,
                changed_on=changed_on,
                slots=_slot_lists(slots),
            )
        )
    return rows


# ── the comparison ──────────────────────────────────────────────────────────


def compare_schemas(
    left: GraphManifest,
    right: GraphManifest,
    *,
    base: GraphManifest | None = None,
    left_hints: RenameHints | None = None,
    right_hints: RenameHints | None = None,
    hints: RenameHints | None = None,
    left_label: str | None = None,
    right_label: str | None = None,
    base_label: str | None = None,
) -> SchemaComparison:
    """Align the schemas of *left* and *right* into rows.

    Args:
        left: One version.
        right: The other version.
        base: Their common ancestor. Rows are anchored on it, and each row's
            ``changed_on`` says which side changed it.
        left_hints: Renames from *base* to *left*, as :func:`merge_three_way`
            takes them (:meth:`RenameHints.from_ops` over that side's ops).
        right_hints: Renames from *base* to *right*.
        hints: Renames from *left* to *right*; read only without *base*.
        left_label: The left side's name (a version), echoed back.
        right_label: The right side's name.
        base_label: The ancestor's name.

    Returns:
        The rows in declaration order: the ancestor's elements first, then what
        the left side added, then what only the right side has.
    """
    ctx = _Context(
        left,
        right,
        base,
        left_hints or RenameHints(),
        right_hints or RenameHints(),
        hints or RenameHints(),
    )
    warnings: list[str] = []
    if base is None and hints is None:
        warnings.append(
            "No common ancestor: rows are paired by name, so a rename shows as "
            "one removed and one added row."
        )
    vertex_rows = _vertex_rows(ctx)
    relation_rows = _relation_rows(ctx)
    counts: dict[str, int] = {}
    for row in [*vertex_rows, *relation_rows]:
        counts[row.status] = counts.get(row.status, 0) + 1
    return SchemaComparison(
        left_label=left_label,
        right_label=right_label,
        base_label=base_label if base is not None else None,
        aligned_on_base=base is not None,
        vertices=vertex_rows,
        relations=relation_rows,
        counts=counts,
        warnings=warnings,
    )


__all__ = [
    "CompareFocus",
    "CompareStatus",
    "FieldRow",
    "FieldSide",
    "RelationRow",
    "RelationSide",
    "SchemaComparison",
    "VertexRow",
    "VertexSide",
    "compare_schemas",
]
