"""A schema as a neo4j-graphrag ``GraphSchema`` document, and back.

``GraphSchema`` is the YAML/JSON document neo4j-graphrag's knowledge-graph
pipeline reads (``GraphSchema.from_file``): node and relationship types with
typed properties, ``(source, relationship, target)`` patterns, and ``KEY`` /
``UNIQUENESS`` / ``EXISTENCE`` constraints. Plain mappings go in and out;
neo4j-graphrag is not imported.

Both directions are lossy, and each returns what it left out.

graflo -> ``GraphSchema`` (:func:`to_graph_schema`):

- vertex -> node type; ``identity`` -> ``KEY``; ``secondary_identities`` ->
  ``UNIQUENESS``; a blank vertex gets no ``KEY``; a hashed identity becomes a
  ``KEY`` on the fields it digests;
- edge -> one ``(source, relation, target)`` pattern; edges sharing a relation
  share one relationship type, which holds the union of their properties;
- edge ``identities`` are left out: a ``GraphSchema`` relationship ``KEY`` is
  unique across all relationships of the type, a graflo edge identity only
  between one pair of endpoints;
- ``DATETIME`` -> ``ZONED_DATETIME``; ``UUID`` and untyped fields -> ``STRING``.

``GraphSchema`` -> graflo (:func:`from_graph_schema`):

- node type -> vertex; its first ``KEY`` -> ``identity``, further ``KEY`` and
  ``UNIQUENESS`` constraints -> ``secondary_identities``; a node type without a
  ``KEY`` is blank: each mention is a new entity;
- each pattern -> an edge carrying its relationship type's properties; a
  relationship ``KEY`` -> edge ``identities``;
- ``DATE`` and both datetimes -> ``DATETIME``; ``LIST``, ``DURATION``, ``POINT``
  and the times -> an untyped field;
- ``EXISTENCE`` constraints and the ``additional_*`` switches have no graflo
  counterpart: a graflo schema is closed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:
    from graflo.architecture.schema.document import Schema

Neo4jType = Literal[
    "BOOLEAN",
    "DATE",
    "DURATION",
    "FLOAT",
    "INTEGER",
    "LIST",
    "LOCAL_DATETIME",
    "LOCAL_TIME",
    "POINT",
    "STRING",
    "ZONED_DATETIME",
    "ZONED_TIME",
]

#: ``GraphSchema`` property type -> graflo field type; ``None`` leaves it untyped.
TO_GRAFLO: dict[str, str | None] = {
    "BOOLEAN": "BOOL",
    "INTEGER": "INT",
    "FLOAT": "FLOAT",
    "STRING": "STRING",
    "DATE": "DATETIME",
    "LOCAL_DATETIME": "DATETIME",
    "ZONED_DATETIME": "DATETIME",
    "LIST": None,
    "DURATION": None,
    "LOCAL_TIME": None,
    "ZONED_TIME": None,
    "POINT": None,
}

#: graflo field type -> ``GraphSchema`` property type.
FROM_GRAFLO: dict[str, str] = {
    "INT": "INTEGER",
    "UINT": "INTEGER",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "BOOL": "BOOLEAN",
    "STRING": "STRING",
    "DATETIME": "ZONED_DATETIME",
    "UUID": "STRING",
    "LIST": "LIST",
}


# -- the GraphSchema document ------------------------------------------------


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Property(_Strict):
    name: str
    type: Neo4jType
    description: str = ""
    required: bool = False


class _NodeType(_Strict):
    label: str
    description: str = ""
    properties: list[_Property] = Field(default_factory=list)
    additional_properties: bool | None = None

    @model_validator(mode="before")
    @classmethod
    def _label_only(cls, data: Any) -> Any:
        return {"label": data} if isinstance(data, str) else data


class _RelationshipType(_NodeType):
    pass


class _Pattern(_Strict):
    source: str
    relationship: str
    target: str

    @model_validator(mode="before")
    @classmethod
    def _triple(cls, data: Any) -> Any:
        if isinstance(data, list | tuple):
            if len(data) != 3:
                raise ValueError("a pattern is [source, relationship, target]")
            return dict(zip(("source", "relationship", "target"), data, strict=True))
        return data


class _Constraint(_Strict):
    type: Literal["KEY", "UNIQUENESS", "EXISTENCE"]
    property_names: list[str] = Field(default_factory=list)
    property_name: str = ""
    node_type: str | None = None
    relationship_type: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> _Constraint:
        if not self.property_names and self.property_name:
            self.property_names = [self.property_name]
        if not self.property_names:
            raise ValueError("a constraint names at least one property")
        if bool(self.node_type) == bool(self.relationship_type):
            raise ValueError("a constraint names one node_type or relationship_type")
        return self

    @property
    def owner(self) -> str:
        return self.node_type or self.relationship_type or ""


class _Document(_Strict):
    node_types: list[_NodeType] = Field(min_length=1)
    relationship_types: list[_RelationshipType] = Field(default_factory=list)
    patterns: list[_Pattern] = Field(default_factory=list)
    constraints: list[_Constraint] = Field(default_factory=list)
    additional_node_types: bool | None = None
    additional_relationship_types: bool | None = None
    additional_patterns: bool | None = None

    @model_validator(mode="after")
    def _references(self) -> _Document:
        nodes = {n.label: n for n in self.node_types}
        rels = {r.label: r for r in self.relationship_types}
        unknown = sorted(
            {p.source for p in self.patterns if p.source not in nodes}
            | {p.target for p in self.patterns if p.target not in nodes}
            | {p.relationship for p in self.patterns if p.relationship not in rels}
        )
        if unknown:
            raise ValueError(f"patterns name undeclared types: {', '.join(unknown)}")
        for c in self.constraints:
            owner = (nodes if c.node_type else rels).get(c.owner)
            if owner is None:
                raise ValueError(f"a constraint names an undeclared type: {c.owner}")
            declared = {p.name for p in owner.properties}
            if not owner.properties and c.node_type:
                declared = {"name"}
            missing = [p for p in c.property_names if p not in declared]
            if missing:
                raise ValueError(
                    f"{c.type} on {c.owner} names undeclared properties: "
                    + ", ".join(missing)
                )
        return self


# -- GraphSchema -> graflo ---------------------------------------------------


def _field_in(owner: str, prop: _Property, losses: list[str]) -> dict[str, Any]:
    field: dict[str, Any] = {"name": prop.name}
    graflo_type = TO_GRAFLO[prop.type]
    if graflo_type is not None:
        field["type"] = graflo_type
    else:
        losses.append(f"`{owner}.{prop.name}`: type {prop.type}, read as untyped")
    if prop.type == "DATE":
        losses.append(f"`{owner}.{prop.name}`: DATE, read as DATETIME")
    if prop.description:
        field["description"] = prop.description
    if prop.required:
        losses.append(f"`{owner}.{prop.name}`: required")
    return field


def _alt_keys(keys: list[list[str]]) -> list[dict[str, Any]]:
    return [{"name": "by_" + "_".join(k), "fields": k} for k in keys]


def from_graph_schema(data: Mapping[str, Any], name: str) -> tuple[Schema, list[str]]:
    """A graflo ``Schema`` from a ``GraphSchema`` mapping, and what it left out.

    Args:
        data: The ``GraphSchema`` document, as read from its YAML or JSON.
        name: The schema's name (``GraphSchema`` carries none).

    Raises:
        pydantic.ValidationError: *data* is not a ``GraphSchema`` document, or
            the schema it stands for is not a valid graflo schema.
    """
    from graflo.architecture.schema.document import Schema

    doc = _Document.model_validate(data)
    losses: list[str] = []
    found: dict[tuple[str, str], list[list[str]]] = {}
    for c in doc.constraints:
        if c.type == "EXISTENCE":
            losses.append(f"`{c.owner}.{c.property_names[0]}`: required (EXISTENCE)")
            continue
        found.setdefault((c.owner, c.type), []).append(list(c.property_names))

    vertices = []
    for node in doc.node_types:
        label = node.label
        props = node.properties or [_Property(name="name", type="STRING")]
        if not node.properties:
            losses.append(f"node type `{label}`: no properties, read as one `name`")
        vertex: dict[str, Any] = {
            "name": label,
            "properties": [_field_in(label, p, losses) for p in props],
        }
        if node.description:
            vertex["description"] = node.description
        keys = found.get((label, "KEY"), [])
        unique = found.get((label, "UNIQUENESS"), [])
        if keys:
            vertex["identity"] = keys[0]
            if keys[1:] + unique:
                vertex["secondary_identities"] = _alt_keys(keys[1:] + unique)
            for extra in keys[1:]:
                losses.append(
                    f"node type `{label}`: KEY {extra}, kept as an alternate key"
                )
        else:
            vertex["blank"] = True
            losses.append(
                f"node type `{label}`: no KEY, so each mention is a new entity; "
                "add a KEY constraint to merge them"
            )
            for u in unique:
                losses.append(
                    f"node type `{label}`: UNIQUENESS {u} (a type without a KEY "
                    "takes no alternate key)"
                )
        open_props = node.additional_properties
        if open_props is None:
            open_props = not node.properties
        if open_props:
            losses.append(f"node type `{label}`: undeclared properties")
        vertices.append(vertex)

    rels = {r.label: r for r in doc.relationship_types}
    edges = []
    seen: set[tuple[str, str, str]] = set()
    for p in doc.patterns:
        triple = (p.source, p.relationship, p.target)
        if triple in seen:
            continue
        seen.add(triple)
        rel = rels[p.relationship]
        edge: dict[str, Any] = {
            "source": p.source,
            "target": p.target,
            "relation": p.relationship,
        }
        if rel.description:
            edge["description"] = rel.description
        if rel.properties:
            edge["properties"] = [
                _field_in(p.relationship, q, losses) for q in rel.properties
            ]
        keys = found.get((p.relationship, "KEY"), [])
        if keys:
            edge["identities"] = keys
        edges.append(edge)

    for label, rel in rels.items():
        keys = found.get((label, "KEY"), [])
        if keys:
            losses.append(
                f"relationship type `{label}`: KEY {keys} is unique between one "
                "pair of endpoints in graflo, not across all relationships"
            )
        for u in found.get((label, "UNIQUENESS"), []):
            losses.append(f"relationship type `{label}`: UNIQUENESS {u}")
        if rel.properties and rel.additional_properties:
            losses.append(f"relationship type `{label}`: undeclared properties")
        if not any(p.relationship == label for p in doc.patterns):
            losses.append(
                f"relationship type `{label}`: no pattern names its endpoints; left out"
            )

    open_world = {
        "additional_node_types": (doc.additional_node_types, False),
        "additional_relationship_types": (
            doc.additional_relationship_types,
            not doc.relationship_types,
        ),
        "additional_patterns": (doc.additional_patterns, not doc.patterns),
    }
    for switch, (value, default) in open_world.items():
        if value if value is not None else default:
            losses.append(f"{switch}: a graflo schema accepts only what it declares")

    schema = Schema.model_validate(
        {
            "metadata": {"name": name},
            "core_schema": {
                "vertex_config": {"vertices": vertices},
                "edge_config": {"edges": edges},
            },
        }
    )
    return schema, losses


# -- graflo -> GraphSchema ---------------------------------------------------

_VERTEX_KEYS = {
    "name",
    "properties",
    "identity",
    "description",
    "blank",
    "hash_identity_properties",
    "secondary_identities",
}
_EDGE_KEYS = {"source", "target", "relation", "description", "properties"}
_FIELD_KEYS = {"name", "type", "description", "item_type"}


def _semantics_lost(owner: str, element: dict[str, Any], losses: list[str]) -> None:
    for key in element.get("semantics") or {}:
        losses.append(f"`{owner}`: semantics.{key}")


def _prop_out(owner: str, field: dict[str, Any], losses: list[str]) -> dict[str, Any]:
    label = f"{owner}.{field['name']}"
    graflo_type = field.get("type")
    if graflo_type is None:
        losses.append(f"`{label}`: untyped, written as STRING")
    elif graflo_type in ("UINT", "UUID"):
        losses.append(
            f"`{label}`: {graflo_type}, written as {FROM_GRAFLO[graflo_type]}"
        )
    if "item_type" in field:
        losses.append(f"`{label}`: list item type {field['item_type']}")
    prop: dict[str, Any] = {
        "name": field["name"],
        "type": FROM_GRAFLO.get(graflo_type or "", "STRING"),
    }
    if "description" in field:
        prop["description"] = field["description"]
    _semantics_lost(label, field, losses)
    for key in sorted(set(field) - _FIELD_KEYS - {"semantics"}):
        losses.append(f"`{label}`: {key}")
    return prop


def _node_out(
    vertex: dict[str, Any], constraints: list[dict[str, Any]], losses: list[str]
) -> dict[str, Any]:
    name = vertex["name"]
    hashed = vertex.get("hash_identity_properties")
    minted = vertex.get("blank") or hashed
    # The ``id`` a minted vertex carries is graflo's own, not the source's.
    fields = [
        f
        for f in vertex.get("properties", [])
        if not (minted and f["name"] == "id" and "type" not in f)
    ]
    node: dict[str, Any] = {"label": name}
    if "description" in vertex:
        node["description"] = vertex["description"]
    if fields:
        node["properties"] = [_prop_out(name, f, losses) for f in fields]
        node["additional_properties"] = False
    else:
        losses.append(f"vertex `{name}`: no fields; neo4j-graphrag adds a `name`")
    key = hashed or ([] if vertex.get("blank") else vertex.get("identity"))
    if hashed:
        losses.append(
            f"vertex `{name}`: identity is a digest of {hashed}, written as a KEY"
        )
    if key:
        constraints.append({"type": "KEY", "node_type": name, "property_names": key})
    for alt in vertex.get("secondary_identities") or []:
        if alt["fields"] != key:
            constraints.append(
                {
                    "type": "UNIQUENESS",
                    "node_type": name,
                    "property_names": alt["fields"],
                }
            )
    _semantics_lost(name, vertex, losses)
    for k in sorted(set(vertex) - _VERTEX_KEYS - {"semantics"}):
        losses.append(f"vertex `{name}`: {k}")
    return node


def to_graph_schema(schema: Schema) -> tuple[dict[str, Any], list[str]]:
    """The ``GraphSchema`` document for *schema*, and what it left out.

    The document loads with ``GraphSchema.model_validate`` (or ``from_file``
    once written as YAML or JSON).
    """
    canonical = schema.to_minimal_canonical_dict()
    losses: list[str] = []
    metadata = canonical.get("metadata") or {}
    if metadata:
        losses.append("schema metadata: " + ", ".join(sorted(metadata)))
    if "db_profile" in canonical:
        # Indexes graflo derives from the schema itself (alternate keys) are
        # carried by the constraints; only a declared profile is lost.
        bare = {k: v for k, v in canonical.items() if k != "db_profile"}
        derived = type(schema).model_validate(bare).to_minimal_canonical_dict()
        if derived.get("db_profile") != canonical["db_profile"]:
            losses.append("db_profile")
    losses.extend(sorted(set(canonical) - {"metadata", "core_schema", "db_profile"}))
    core = canonical.get("core_schema") or {}
    vertex_config = core.get("vertex_config") or {}
    edge_config = core.get("edge_config") or {}
    for key in sorted(set(core) - {"vertex_config", "edge_config"}):
        losses.append(f"core_schema.{key}")
    for key in sorted(set(vertex_config) - {"vertices"}):
        losses.append(f"vertex_config.{key}")
    for key in sorted(set(edge_config) - {"edges"}):
        losses.append(f"edge_config.{key}")

    constraints: list[dict[str, Any]] = []
    nodes = [
        _node_out(v, constraints, losses) for v in vertex_config.get("vertices", [])
    ]

    rels: dict[str, dict[str, Any]] = {}
    patterns: list[dict[str, str]] = []
    for edge in edge_config.get("edges", []):
        relation = edge.get("relation")
        if relation is None:
            relation = edge["target"]
            losses.append(
                f"edge `{edge['source']} -> {edge['target']}`: no relation, "
                f"written as `{relation}`"
            )
        label = f"{edge['source']} {relation} {edge['target']}"
        patterns.append(
            {
                "source": edge["source"],
                "relationship": relation,
                "target": edge["target"],
            }
        )
        rel = rels.setdefault(relation, {"label": relation, "properties": {}})
        described = edge.get("description")
        if (
            described is not None
            and rel.setdefault("description", described) != described
        ):
            losses.append(f"edge `{label}`: description (another edge's kept)")
        for field in edge.get("properties", []):
            prop = _prop_out(label, field, losses)
            kept = rel["properties"].setdefault(prop["name"], prop)
            if kept["type"] != prop["type"]:
                losses.append(f"`{label}.{prop['name']}`: type (another edge's kept)")
        if edge.get("identities"):
            losses.append(
                f"edge `{label}`: identities {edge['identities']} (a relationship "
                "KEY would be unique across all relationships of the type)"
            )
        _semantics_lost(label, edge, losses)
        for k in sorted(set(edge) - _EDGE_KEYS - {"semantics", "identities"}):
            losses.append(f"edge `{label}`: {k}")

    relationship_types = []
    for rel in rels.values():
        out: dict[str, Any] = {"label": rel["label"]}
        if "description" in rel:
            out["description"] = rel["description"]
        if rel["properties"]:
            out["properties"] = list(rel["properties"].values())
            out["additional_properties"] = False
        relationship_types.append(out)

    document: dict[str, Any] = {
        "node_types": nodes,
        "relationship_types": relationship_types,
        "patterns": patterns,
        "constraints": constraints,
        "additional_node_types": False,
        "additional_relationship_types": False,
        "additional_patterns": False,
    }
    return document, losses
