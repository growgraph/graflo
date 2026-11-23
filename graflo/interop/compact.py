"""The compact schema notation: a short YAML form of a GraFlo schema.

An optional way in. GraFlo's own schema (or manifest) stays the complete
format; the compact notation covers what a first schema needs: types with
descriptions, other names, keys, alternate keys and typed fields, and
relations with fields. :func:`from_compact` turns it into a GraFlo
``Schema``; :func:`to_compact` turns a schema back, and refuses one that uses
anything the notation cannot say.

```yaml
name: support
version: 1.0.0
types:
  Customer:
    about: A customer account.
    aka: [client]
    key: account
    alt_keys: [email]
    fields: {account: str, email: str, name: str}
  Tag:
    key: label
    hashed: true
    fields: {label: str}
relations:
  - Customer RAISED Ticket
  - {from: Agent, name: HANDLES, to: Ticket, fields: {since: datetime}}
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:
    from graflo.architecture.schema.document import Schema

TypeName = Literal["str", "int", "float", "bool", "datetime", "uuid", "any"]

#: Compact type name → GraFlo field type; ``any`` leaves the field untyped.
FIELD_TYPES: dict[str, str | None] = {
    "str": "STRING",
    "int": "INT",
    "float": "FLOAT",
    "bool": "BOOL",
    "datetime": "DATETIME",
    "uuid": "UUID",
    "any": None,
}
_TYPE_NAMES = {graflo: name for name, graflo in FIELD_TYPES.items()}


class CompactUnsupported(ValueError):
    """A schema uses what the compact notation cannot say.

    Attributes:
        features: Paths in the schema's canonical form that would be lost.
    """

    def __init__(self, features: list[str]) -> None:
        self.features = features
        super().__init__("the compact notation cannot express: " + ", ".join(features))


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class CompactField(_Strict):
    """A field: its type name alone, or this long form."""

    type: TypeName
    about: str | None = None
    aka: list[str] = Field(default_factory=list)
    unit: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _short_form(cls, data: Any) -> Any:
        return {"type": data} if isinstance(data, str) else data


def _names(key: str | list[str]) -> list[str]:
    return [key] if isinstance(key, str) else list(key)


class CompactType(_Strict):
    """A vertex type."""

    about: str | None = None
    aka: list[str] = Field(default_factory=list)
    key: str | list[str] | None = Field(
        default=None,
        description="The identity fields; none: each mention is a new entity.",
    )
    hashed: bool = Field(
        default=False, description="The identity is a digest of the key fields."
    )
    alt_keys: list[str | list[str]] | dict[str, str | list[str]] = Field(
        default_factory=list,
        description="Other keys: a list (named ``by_<fields>``) or a mapping name → fields.",
    )
    fields: dict[str, CompactField] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _keys_are_fields(self) -> CompactType:
        declared = set(self.fields)
        if self.hashed and self.key is None:
            raise ValueError("hashed needs a key")
        missing = [k for k in _names(self.key or []) if k not in declared]
        for fields in self.alt_key_map().values():
            missing += [f for f in fields if f not in declared]
        if missing:
            raise ValueError(f"key fields not in fields: {', '.join(missing)}")
        return self

    def alt_key_map(self) -> dict[str, list[str]]:
        """Each alternate key by name."""
        if isinstance(self.alt_keys, dict):
            return {name: _names(f) for name, f in self.alt_keys.items()}
        keys = [_names(k) for k in self.alt_keys]
        return {"by_" + "_".join(fields): fields for fields in keys}


class CompactRelation(_Strict):
    """A relation in long form; the short form is ``"Source RELATION Target"``."""

    source: str = Field(alias="from")
    name: str
    target: str = Field(alias="to")
    about: str | None = None
    aka: list[str] = Field(default_factory=list)
    fields: dict[str, CompactField] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _short_form(cls, data: Any) -> Any:
        if isinstance(data, str):
            parts = data.split()
            if len(parts) != 3:
                raise ValueError(f"{data!r}: a relation is 'Source RELATION Target'")
            return {"from": parts[0], "name": parts[1], "to": parts[2]}
        return data


class CompactSchema(_Strict):
    """A whole schema in the compact notation."""

    name: str
    version: str | None = None
    about: str | None = None
    types: dict[str, CompactType] = Field(min_length=1)
    relations: list[CompactRelation] = Field(default_factory=list)

    @model_validator(mode="after")
    def _endpoints_are_types(self) -> CompactSchema:
        unknown = sorted(
            {
                end
                for r in self.relations
                for end in (r.source, r.target)
                if end not in self.types
            }
        )
        if unknown:
            raise ValueError(f"relations name unknown types: {', '.join(unknown)}")
        return self


def is_compact(data: Any) -> bool:
    """Whether a mapping is in the compact notation (it has ``types``)."""
    return isinstance(data, dict) and "types" in data


def _semantics(aka: list[str], unit: str | None = None) -> dict[str, Any] | None:
    semantics: dict[str, Any] = {}
    if aka:
        semantics["synonyms"] = list(aka)
    if unit is not None:
        semantics["unit"] = unit
    return semantics or None


def _described(target: dict[str, Any], about: str | None, semantics: Any) -> None:
    if about is not None:
        target["description"] = about
    if semantics:
        target["semantics"] = semantics


def _field(name: str, long: CompactField) -> dict[str, Any]:
    field: dict[str, Any] = {"name": name}
    graflo_type = FIELD_TYPES[long.type]
    if graflo_type is not None:
        field["type"] = graflo_type
    _described(field, long.about, _semantics(long.aka, long.unit))
    return field


def _vertex(name: str, spec: CompactType) -> dict[str, Any]:
    vertex: dict[str, Any] = {
        "name": name,
        "properties": [_field(f, s) for f, s in spec.fields.items()],
    }
    _described(vertex, spec.about, _semantics(spec.aka))
    if spec.key is None:
        vertex["blank"] = True
    elif spec.hashed:
        vertex["hash_identity_properties"] = _names(spec.key)
    else:
        vertex["identity"] = _names(spec.key)
    alt = spec.alt_key_map()
    if alt:
        vertex["secondary_identities"] = [
            {"name": n, "fields": fields} for n, fields in alt.items()
        ]
    return vertex


def _edge(spec: CompactRelation) -> dict[str, Any]:
    edge: dict[str, Any] = {
        "source": spec.source,
        "target": spec.target,
        "relation": spec.name,
    }
    _described(edge, spec.about, _semantics(spec.aka))
    if spec.fields:
        edge["properties"] = [_field(f, s) for f, s in spec.fields.items()]
    return edge


def compact_to_mapping(data: Any) -> dict[str, Any]:
    """The GraFlo schema mapping a compact schema stands for.

    Raises:
        pydantic.ValidationError: when *data* is not a valid compact schema;
            the error's location names the type, field or relation.
    """
    spec = CompactSchema.model_validate(data)
    metadata: dict[str, Any] = {"name": spec.name}
    if spec.version is not None:
        metadata["version"] = spec.version
    if spec.about is not None:
        metadata["description"] = spec.about
    return {
        "metadata": metadata,
        "core_schema": {
            "vertex_config": {
                "vertices": [_vertex(n, t) for n, t in spec.types.items()]
            },
            "edge_config": {"edges": [_edge(r) for r in spec.relations]},
        },
    }


def from_compact(data: Any) -> Schema:
    """A GraFlo ``Schema`` from a compact schema mapping.

    Raises:
        pydantic.ValidationError: when *data* is not a valid compact schema, or
            the schema it stands for is not a valid GraFlo schema.
    """
    from graflo.architecture.schema.document import Schema

    return Schema.model_validate(compact_to_mapping(data))


# -- back to the compact notation -------------------------------------------


def _compact_field(field: dict[str, Any]) -> Any:
    type_name = _TYPE_NAMES.get(field.get("type"), field.get("type"))
    semantics = field.get("semantics") or {}
    if "description" not in field and not semantics:
        return type_name
    long: dict[str, Any] = {"type": type_name}
    if "description" in field:
        long["about"] = field["description"]
    if semantics.get("synonyms"):
        long["aka"] = semantics["synonyms"]
    if "unit" in semantics:
        long["unit"] = semantics["unit"]
    return long


def _compact_about(source: dict[str, Any], target: dict[str, Any]) -> None:
    if "description" in source:
        target["about"] = source["description"]
    synonyms = (source.get("semantics") or {}).get("synonyms")
    if synonyms:
        target["aka"] = synonyms


def _compact_type(vertex: dict[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = {}
    _compact_about(vertex, spec)
    minted = vertex.get("blank") or vertex.get("hash_identity_properties")
    if vertex.get("hash_identity_properties"):
        key = vertex["hash_identity_properties"]
        spec["key"] = key[0] if len(key) == 1 else key
        spec["hashed"] = True
    elif not vertex.get("blank") and vertex.get("identity"):
        key = vertex["identity"]
        spec["key"] = key[0] if len(key) == 1 else key
    secondary = vertex.get("secondary_identities") or []
    if secondary:
        named = {s["name"]: s["fields"] for s in secondary}
        generated = all(n == "by_" + "_".join(f) for n, f in named.items())
        if generated:
            spec["alt_keys"] = [f[0] if len(f) == 1 else f for f in named.values()]
        else:
            spec["alt_keys"] = {n: f[0] if len(f) == 1 else f for n, f in named.items()}
    # The ``id`` a minted type carries is GraFlo's, not the notation's.
    spec["fields"] = {
        f["name"]: _compact_field(f)
        for f in vertex.get("properties", [])
        if not (minted and f["name"] == "id" and "type" not in f)
    }
    return spec


def _compact_relation(edge: dict[str, Any]) -> Any:
    plain = set(edge) <= {"source", "target", "relation"}
    if plain:
        return f"{edge['source']} {edge['relation']} {edge['target']}"
    spec: dict[str, Any] = {
        "from": edge["source"],
        "name": edge.get("relation"),
        "to": edge["target"],
    }
    _compact_about(edge, spec)
    if edge.get("properties"):
        spec["fields"] = {p["name"]: _compact_field(p) for p in edge["properties"]}
    return spec


def _differences(a: Any, b: Any, path: str = "") -> list[str]:
    if isinstance(a, dict) and b is None:
        b = {}
    if isinstance(b, dict) and a is None:
        a = {}
    if isinstance(a, dict) and isinstance(b, dict):
        out: list[str] = []
        for key in sorted(set(a) | set(b), key=str):
            out += _differences(
                a.get(key), b.get(key), f"{path}.{key}" if path else str(key)
            )
        return out
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        out = []
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            label = x.get("name", i) if isinstance(x, dict) else i
            out += _differences(x, y, f"{path}[{label}]")
        return out
    return [] if a == b else [path or "<root>"]


def _all_fields(core: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
    vertices = core.get("vertex_config", {}).get("vertices", [])
    edges = core.get("edge_config", {}).get("edges", [])
    return [(v["name"], v.get("properties", [])) for v in vertices] + [
        (f"{e['source']} {e.get('relation')} {e['target']}", e.get("properties", []))
        for e in edges
    ]


def to_compact(schema: Schema) -> dict[str, Any]:
    """The compact form of *schema*.

    Raises:
        CompactUnsupported: when the schema uses anything the notation cannot
            say (inverses, edge identities, filters, IRIs, other field types…);
            the error names each part that would be lost.
    """
    canonical = schema.to_minimal_canonical_dict()
    metadata = canonical.get("metadata", {})
    core = canonical.get("core_schema", {})
    compact: dict[str, Any] = {"name": metadata.get("name")}
    if "version" in metadata:
        compact["version"] = metadata["version"]
    if "description" in metadata:
        compact["about"] = metadata["description"]
    compact["types"] = {
        v["name"]: _compact_type(v)
        for v in core.get("vertex_config", {}).get("vertices", [])
    }
    relations = [
        _compact_relation(e) for e in core.get("edge_config", {}).get("edges", [])
    ]
    if relations:
        compact["relations"] = relations
    untyped = [
        f"{owner}.{field['name']}.type={field['type']}"
        for owner, fields in _all_fields(core)
        for field in fields
        if "type" in field and field["type"] not in _TYPE_NAMES
    ]
    if untyped:
        raise CompactUnsupported(untyped)
    back = from_compact(compact).to_minimal_canonical_dict()
    lost = _differences(canonical, back)
    if lost:
        raise CompactUnsupported(lost)
    return compact
