"""A manifest as Apache Ossie documents: ontology, semantic model, mapping.

**Experimental**: written against Ossie ``0.3.0.dev0`` (:data:`OSSIE_VERSION`),
a draft that has no release yet and may change.

Ossie's ontology layer is fact-based: a property is a relationship from a
concept to a value type, and identity is ``identify_by`` over identifying
relationships. The mapping:

- vertex -> ``EntityType`` concept, ``identity`` -> ``identify_by``,
  ``subclass_of`` (vertex parents) -> ``extends``;
- field -> a relationship whose second role is a value type (built-in, or a
  declared ``ValueType`` for ``UUID`` and for a field with a unit);
- edge without properties or identities -> a relationship under its source
  concept whose second role is the target, with the relation and its declared
  inverse as ``verbalizes`` patterns;
- edge with properties or identities -> a reified ``EntityType``
  ``{source}_{relation}_{target}`` identified by its ``source`` and ``target``
  relationships plus its identity attributes, so graflo's edge key survives.

The semantic model and the mapping cover only resources bound to one SQL
table whose pipeline is plain vertex and edge steps, with every identity field
read straight from a column. Everything Ossie cannot express (transforms,
other connectors, ``symmetric``, indirect edges, filters, the database
profile, ...) is listed in :attr:`OssieExport.losses`; where it is
schema-level it is also carried in ``annotations.graflo``.
"""

from __future__ import annotations

from typing import Any

import yaml
from pydantic import BaseModel
from pydantic import Field as PydanticField
from suthing import slugify

from graflo.architecture.contract.bindings.connectors import TableConnector
from graflo.architecture.contract.ingestion.emits import may_emit
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.steps.models import (
    EdgeActorConfig,
    VertexActorConfig,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types.enums import EdgeType
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field, FieldType, Vertex, field_type_value
from graflo.rdf.iri import _Minter

#: The Ossie specification version these documents declare.
OSSIE_VERSION = "0.3.0.dev0"

_BUILTIN: dict[str, str] = {
    FieldType.INT.value: "Integer",
    FieldType.UINT.value: "Integer",
    FieldType.FLOAT.value: "Float",
    FieldType.DOUBLE.value: "Float",
    FieldType.BOOL.value: "Boolean",
    FieldType.STRING.value: "String",
    FieldType.DATETIME.value: "DateTime",
}

_DATATYPE: dict[str, str] = {
    **_BUILTIN,
    FieldType.UUID.value: "String",
}

Doc = dict[str, Any]


class OssieExport(BaseModel):
    """The three Ossie documents for one manifest, and what they leave out."""

    name: str = PydanticField(description="File stem shared by the documents.")
    ontology: Doc
    semantic_model: Doc | None = PydanticField(
        default=None, description="Absent when no resource maps to a SQL table."
    )
    mapping: Doc | None = None
    losses: list[str] = PydanticField(default_factory=list)

    def files(self) -> dict[str, str]:
        """File name -> text: the YAML documents and ``losses.md``."""
        out = {f"{self.name}.ontology.yaml": _dump(self.ontology)}
        if self.semantic_model is not None:
            out[f"{self.name}.semantic_model.yaml"] = _dump(self.semantic_model)
        if self.mapping is not None:
            out[f"{self.name}.mapping.yaml"] = _dump(self.mapping)
        lines = [f"# Not expressed in Ossie {OSSIE_VERSION}", ""]
        lines += [f"- {loss}" for loss in self.losses] or ["- nothing"]
        out["losses.md"] = "\n".join(lines) + "\n"
        return out


class _NoAliasDumper(yaml.SafeDumper):
    """Writes a repeated object mapping out in full: Ossie readers need no anchors."""

    def ignore_aliases(self, data: Any) -> bool:
        return True


def _dump(doc: Doc) -> str:
    return yaml.dump(doc, Dumper=_NoAliasDumper, sort_keys=False, allow_unicode=True)


def _relation(edge: Edge) -> str:
    return edge.relation or edge.target


def _is_reified(edge: Edge) -> bool:
    return bool(edge.properties or edge.identities)


def _reified_name(edge: Edge) -> str:
    return f"{edge.source}_{_relation(edge)}_{edge.target}"


class _OntologyBuilder:
    def __init__(self, schema: Schema, mint: _Minter, losses: list[str]) -> None:
        self.schema = schema
        self.mint = mint
        self.losses = losses
        self.value_types: dict[str, Doc] = {}
        self.edge_names: dict[tuple[str, str, str | None], str] = {}

    def value_type(self, owner: str, field: Field) -> str:
        type_value = field_type_value(field.type)
        if type_value == FieldType.LIST.value:
            type_value = field_type_value(field.item_type)
        if type_value is None:
            name = "Any"
        elif type_value == FieldType.UUID.value:
            name = "UUID"
            self.value_types.setdefault(
                name, {"concept": name, "type": "ValueType", "extends": ["String"]}
            )
        else:
            name = _BUILTIN[type_value]
        unit = field.semantics.unit if field.semantics is not None else None
        if unit:
            declared = f"{owner}_{field.name}"
            self.value_types[declared] = {
                "concept": declared,
                "type": "ValueType",
                "extends": [name],
                "annotations": {"graflo": {"unit": unit}},
            }
            name = declared
        return name

    def field_relationship(
        self, owner: str, field: Field, *, sole_identity: bool
    ) -> Doc:
        value_type = self.value_type(owner, field)
        relationship: Doc = {
            "name": field.name,
            "roles": [{"concept": value_type}],
            "verbalizes": [f"{{{owner}}} has {field.name} {{{value_type}}}"],
        }
        if sole_identity:
            relationship["multiplicity"] = "OneToOne"
        elif field_type_value(field.type) != FieldType.LIST.value:
            relationship["multiplicity"] = "ManyToOne"
        if field.description:
            relationship["description"] = field.description
        relationship["iri"] = self.mint.field(owner, field)
        return relationship

    def vertex(self, vertex: Vertex) -> Doc:
        concept: Doc = {"concept": vertex.name, "type": "EntityType"}
        if vertex.description:
            concept["description"] = vertex.description
        parents = [p.vertex for p in vertex.subclass_of if p.vertex is not None]
        if parents:
            concept["extends"] = parents
        if vertex.identity and not vertex.blank:
            concept["identify_by"] = list(vertex.identity)
        concept["iri"] = self.mint.vertex(vertex)
        sole = vertex.identity[0] if len(vertex.identity) == 1 else None
        concept["relationships"] = [
            self.field_relationship(vertex.name, f, sole_identity=f.name == sole)
            for f in vertex.properties
        ]
        annotations = self._vertex_annotations(vertex)
        if annotations:
            concept["annotations"] = {"graflo": annotations}
        return concept

    def _vertex_annotations(self, vertex: Vertex) -> Doc:
        out: Doc = {}
        semantics = vertex.semantics
        if semantics is not None:
            for key in ("synonyms", "exact_match", "close_match", "broader"):
                if getattr(semantics, key):
                    out[key] = list(getattr(semantics, key))
        if vertex.secondary_identities:
            out["secondary_identities"] = [
                s.fields for s in vertex.secondary_identities
            ]
            self.losses.append(
                f"vertex `{vertex.name}`: secondary identities (Ossie names one "
                "preferred identifier; kept in annotations)"
            )
        iri_parents = [p.iri for p in vertex.subclass_of if p.iri is not None]
        if iri_parents:
            out["subclass_of"] = iri_parents
            self.losses.append(
                f"vertex `{vertex.name}`: superclasses outside the schema "
                "(kept in annotations)"
            )
        if vertex.filters:
            self.losses.append(f"vertex `{vertex.name}`: filters")
        if vertex.identity_funnel is not None or vertex.hash_identity_properties:
            self.losses.append(f"vertex `{vertex.name}`: derived (hashed) identity")
        return out

    def edge_name(self, edge: Edge) -> str:
        return self.edge_names[edge.edge_id]

    def plain_edge(self, edge: Edge, taken: set[str]) -> Doc:
        relation = _relation(edge)
        name = relation if relation not in taken else f"{relation}_{edge.target}"
        taken.add(name)
        self.edge_names[edge.edge_id] = name
        role: Doc = {"concept": edge.target}
        if edge.source == edge.target:
            role["name"] = "target"
        patterns = [f"{{{edge.source}}} {relation} {{{edge.target}}}"]
        inverse = self.schema.core_schema.edge_config.inverse_of(edge.relation)
        if inverse is not None:
            patterns.append(f"{{{edge.target}}} {inverse} {{{edge.source}}}")
        relationship: Doc = {
            "name": name,
            "roles": [role],
            # A symmetric relation is its own inverse: one pattern, not two.
            "verbalizes": list(dict.fromkeys(patterns)),
        }
        if edge.description:
            relationship["description"] = edge.description
        relationship["iri"] = self.mint.edge(edge)
        annotations = self._edge_annotations(edge)
        if annotations:
            relationship["annotations"] = {"graflo": annotations}
        return relationship

    def reified_edge(self, edge: Edge) -> Doc:
        name = _reified_name(edge)
        self.edge_names[edge.edge_id] = name
        relationships: list[Doc] = []
        for end, concept_name in (("source", edge.source), ("target", edge.target)):
            relationships.append(
                {
                    "name": end,
                    "roles": [{"concept": concept_name}],
                    "verbalizes": [f"{{{name}}} has {end} {{{concept_name}}}"],
                    "multiplicity": "ManyToOne",
                }
            )
        relationships += [
            self.field_relationship(name, f, sole_identity=False)
            for f in edge.properties
        ]
        attributes = {f.name for f in edge.properties}
        identity_attributes = sorted(
            {token for group in edge.identities for token in group} & attributes
        )
        concept: Doc = {
            "concept": name,
            "type": "EntityType",
            "description": edge.description
            or f"{edge.source} {_relation(edge)} {edge.target}",
            "identify_by": ["source", "target", *identity_attributes],
            "iri": self.mint.edge(edge),
            "relationships": relationships,
        }
        annotations = self._edge_annotations(edge)
        annotations["relation"] = _relation(edge)
        concept["annotations"] = {"graflo": annotations}
        return concept

    def _edge_annotations(self, edge: Edge) -> Doc:
        out: Doc = {}
        label = f"edge `{edge.source}` -[{_relation(edge)}]-> `{edge.target}`"
        if self.schema.core_schema.edge_config.is_symmetric(edge.relation):
            out["symmetric"] = True
            self.losses.append(f"{label}: symmetric (kept in annotations)")
        elif not edge.directed:
            out["directed"] = False
            self.losses.append(f"{label}: undirected (kept in annotations)")
        if edge.type == EdgeType.INDIRECT:
            out["type"] = EdgeType.INDIRECT.value
            out["by"] = edge.by
            self.losses.append(f"{label}: indirect, through `{edge.by}`")
        if edge.subproperty_of:
            self.losses.append(f"{label}: superproperties")
        return out

    def build(self) -> Doc:
        concepts: list[Doc] = []
        reified: list[Doc] = []
        vertices = self.schema.core_schema.vertex_config.vertices
        by_name: dict[str, Doc] = {}
        for vertex in vertices:
            concept = self.vertex(vertex)
            by_name[vertex.name] = concept
            concepts.append(concept)
        taken = {
            vertex.name: {f.name for f in vertex.properties} for vertex in vertices
        }
        for edge in self.schema.core_schema.edge_config.edges:
            if _is_reified(edge):
                reified.append(self.reified_edge(edge))
            else:
                by_name[edge.source]["relationships"].append(
                    self.plain_edge(edge, taken[edge.source])
                )
        for concept in concepts:
            if not concept["relationships"]:
                del concept["relationships"]
        metadata = self.schema.metadata
        doc: Doc = {"version": OSSIE_VERSION, "name": metadata.name}
        if metadata.description:
            doc["description"] = metadata.description
        if metadata.namespaces:
            doc["prefixes"] = dict(sorted(metadata.namespaces.items()))
        doc["ontology"] = list(self.value_types.values()) + concepts + reified
        return doc


_STEP_KIND = {
    "TransformActorConfig": "transform",
    "DescendActorConfig": "descend",
    "VertexRouterActorConfig": "vertex_router",
}


def _column_map(step: VertexActorConfig, vertex: Vertex) -> dict[str, str]:
    """Vertex field -> column, for a plain vertex step."""
    if step.from_doc:
        return dict(step.from_doc)
    return {f.name: f.name for f in vertex.properties}


def _unmappable(resource: ResourceConfig) -> str | None:
    for step in resource.pipeline:
        if isinstance(step, VertexActorConfig):
            if step.lookup_only or step.find:
                return "a lookup-only vertex step"
            continue
        if isinstance(step, EdgeActorConfig):
            if step.links is not None or step.source is None or step.target is None:
                return "an edge step whose endpoints come from the data"
            if step.relation_field or step.relation_from_key:
                return "an edge step whose relation comes from the data"
            continue
        return f"a {_STEP_KIND.get(type(step).__name__, 'other')} step"
    return None


def _source(table: TableConnector) -> str:
    return ".".join(
        part for part in (table.database, table.schema_name, table.table_name) if part
    )


class _MappingBuilder:
    def __init__(
        self, manifest: GraphManifest, ontology: _OntologyBuilder, losses: list[str]
    ) -> None:
        self.manifest = manifest
        self.ontology = ontology
        self.losses = losses
        schema = ontology.schema
        self.vertices = {v.name: v for v in schema.core_schema.vertex_config.vertices}
        self.datasets: list[Doc] = []
        self.concepts: dict[str, Doc] = {}

    def _concept(self, name: str) -> Doc:
        return self.concepts.setdefault(name, {"concept": name})

    def _object(self, vertex: Vertex, dataset: str, columns: dict[str, str]) -> Doc:
        return {
            "referent_mappings": [
                {"relationship": field, "expression": f"{dataset}.{columns[field]}"}
                for field in vertex.identity
            ]
        }

    def _value(self, owner: str, field: Field, dataset: str, column: str) -> Doc:
        return {
            "object_mapping": {
                "concept": self.ontology.value_type(owner, field),
                "expression": f"{dataset}.{column}",
            },
            "relationship": field.name,
        }

    def resource(self, resource: ResourceConfig, table: TableConnector) -> None:
        dataset = resource.name
        steps: dict[str, list[dict[str, str]]] = {}
        columns: dict[str, Field | None] = {}
        for step in resource.pipeline:
            if not isinstance(step, VertexActorConfig):
                continue
            vertex = self.vertices[step.vertex]
            mapped = _column_map(step, vertex)
            if not vertex.identity or not set(vertex.identity) <= set(mapped):
                self.losses.append(
                    f"resource `{dataset}`: vertex `{vertex.name}` is not identified "
                    "by columns; not mapped"
                )
                continue
            steps.setdefault(vertex.name, []).append(mapped)
            fields = {f.name: f for f in vertex.properties}
            for field_name, column in mapped.items():
                columns.setdefault(column, fields.get(field_name))
            concept = self._concept(vertex.name)
            om = self._object(vertex, dataset, mapped)
            concept.setdefault("object_mappings", []).append(om)
            children = [
                self._value(vertex.name, fields[name], dataset, column)
                for name, column in mapped.items()
                if name in fields and name not in vertex.identity
            ]
            if children:
                concept.setdefault("link_mappings", []).append(
                    {"object_mapping": om, "children": children}
                )

        produced = set(steps)
        for edge in self.ontology.schema.core_schema.edge_config.edges:
            if not may_emit(resource, edge.edge_id, produced):
                continue
            if edge.source == edge.target and len(steps.get(edge.source, [])) < 2:
                continue  # one reading of a vertex makes no self-edge
            ends = self._endpoints(edge, steps)
            if ends is None:
                self.losses.append(
                    f"resource `{dataset}`: edge `{edge.source}` -[{_relation(edge)}]-> "
                    f"`{edge.target}` has no single reading of its endpoints; not mapped"
                )
                continue
            source_om = self._object(self.vertices[edge.source], dataset, ends[0])
            target_om = self._object(self.vertices[edge.target], dataset, ends[1])
            if _is_reified(edge):
                self._reified(edge, dataset, source_om, target_om, columns)
            else:
                self._concept(edge.source).setdefault("link_mappings", []).append(
                    {
                        "object_mapping": source_om,
                        "children": [
                            {
                                "object_mapping": {
                                    "concept": edge.target,
                                    **target_om,
                                },
                                "relationship": self.ontology.edge_name(edge),
                            }
                        ],
                    }
                )

        fields_doc: list[Doc] = []
        for column, field in columns.items():
            entry: Doc = {"name": column, "expression": {"sql": column}}
            type_value = field_type_value(field.type) if field is not None else None
            if type_value in _DATATYPE:
                entry["datatype"] = _DATATYPE[type_value]
            fields_doc.append(entry)
        dataset_doc: Doc = {"name": dataset, "source": _source(table)}
        if fields_doc:
            dataset_doc["fields"] = fields_doc
        self.datasets.append(dataset_doc)

    def _endpoints(
        self, edge: Edge, steps: dict[str, list[dict[str, str]]]
    ) -> tuple[dict[str, str], dict[str, str]] | None:
        if edge.source == edge.target:
            readings = steps.get(edge.source, [])
            return (readings[0], readings[1]) if len(readings) == 2 else None
        # One reading of each end; several readings of one end is ambiguous.
        sources, targets = steps.get(edge.source, []), steps.get(edge.target, [])
        if len(sources) == 1 and len(targets) == 1:
            return sources[0], targets[0]
        return None

    def _reified(
        self,
        edge: Edge,
        dataset: str,
        source_om: Doc,
        target_om: Doc,
        columns: dict[str, Field | None],
    ) -> None:
        name = _reified_name(edge)
        identity_attributes = {t for group in edge.identities for t in group}
        referents: list[Doc] = [
            {"relationship": "source", **source_om},
            {"relationship": "target", **target_om},
        ]
        children: list[Doc] = []
        for field in edge.properties:
            columns.setdefault(field.name, field)
            if field.name in identity_attributes:
                referents.append(
                    {
                        "relationship": field.name,
                        "expression": f"{dataset}.{field.name}",
                    }
                )
            else:
                children.append(self._value(name, field, dataset, field.name))
        om = {"referent_mappings": referents}
        concept = self._concept(name)
        concept.setdefault("object_mappings", []).append(om)
        if children:
            concept.setdefault("link_mappings", []).append(
                {"object_mapping": om, "children": children}
            )

    def build(self) -> None:
        ingestion = self.manifest.ingestion_model
        bindings = self.manifest.bindings
        if ingestion is None:
            return
        if ingestion.transforms:
            self.losses.append(
                "transforms: "
                + ", ".join(f"`{t.name}`" for t in ingestion.transforms if t.name)
            )
        for resource in ingestion.resources:
            connectors = (
                bindings.get_connectors_for_resource(resource.name) if bindings else []
            )
            if not connectors:
                self.losses.append(f"resource `{resource.name}`: no connector bound")
                continue
            if len(connectors) != 1 or not isinstance(connectors[0], TableConnector):
                kinds = sorted({type(c).__name__ for c in connectors})
                self.losses.append(
                    f"resource `{resource.name}`: bound to {', '.join(kinds)}; "
                    "only a single SQL table maps to Ossie"
                )
                continue
            table = connectors[0]
            reason = _unmappable(resource)
            if reason is None and (table.joins or table.view is not None):
                reason = "a table connector with joins or a view"
            if reason is not None:
                self.losses.append(f"resource `{resource.name}`: {reason}; not mapped")
                continue
            self.resource(resource, table)


def manifest_to_ossie(
    manifest: GraphManifest, base_iri: str | None = None
) -> OssieExport:
    """Write *manifest* as Ossie documents.

    Args:
        manifest: An initialised manifest with a schema.
        base_iri: Overrides ``metadata.base_iri`` for minted IRIs.

    Raises:
        ValueError: the manifest has no schema.
    """
    schema = manifest.graph_schema
    if schema is None:
        raise ValueError("an Ossie export needs a manifest with a schema")
    losses: list[str] = []
    ontology = _OntologyBuilder(schema, _Minter(schema, base_iri), losses)
    ontology_doc = ontology.build()
    mapping = _MappingBuilder(manifest, ontology, losses)
    mapping.build()
    # Declared value types may have grown while mapping attribute values.
    ontology_doc["ontology"] = list(ontology.value_types.values()) + [
        c for c in ontology_doc["ontology"] if c.get("type") == "EntityType"
    ]
    if schema.db_profile.model_dump(exclude_defaults=True):
        losses.append("db_profile: physical storage settings")

    stem = slugify(schema.metadata.name or "", fallback="manifest")
    if not mapping.datasets:
        losses.append(
            "no resource is bound to a single SQL table, so no semantic model "
            "or mapping is written"
        )
        return OssieExport(name=stem, ontology=ontology_doc, losses=losses)

    semantic_name = f"{schema.metadata.name} semantic model"
    semantic_model: Doc = {
        "version": OSSIE_VERSION,
        "name": semantic_name,
        "datasets": mapping.datasets,
    }
    mapping_doc: Doc = {
        "version": OSSIE_VERSION,
        "name": f"{schema.metadata.name} mapping",
        "ontology_ref": {
            "name": schema.metadata.name,
            "iri": f"./{stem}.ontology.yaml",
        },
        "semantic_model_ref": {
            "name": semantic_name,
            "iri": f"./{stem}.semantic_model.yaml",
        },
        "concept_mappings": list(mapping.concepts.values()),
    }
    return OssieExport(
        name=stem,
        ontology=ontology_doc,
        semantic_model=semantic_model,
        mapping=mapping_doc,
        losses=losses,
    )
