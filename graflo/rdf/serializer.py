"""Serialize GraphManifest instances to RDF."""

from __future__ import annotations

from typing import Any

from rdflib import BNode, Graph, RDF, URIRef
from rdflib.namespace import XSD

from graflo.architecture.contract.bindings.connectors import (
    FileConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.ingestion.transform import (
    DressConfig,
    KeySelectionConfig,
    ProtoTransform,
)
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Field, Vertex
from graflo.rdf import namespace as ns
from graflo.rdf.utils import (
    add_enum_individual,
    add_literal,
    actor_step_class,
    actor_step_type,
    join_uri,
    json_literal,
    load_ontology_graph,
)


class ManifestRdfSerializer:
    """Convert a :class:`GraphManifest` into RDF using the GraFlo meta-ontology."""

    def __init__(self, *, include_ontology: bool = True) -> None:
        self._include_ontology = include_ontology

    def to_graph(self, manifest: GraphManifest, base_uri: str) -> Graph:
        """Serialize manifest to an rdflib graph."""
        graph = Graph()
        graph.bind("gf", ns.GF)
        graph.bind("xsd", XSD)
        if self._include_ontology:
            graph += load_ontology_graph()

        manifest_uri = URIRef(base_uri.rstrip("/"))
        graph.add((manifest_uri, RDF.type, ns.GraphManifest))

        if manifest.graph_schema is not None:
            schema_uri = URIRef(join_uri(base_uri, "schema"))
            graph.add((manifest_uri, ns.hasSchema, schema_uri))
            self._emit_schema(graph, schema_uri, manifest.graph_schema)

        if manifest.ingestion_model is not None:
            ingestion_uri = URIRef(join_uri(base_uri, "ingestion"))
            graph.add((manifest_uri, ns.hasIngestionModel, ingestion_uri))
            self._emit_ingestion_model(
                graph, base_uri, ingestion_uri, manifest.ingestion_model
            )

        if manifest.bindings is not None:
            bindings_uri = URIRef(join_uri(base_uri, "bindings"))
            graph.add((manifest_uri, ns.hasBindings, bindings_uri))
            self._emit_bindings(graph, base_uri, bindings_uri, manifest.bindings)

        return graph

    def to_turtle(self, manifest: GraphManifest, base_uri: str) -> str:
        """Serialize manifest to Turtle."""
        return self.to_graph(manifest, base_uri).serialize(format="turtle")

    def to_json_ld(self, manifest: GraphManifest, base_uri: str) -> str:
        """Serialize manifest to JSON-LD."""
        return self.to_graph(manifest, base_uri).serialize(format="json-ld")

    def _emit_schema(self, graph: Graph, schema_uri: URIRef, schema: Any) -> None:
        graph.add((schema_uri, RDF.type, ns.Schema))

        metadata_uri = URIRef(join_uri(str(schema_uri), "metadata"))
        graph.add((schema_uri, ns.hasMetadata, metadata_uri))
        graph.add((metadata_uri, RDF.type, ns.GraphMetadata))
        add_literal(graph, metadata_uri, ns.name, schema.metadata.name)
        add_literal(graph, metadata_uri, ns.version, schema.metadata.version)
        add_literal(graph, metadata_uri, ns.description, schema.metadata.description)

        core_uri = URIRef(join_uri(str(schema_uri), "core"))
        graph.add((schema_uri, ns.hasCoreSchema, core_uri))
        graph.add((core_uri, RDF.type, ns.CoreSchema))

        vertex_uri_by_name: dict[str, URIRef] = {}
        for index, vertex in enumerate(schema.core_schema.vertex_config.vertices):
            vertex_uri = URIRef(join_uri(str(core_uri), "vertex", vertex.name))
            vertex_uri_by_name[vertex.name] = vertex_uri
            graph.add((core_uri, ns.definesVertex, vertex_uri))
            add_literal(graph, vertex_uri, ns.artifactIndex, index)
            self._emit_vertex(graph, vertex_uri, vertex)

        for index, edge in enumerate(schema.core_schema.edge_config.edges):
            edge_key = self._edge_key(edge)
            edge_uri = URIRef(join_uri(str(core_uri), "edge", edge_key))
            graph.add((core_uri, ns.definesEdge, edge_uri))
            add_literal(graph, edge_uri, ns.artifactIndex, index)
            self._emit_edge(graph, edge_uri, edge, vertex_uri_by_name)

        profile_uri = URIRef(join_uri(str(schema_uri), "db-profile"))
        graph.add((schema_uri, ns.hasDatabaseProfile, profile_uri))
        self._emit_database_profile(graph, profile_uri, schema.db_profile)

    def _emit_vertex(self, graph: Graph, vertex_uri: URIRef, vertex: Vertex) -> None:
        graph.add((vertex_uri, RDF.type, ns.VertexDefinition))
        add_literal(graph, vertex_uri, ns.name, vertex.name)
        add_literal(graph, vertex_uri, ns.description, vertex.description)
        add_literal(graph, vertex_uri, ns.blank, vertex.blank)

        for identity in vertex.identity:
            add_literal(graph, vertex_uri, ns.identityField, identity)

        payload = {}
        if vertex.filters:
            payload["filters"] = [
                f.model_dump(mode="json", by_alias=True)
                if hasattr(f, "model_dump")
                else f
                for f in vertex.filters
            ]
        if payload:
            graph.add((vertex_uri, ns.vertexPayload, json_literal(payload)))

        for index, field in enumerate(vertex.properties):
            self._emit_field(graph, vertex_uri, field, index)

    def _emit_field(
        self,
        graph: Graph,
        owner_uri: URIRef,
        field: Field | str,
        index: int,
    ) -> None:
        if isinstance(field, str):
            field_obj = Field(name=field)
        else:
            field_obj = field
        field_uri = URIRef(join_uri(str(owner_uri), "field", field_obj.name))
        graph.add((owner_uri, ns.hasField, field_uri))
        graph.add((field_uri, RDF.type, ns.FieldDefinition))
        add_literal(graph, field_uri, ns.artifactIndex, index)
        add_literal(graph, field_uri, ns.name, field_obj.name)
        add_literal(graph, field_uri, ns.description, field_obj.description)
        if field_obj.type is not None:
            add_enum_individual(
                graph,
                field_uri,
                ns.fieldType,
                str(field_obj.type),
                ns.FIELD_TYPE_INDIVIDUALS,
            )

    def _emit_edge(
        self,
        graph: Graph,
        edge_uri: URIRef,
        edge: Edge,
        vertex_uri_by_name: dict[str, URIRef],
    ) -> None:
        graph.add((edge_uri, RDF.type, ns.EdgeDefinition))
        add_literal(graph, edge_uri, ns.relation, edge.relation)
        add_literal(graph, edge_uri, ns.description, edge.description)

        source_uri = vertex_uri_by_name.get(edge.source)
        target_uri = vertex_uri_by_name.get(edge.target)
        if source_uri is not None:
            graph.add((edge_uri, ns.edgeSource, source_uri))
        if target_uri is not None:
            graph.add((edge_uri, ns.edgeTarget, target_uri))

        payload: dict[str, Any] = {}
        if edge.identities:
            payload["identities"] = edge.identities
        if edge.type is not None:
            payload["type"] = str(edge.type)
        if edge.by is not None:
            payload["by"] = edge.by
        if payload:
            graph.add((edge_uri, ns.edgePayload, json_literal(payload)))

        for index, field in enumerate(edge.properties):
            self._emit_field(graph, edge_uri, field, index)

    def _emit_database_profile(
        self, graph: Graph, profile_uri: URIRef, profile: Any
    ) -> None:
        graph.add((profile_uri, RDF.type, ns.DatabaseProfile))
        add_enum_individual(
            graph,
            profile_uri,
            ns.dbFlavor,
            str(profile.db_flavor),
            ns.DB_TYPE_INDIVIDUALS,
        )
        add_literal(graph, profile_uri, ns.targetNamespace, profile.target_namespace)

        payload = profile.model_dump(
            mode="json",
            by_alias=True,
            exclude={"db_flavor", "target_namespace"},
            exclude_none=True,
            exclude_defaults=True,
        )
        if payload:
            graph.add((profile_uri, ns.profilePayload, json_literal(payload)))

    def _emit_ingestion_model(
        self,
        graph: Graph,
        base_uri: str,
        ingestion_uri: URIRef,
        ingestion_model: Any,
    ) -> None:
        graph.add((ingestion_uri, RDF.type, ns.IngestionModel))
        add_enum_individual(
            graph,
            ingestion_uri,
            ns.edgesOnDuplicate,
            ingestion_model.edges_on_duplicate,
            ns.EDGE_DUPLICATE_POLICY_INDIVIDUALS,
        )

        for index, transform in enumerate(ingestion_model.transforms):
            transform_uri = URIRef(
                join_uri(
                    base_uri, "ingestion", "transform", transform.name or "unnamed"
                )
            )
            graph.add((ingestion_uri, ns.hasTransform, transform_uri))
            add_literal(graph, transform_uri, ns.artifactIndex, index)
            self._emit_proto_transform(graph, transform_uri, transform)

        for index, resource in enumerate(ingestion_model.resources):
            resource_uri = URIRef(
                join_uri(base_uri, "ingestion", "resource", resource.name)
            )
            graph.add((ingestion_uri, ns.hasResource, resource_uri))
            add_literal(graph, resource_uri, ns.artifactIndex, index)
            self._emit_resource(graph, resource_uri, resource)

    def _emit_proto_transform(
        self,
        graph: Graph,
        transform_uri: URIRef,
        transform: ProtoTransform,
    ) -> None:
        graph.add((transform_uri, RDF.type, ns.ProtoTransform))
        add_literal(graph, transform_uri, ns.name, transform.name)
        add_literal(graph, transform_uri, ns.transformModule, transform.module)
        add_literal(graph, transform_uri, ns.transformFunction, transform.foo)

        for item in transform.input:
            add_literal(graph, transform_uri, ns.transformInput, item)
        for item in transform.output:
            add_literal(graph, transform_uri, ns.transformOutput, item)

        extra_payload: dict[str, Any] = {}
        if transform.params:
            extra_payload["params"] = transform.params
        if transform.input_groups:
            extra_payload["input_groups"] = [
                list(group) for group in transform.input_groups
            ]
        if transform.output_groups:
            extra_payload["output_groups"] = [
                list(group) for group in transform.output_groups
            ]
        if extra_payload:
            graph.add((transform_uri, ns.transformParams, json_literal(extra_payload)))

        add_enum_individual(
            graph,
            transform_uri,
            ns.transformTarget,
            transform.target,
            ns.TRANSFORM_TARGET_INDIVIDUALS,
        )

        if transform.dress is not None:
            dress_uri = BNode()
            graph.add((transform_uri, ns.hasDress, dress_uri))
            self._emit_dress_config(graph, dress_uri, transform.dress)

        if transform.keys.mode != "all" or transform.keys.names:
            keys_uri = BNode()
            graph.add((transform_uri, ns.hasKeySelection, keys_uri))
            self._emit_key_selection(graph, keys_uri, transform.keys)

    def _emit_dress_config(
        self, graph: Graph, dress_uri: BNode, dress: DressConfig
    ) -> None:
        graph.add((dress_uri, RDF.type, ns.DressConfig))
        add_literal(graph, dress_uri, ns.dressKey, dress.key)
        add_literal(graph, dress_uri, ns.dressValue, dress.value)

    def _emit_key_selection(
        self,
        graph: Graph,
        keys_uri: BNode,
        keys: KeySelectionConfig,
    ) -> None:
        graph.add((keys_uri, RDF.type, ns.KeySelectionConfig))
        add_enum_individual(
            graph,
            keys_uri,
            ns.keySelectionMode,
            keys.mode,
            ns.KEY_SELECTION_MODE_INDIVIDUALS,
        )
        for key_name in keys.names:
            add_literal(graph, keys_uri, ns.keySelectionName, key_name)

    def _emit_resource(self, graph: Graph, resource_uri: URIRef, resource: Any) -> None:
        graph.add((resource_uri, RDF.type, ns.ResourceDefinition))
        add_literal(graph, resource_uri, ns.name, resource.name)

        payload = resource.model_dump(
            mode="json",
            by_alias=True,
            exclude={"name", "pipeline"},
            exclude_none=True,
            exclude_defaults=True,
        )
        if payload:
            graph.add((resource_uri, ns.resourcePayload, json_literal(payload)))

        for index, step in enumerate(resource.pipeline):
            step_node = BNode()
            graph.add((resource_uri, ns.hasPipelineStep, step_node))
            step_type = actor_step_type(step)
            graph.add((step_node, RDF.type, URIRef(str(actor_step_class(step_type)))))
            add_literal(graph, step_node, ns.actorType, step_type)
            add_literal(graph, step_node, ns.stepIndex, index)
            graph.add((step_node, ns.stepPayload, json_literal(step)))

        for spec in resource.infer_edge_only:
            spec_uri = BNode()
            graph.add((resource_uri, ns.hasEdgeInferOnly, spec_uri))
            self._emit_edge_infer_spec(graph, spec_uri, spec)
        for spec in resource.infer_edge_except:
            spec_uri = BNode()
            graph.add((resource_uri, ns.hasEdgeInferExcept, spec_uri))
            self._emit_edge_infer_spec(graph, spec_uri, spec)

    def _emit_edge_infer_spec(self, graph: Graph, spec_uri: BNode, spec: Any) -> None:
        graph.add((spec_uri, RDF.type, ns.EdgeInferSpec))
        graph.add(
            (
                spec_uri,
                ns.stepPayload,
                json_literal(
                    {
                        "source": spec.source,
                        "target": spec.target,
                        "relation": spec.relation,
                    }
                ),
            )
        )

    def _emit_bindings(
        self,
        graph: Graph,
        base_uri: str,
        bindings_uri: URIRef,
        bindings: Any,
    ) -> None:
        graph.add((bindings_uri, RDF.type, ns.BindingsConfig))

        for index, connector in enumerate(bindings.connectors):
            connector_hash = connector.hash or connector.__class__.__name__
            connector_uri = URIRef(
                join_uri(base_uri, "bindings", "connector", connector_hash)
            )
            graph.add((bindings_uri, ns.hasConnector, connector_uri))
            add_literal(graph, connector_uri, ns.artifactIndex, index)
            self._emit_connector(graph, connector_uri, connector)

        for mapping in bindings.resource_connector:
            binding_uri = BNode()
            graph.add((bindings_uri, ns.bindsResourceToConnector, binding_uri))
            graph.add((binding_uri, RDF.type, ns.ResourceConnectorBinding))
            resource = (
                mapping.resource
                if hasattr(mapping, "resource")
                else mapping["resource"]
            )
            connector = (
                mapping.connector
                if hasattr(mapping, "connector")
                else mapping["connector"]
            )
            add_literal(graph, binding_uri, ns.resourceName, resource)
            add_literal(graph, binding_uri, ns.connectorName, connector)

        for mapping in bindings.connector_connection:
            binding_uri = BNode()
            graph.add((bindings_uri, ns.bindsConnectorToConnProxy, binding_uri))
            graph.add((binding_uri, RDF.type, ns.ConnectorConnectionBinding))
            connector = (
                mapping.connector
                if hasattr(mapping, "connector")
                else mapping["connector"]
            )
            proxy = (
                mapping.conn_proxy
                if hasattr(mapping, "conn_proxy")
                else mapping["conn_proxy"]
            )
            add_literal(graph, binding_uri, ns.connectorName, connector)
            add_literal(graph, binding_uri, ns.connProxy, proxy)

        for mapping in bindings.staging_proxy:
            binding_uri = BNode()
            graph.add((bindings_uri, ns.hasStagingProxy, binding_uri))
            graph.add((binding_uri, RDF.type, ns.StagingProxyBinding))
            name = mapping.name if hasattr(mapping, "name") else mapping["name"]
            proxy = (
                mapping.conn_proxy
                if hasattr(mapping, "conn_proxy")
                else mapping["conn_proxy"]
            )
            add_literal(graph, binding_uri, ns.name, name)
            add_literal(graph, binding_uri, ns.connProxy, proxy)

    def _emit_connector(
        self,
        graph: Graph,
        connector_uri: URIRef,
        connector: FileConnector | TableConnector | SparqlConnector,
    ) -> None:
        connector_class = ns.CONNECTOR_CLASSES[type(connector).__name__]
        graph.add((connector_uri, RDF.type, URIRef(str(connector_class))))
        graph.add((connector_uri, RDF.type, ns.BoundConnector))
        add_literal(graph, connector_uri, ns.name, connector.name)
        add_literal(graph, connector_uri, ns.resourceName, connector.resource_name)
        add_enum_individual(
            graph,
            connector_uri,
            ns.boundSourceKind,
            connector.bound_source_kind().value,
            ns.BOUND_SOURCE_KIND_INDIVIDUALS,
        )

        payload = connector.model_dump(
            mode="json",
            by_alias=True,
            exclude={"hash", "name", "resource_name"},
            exclude_none=True,
            exclude_defaults=True,
        )
        if payload:
            graph.add((connector_uri, ns.connectorPayload, json_literal(payload)))

    @staticmethod
    def _edge_key(edge: Edge) -> str:
        relation = edge.relation or "relates"
        return f"{edge.source}_{relation}_{edge.target}"
