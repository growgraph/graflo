"""Serialize GraphManifest instances to RDF."""

from __future__ import annotations

from typing import Any, cast

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
from graflo.architecture.graph_types import EdgeId
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
        vertex_uri_by_name: dict[str, URIRef] | None = None
        edge_uri_by_id: dict[EdgeId, URIRef] | None = None

        if manifest.graph_schema is not None:
            schema_uri = URIRef(join_uri(base_uri, "schema"))
            graph.add((manifest_uri, ns.hasSchema, schema_uri))
            self._emit_schema(graph, schema_uri, manifest.graph_schema)
            core_uri = URIRef(join_uri(str(schema_uri), "core"))
            vertex_uri_by_name = {
                vertex.name: URIRef(join_uri(str(core_uri), "vertex", vertex.name))
                for vertex in manifest.graph_schema.core_schema.vertex_config.vertices
            }
            edge_uri_by_id = {
                edge.edge_id: URIRef(
                    join_uri(str(core_uri), "edge", self._edge_key(edge))
                )
                for edge in manifest.graph_schema.core_schema.edge_config.edges
            }

        if manifest.ingestion_model is not None:
            ingestion_uri = URIRef(join_uri(base_uri, "ingestion"))
            graph.add((manifest_uri, ns.hasIngestionModel, ingestion_uri))
            self._emit_ingestion_model(
                graph,
                base_uri,
                ingestion_uri,
                manifest.ingestion_model,
                vertex_uri_by_name=vertex_uri_by_name,
                edge_uri_by_id=edge_uri_by_id,
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

        vertex_config_uri = URIRef(join_uri(str(core_uri), "vertex-config"))
        graph.add((core_uri, ns.hasVertexConfig, vertex_config_uri))
        graph.add((vertex_config_uri, RDF.type, ns.VertexConfig))
        self._emit_vertex_config(
            graph, vertex_config_uri, schema.core_schema.vertex_config
        )

        edge_config_uri = URIRef(join_uri(str(core_uri), "edge-config"))
        graph.add((core_uri, ns.hasEdgeConfig, edge_config_uri))
        graph.add((edge_config_uri, RDF.type, ns.EdgeConfig))

        vertex_uri_by_name: dict[str, URIRef] = {}
        for index, vertex in enumerate(schema.core_schema.vertex_config.vertices):
            vertex_uri = URIRef(join_uri(str(core_uri), "vertex", vertex.name))
            vertex_uri_by_name[vertex.name] = vertex_uri
            graph.add((vertex_config_uri, ns.hasVertex, vertex_uri))
            add_literal(graph, vertex_uri, ns.artifactIndex, index)
            self._emit_vertex(graph, vertex_uri, vertex)

        edge_uri_by_id: dict[EdgeId, URIRef] = {}
        for index, edge in enumerate(schema.core_schema.edge_config.edges):
            edge_key = self._edge_key(edge)
            edge_uri = URIRef(join_uri(str(core_uri), "edge", edge_key))
            edge_uri_by_id[edge.edge_id] = edge_uri
            graph.add((edge_config_uri, ns.hasEdge, edge_uri))
            add_literal(graph, edge_uri, ns.artifactIndex, index)
            self._emit_edge(graph, edge_uri, edge, vertex_uri_by_name)

        profile_uri = URIRef(join_uri(str(schema_uri), "db-profile"))
        graph.add((schema_uri, ns.hasDatabaseProfile, profile_uri))
        self._emit_database_profile(
            graph, profile_uri, schema.db_profile, edge_uri_by_id=edge_uri_by_id
        )

    def _emit_vertex_config(
        self,
        graph: Graph,
        vertex_config_uri: URIRef,
        vertex_config: Any,
    ) -> None:
        if vertex_config.force_types:
            graph.add(
                (
                    vertex_config_uri,
                    ns.forceTypes,
                    json_literal(vertex_config.force_types),
                )
            )
        add_literal(
            graph,
            vertex_config_uri,
            ns.identityFromAllProperties,
            vertex_config.identity_from_all_properties,
        )

    def _emit_vertex(self, graph: Graph, vertex_uri: URIRef, vertex: Vertex) -> None:
        graph.add((vertex_uri, RDF.type, ns.Vertex))
        add_literal(graph, vertex_uri, ns.name, vertex.name)
        add_literal(graph, vertex_uri, ns.description, vertex.description)
        add_literal(graph, vertex_uri, ns.blank, vertex.blank)

        for identity in vertex.identity:
            identity_node = BNode()
            graph.add((vertex_uri, ns.hasIdentity, identity_node))
            graph.add((identity_node, RDF.type, ns.Identity))
            add_literal(graph, identity_node, ns.identityName, identity)

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
        graph.add((field_uri, RDF.type, ns.Field))
        add_literal(graph, field_uri, ns.artifactIndex, index)
        add_literal(graph, field_uri, ns.name, field_obj.name)
        add_literal(graph, field_uri, ns.description, field_obj.description)
        if field_obj.type is not None:
            add_enum_individual(
                graph,
                field_uri,
                ns.fieldType,
                str(field_obj.type),
                ns.ENUM_REGISTRIES["field_type"],
            )

    def _emit_edge(
        self,
        graph: Graph,
        edge_uri: URIRef,
        edge: Edge,
        vertex_uri_by_name: dict[str, URIRef],
    ) -> None:
        graph.add((edge_uri, RDF.type, ns.Edge))
        add_literal(graph, edge_uri, ns.relation, edge.relation)
        add_literal(graph, edge_uri, ns.description, edge.description)

        source_uri = vertex_uri_by_name.get(edge.source)
        target_uri = vertex_uri_by_name.get(edge.target)
        if source_uri is not None:
            graph.add((edge_uri, ns.edgeSource, source_uri))
        if target_uri is not None:
            graph.add((edge_uri, ns.edgeTarget, target_uri))

        payload: dict[str, Any] = {}
        if not edge.directed:
            payload["directed"] = edge.directed
        if edge.identities:
            graph.add((edge_uri, ns.edgeIdentities, json_literal(edge.identities)))
        if edge.type is not None:
            add_literal(graph, edge_uri, ns.edgeType, str(edge.type))
        if edge.by is not None:
            add_literal(graph, edge_uri, ns.edgeBy, edge.by)
        if payload:
            graph.add((edge_uri, ns.edgePayload, json_literal(payload)))

        for index, field in enumerate(edge.properties):
            self._emit_field(graph, edge_uri, field, index)

    def _emit_database_profile(
        self,
        graph: Graph,
        profile_uri: URIRef,
        profile: Any,
        *,
        edge_uri_by_id: dict[EdgeId, URIRef] | None = None,
    ) -> None:
        graph.add((profile_uri, RDF.type, ns.DatabaseProfile))
        add_enum_individual(
            graph,
            profile_uri,
            ns.dbFlavor,
            str(profile.db_flavor),
            ns.ENUM_REGISTRIES["db_type"],
        )
        add_literal(graph, profile_uri, ns.targetNamespace, profile.target_namespace)
        self._emit_profile_indexes(
            graph, profile_uri, profile, edge_uri_by_id=edge_uri_by_id
        )

        payload = self._model_payload(
            profile, ns.MODEL_PAYLOAD_EXCLUDES["database_profile"]
        )
        if payload:
            graph.add((profile_uri, ns.profilePayload, json_literal(payload)))

    def _emit_profile_indexes(
        self,
        graph: Graph,
        profile_uri: URIRef,
        profile: Any,
        *,
        edge_uri_by_id: dict[EdgeId, URIRef] | None = None,
    ) -> None:
        for vertex_name, indexes in profile.vertex_indexes.items():
            for index_position, index in enumerate(indexes):
                index_uri = URIRef(
                    join_uri(
                        str(profile_uri),
                        "vertex-index",
                        vertex_name,
                        str(index_position),
                    )
                )
                graph.add((profile_uri, ns.hasVertexIndex, index_uri))
                self._emit_index(
                    graph,
                    index_uri,
                    index,
                    vertex_name=vertex_name,
                )

        for spec_position, edge_spec in enumerate(profile.edge_specs):
            spec_uri = URIRef(
                join_uri(str(profile_uri), "edge-spec", str(spec_position))
            )
            graph.add((profile_uri, ns.hasEdgeSpec, spec_uri))
            graph.add((spec_uri, RDF.type, ns.EdgePhysicalSpec))
            add_literal(graph, spec_uri, ns.specSource, edge_spec.source)
            add_literal(graph, spec_uri, ns.specTarget, edge_spec.target)
            add_literal(graph, spec_uri, ns.specRelation, edge_spec.relation)
            add_literal(graph, spec_uri, ns.specPurpose, edge_spec.purpose)
            add_literal(graph, spec_uri, ns.specRelationName, edge_spec.relation_name)
            add_literal(graph, spec_uri, ns.specIndexesMode, edge_spec.indexes_mode)
            if edge_uri_by_id is not None:
                edge_uri = edge_uri_by_id.get(
                    (edge_spec.source, edge_spec.target, edge_spec.relation)
                )
                if edge_uri is not None:
                    graph.add((spec_uri, ns.refinesEdge, edge_uri))

            for index_position, index in enumerate(edge_spec.indexes):
                index_uri = URIRef(
                    join_uri(str(spec_uri), "index", str(index_position))
                )
                graph.add((spec_uri, ns.hasIndex, index_uri))
                self._emit_index(graph, index_uri, index)

    def _emit_index(
        self,
        graph: Graph,
        index_uri: URIRef,
        index: Any,
        *,
        vertex_name: str | None = None,
    ) -> None:
        graph.add((index_uri, RDF.type, ns.Index))
        add_literal(graph, index_uri, ns.profileVertexName, vertex_name)
        add_literal(graph, index_uri, ns.indexName, index.name)
        add_literal(graph, index_uri, ns.indexUnique, index.unique)
        index_type = getattr(index.type, "value", str(index.type))
        add_literal(graph, index_uri, ns.indexType, index_type)
        add_literal(graph, index_uri, ns.indexDeduplicate, index.deduplicate)
        add_literal(graph, index_uri, ns.indexSparse, index.sparse)
        add_literal(
            graph,
            index_uri,
            ns.indexExcludeEdgeEndpoints,
            index.exclude_edge_endpoints,
        )
        for field in index.fields:
            add_literal(graph, index_uri, ns.indexField, field)

    def _emit_ingestion_model(
        self,
        graph: Graph,
        base_uri: str,
        ingestion_uri: URIRef,
        ingestion_model: Any,
        *,
        vertex_uri_by_name: dict[str, URIRef] | None = None,
        edge_uri_by_id: dict[EdgeId, URIRef] | None = None,
    ) -> None:
        graph.add((ingestion_uri, RDF.type, ns.IngestionModel))
        add_enum_individual(
            graph,
            ingestion_uri,
            ns.edgesOnDuplicate,
            ingestion_model.edges_on_duplicate,
            ns.ENUM_REGISTRIES["edge_duplicate_policy"],
        )

        transform_uri_by_name: dict[str, URIRef] = {}
        for index, transform in enumerate(ingestion_model.transforms):
            transform_uri = URIRef(
                join_uri(
                    base_uri, "ingestion", "transform", transform.name or "unnamed"
                )
            )
            if transform.name:
                transform_uri_by_name[transform.name] = transform_uri
            graph.add((ingestion_uri, ns.hasTransform, transform_uri))
            add_literal(graph, transform_uri, ns.artifactIndex, index)
            self._emit_proto_transform(graph, transform_uri, transform)

        for index, resource in enumerate(ingestion_model.resources):
            resource_uri = URIRef(
                join_uri(base_uri, "ingestion", "resource", resource.name)
            )
            graph.add((ingestion_uri, ns.hasResource, resource_uri))
            add_literal(graph, resource_uri, ns.artifactIndex, index)
            self._emit_resource(
                graph,
                resource_uri,
                resource,
                transform_uri_by_name=transform_uri_by_name,
                vertex_uri_by_name=vertex_uri_by_name,
                edge_uri_by_id=edge_uri_by_id,
            )

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
            ns.ENUM_REGISTRIES["transform_target"],
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
            ns.ENUM_REGISTRIES["key_selection_mode"],
        )
        for key_name in keys.names:
            add_literal(graph, keys_uri, ns.keySelectionName, key_name)

    def _emit_resource(
        self,
        graph: Graph,
        resource_uri: URIRef,
        resource: Any,
        *,
        transform_uri_by_name: dict[str, URIRef] | None = None,
        vertex_uri_by_name: dict[str, URIRef] | None = None,
        edge_uri_by_id: dict[EdgeId, URIRef] | None = None,
    ) -> None:
        graph.add((resource_uri, RDF.type, ns.Resource))
        add_literal(graph, resource_uri, ns.name, resource.name)

        payload = self._model_payload(resource, ns.MODEL_PAYLOAD_EXCLUDES["resource"])
        if payload:
            graph.add((resource_uri, ns.resourcePayload, json_literal(payload)))

        for index, step in enumerate(resource.pipeline):
            step_node = self._emit_actor_step(
                graph,
                step,
                index=index,
                transform_uri_by_name=transform_uri_by_name,
                vertex_uri_by_name=vertex_uri_by_name,
                edge_uri_by_id=edge_uri_by_id,
            )
            graph.add((resource_uri, ns.hasActor, step_node))

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

    def _emit_actor_step(
        self,
        graph: Graph,
        step: dict[str, Any],
        *,
        index: int,
        transform_uri_by_name: dict[str, URIRef] | None = None,
        vertex_uri_by_name: dict[str, URIRef] | None = None,
        edge_uri_by_id: dict[EdgeId, URIRef] | None = None,
    ) -> BNode:
        step_node = BNode()
        step_type = actor_step_type(step)
        graph.add((step_node, RDF.type, URIRef(str(actor_step_class(step_type)))))
        graph.add((step_node, RDF.type, ns.Actor))
        add_literal(graph, step_node, ns.actorType, step_type)
        add_literal(graph, step_node, ns.stepIndex, index)
        graph.add((step_node, ns.stepPayload, json_literal(step)))
        if step_type == "vertex":
            vertex_name = step.get("vertex")
            if (
                isinstance(vertex_name, str)
                and vertex_uri_by_name is not None
                and vertex_name in vertex_uri_by_name
            ):
                graph.add(
                    (step_node, ns.targetsVertex, vertex_uri_by_name[vertex_name])
                )
        if step_type == "vertex_router" and vertex_uri_by_name is not None:
            type_map = step.get("type_map")
            if isinstance(type_map, dict):
                for mapped in type_map.values():
                    if isinstance(mapped, str) and mapped in vertex_uri_by_name:
                        graph.add(
                            (step_node, ns.targetsVertex, vertex_uri_by_name[mapped])
                        )
        if step_type == "edge" and edge_uri_by_id is not None:

            def _link_edge(source: str, target: str, relation: str | None) -> None:
                edge_uri = edge_uri_by_id.get((source, target, relation))
                if edge_uri is not None:
                    graph.add((step_node, ns.targetsEdge, edge_uri))

            source = step.get("from")
            target = step.get("to")
            relation = step.get("relation")
            if isinstance(source, str) and isinstance(target, str):
                _link_edge(
                    source, target, relation if isinstance(relation, str) else None
                )
            links = step.get("links")
            if isinstance(links, list):
                for link in links:
                    if not isinstance(link, dict):
                        continue
                    link_source = link.get("from")
                    link_target = link.get("to")
                    link_relation = link.get("relation")
                    if isinstance(link_source, str) and isinstance(link_target, str):
                        _link_edge(
                            link_source,
                            link_target,
                            link_relation if isinstance(link_relation, str) else None,
                        )
        if step_type == "transform":
            transform_name = step.get("name")
            if not isinstance(transform_name, str):
                call_spec = step.get("call")
                if isinstance(call_spec, dict):
                    use_name = call_spec.get("use")
                    if isinstance(use_name, str):
                        transform_name = use_name
            if not isinstance(transform_name, str):
                transform_step = step.get("transform")
                if isinstance(transform_step, dict):
                    direct_name = transform_step.get("name")
                    if isinstance(direct_name, str):
                        transform_name = direct_name
                    nested_call = transform_step.get("call")
                    if not isinstance(transform_name, str) and isinstance(
                        nested_call, dict
                    ):
                        nested_use = nested_call.get("use")
                        if isinstance(nested_use, str):
                            transform_name = nested_use
            if isinstance(transform_name, str) and transform_uri_by_name is not None:
                transform_uri = transform_uri_by_name.get(transform_name)
                if transform_uri is not None:
                    graph.add((step_node, ns.executesTransform, transform_uri))

        if step_type == "descend":
            nested_steps = step.get("pipeline")
            if isinstance(nested_steps, list):
                for nested_index, nested_step in enumerate(nested_steps):
                    if isinstance(nested_step, dict):
                        nested_node = self._emit_actor_step(
                            graph,
                            cast(dict[str, Any], nested_step),
                            index=nested_index,
                            transform_uri_by_name=transform_uri_by_name,
                            vertex_uri_by_name=vertex_uri_by_name,
                            edge_uri_by_id=edge_uri_by_id,
                        )
                        graph.add((step_node, ns.hasActor, nested_node))
        return step_node

    def _emit_bindings(
        self,
        graph: Graph,
        base_uri: str,
        bindings_uri: URIRef,
        bindings: Any,
    ) -> None:
        graph.add((bindings_uri, RDF.type, ns.Bindings))

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
            ns.ENUM_REGISTRIES["bound_source_kind"],
        )

        payload = self._model_payload(connector, ns.MODEL_PAYLOAD_EXCLUDES["connector"])
        if payload:
            graph.add((connector_uri, ns.connectorPayload, json_literal(payload)))

    @staticmethod
    def _model_payload(model: Any, exclude_fields: set[str]) -> dict[str, Any]:
        payload = model.model_dump(
            mode="json",
            by_alias=True,
            exclude=exclude_fields,
            exclude_none=True,
            exclude_defaults=True,
        )
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _edge_key(edge: Edge) -> str:
        relation = edge.relation or "relates"
        return f"{edge.source}_{relation}_{edge.target}"
