"""Deserialize RDF graphs into GraphManifest instances."""

from __future__ import annotations

from typing import Any

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF

from graflo.architecture.contract.manifest import GraphManifest
from graflo.rdf import namespace as ns
from graflo.rdf.utils import parse_json_literal, reverse_enum


class ManifestRdfDeserializer:
    """Reconstruct a :class:`GraphManifest` from RDF using the GraFlo meta-ontology."""

    def from_turtle(self, ttl: str, manifest_uri: str) -> GraphManifest:
        """Load Turtle and deserialize."""
        graph = Graph()
        graph.parse(data=ttl, format="turtle")
        return self.from_graph(graph, manifest_uri)

    def from_graph(self, graph: Graph, manifest_uri: str) -> GraphManifest:
        """Deserialize manifest from an rdflib graph."""
        manifest_ref = URIRef(manifest_uri.rstrip("/"))
        payload: dict[str, Any] = {}

        schema_uri = self._object(graph, manifest_ref, ns.hasSchema)
        if schema_uri is not None:
            payload["schema"] = self._parse_schema(graph, schema_uri)

        ingestion_uri = self._object(graph, manifest_ref, ns.hasIngestionModel)
        if ingestion_uri is not None:
            payload["ingestion_model"] = self._parse_ingestion_model(
                graph, ingestion_uri
            )

        bindings_uri = self._object(graph, manifest_ref, ns.hasBindings)
        if bindings_uri is not None:
            payload["bindings"] = self._parse_bindings(graph, bindings_uri)

        return GraphManifest.from_dict(payload)

    def _parse_schema(self, graph: Graph, schema_uri: URIRef | BNode) -> dict[str, Any]:
        metadata_uri = self._object(graph, schema_uri, ns.hasMetadata)
        core_uri = self._object(graph, schema_uri, ns.hasCoreSchema)
        profile_uri = self._object(graph, schema_uri, ns.hasDatabaseProfile)

        schema: dict[str, Any] = {}
        if metadata_uri is not None:
            schema["metadata"] = {
                "name": self._literal(graph, metadata_uri, ns.name),
                "version": self._literal(graph, metadata_uri, ns.version),
                "description": self._literal(graph, metadata_uri, ns.description),
            }
            schema["metadata"] = {
                key: value
                for key, value in schema["metadata"].items()
                if value is not None
            }

        if core_uri is not None:
            schema["core_schema"] = self._parse_core_schema(graph, core_uri)

        if profile_uri is not None:
            schema["db_profile"] = self._parse_database_profile(graph, profile_uri)

        return schema

    def _parse_core_schema(
        self, graph: Graph, core_uri: URIRef | BNode
    ) -> dict[str, Any]:
        vertex_config_uri = self._object(graph, core_uri, ns.hasVertexConfig)
        edge_config_uri = self._object(graph, core_uri, ns.hasEdgeConfig)
        vertices = self._ordered_nodes(
            graph,
            vertex_config_uri,
            ns.hasVertex,
            self._parse_vertex,
        )
        edges = self._ordered_nodes(
            graph,
            edge_config_uri,
            ns.hasEdge,
            self._parse_edge,
        )
        return {
            "vertex_config": self._parse_vertex_config(
                graph, vertex_config_uri, vertices
            ),
            "edge_config": {"edges": edges},
        }

    def _parse_vertex_config(
        self,
        graph: Graph,
        vertex_config_uri: URIRef | BNode | None,
        vertices: list[dict[str, Any]],
    ) -> dict[str, Any]:
        vertex_config: dict[str, Any] = {"vertices": vertices}
        force_types = parse_json_literal(
            self._literal(graph, vertex_config_uri, ns.forceTypes)
        )
        if isinstance(force_types, dict):
            vertex_config["force_types"] = force_types

        identity_from_all_properties = self._literal(
            graph, vertex_config_uri, ns.identityFromAllProperties
        )
        if identity_from_all_properties is not None:
            vertex_config["identity_from_all_properties"] = (
                identity_from_all_properties.lower() == "true"
            )
        return vertex_config

    def _parse_vertex(self, graph: Graph, vertex_uri: URIRef | BNode) -> dict[str, Any]:
        identities = []
        for identity_node in self._related_nodes(graph, vertex_uri, ns.hasIdentity):
            identity = self._literal(graph, identity_node, ns.identityName)
            if identity is not None:
                identities.append(identity)
        vertex: dict[str, Any] = {
            "name": self._literal(graph, vertex_uri, ns.name),
            "identity": identities,
            "properties": self._ordered_nodes(
                graph,
                vertex_uri,
                ns.hasField,
                self._parse_field,
            ),
        }
        description = self._literal(graph, vertex_uri, ns.description)
        if description is not None:
            vertex["description"] = description
        blank_value = self._literal(graph, vertex_uri, ns.blank)
        if blank_value is not None:
            vertex["blank"] = blank_value.lower() == "true"

        payload = parse_json_literal(self._literal(graph, vertex_uri, ns.vertexPayload))
        if isinstance(payload, dict):
            vertex.update(payload)

        return vertex

    def _parse_field(
        self, graph: Graph, field_uri: URIRef | BNode
    ) -> dict[str, Any] | str:
        name = self._literal(graph, field_uri, ns.name)
        if name is None:
            return {}
        field_type_uri = self._object(graph, field_uri, ns.fieldType)
        description = self._literal(graph, field_uri, ns.description)
        if field_type_uri is None and description is None:
            return name
        field: dict[str, Any] = {"name": name}
        if field_type_uri is not None:
            field_type = reverse_enum(ns.ENUM_REGISTRIES["field_type"], field_type_uri)
            if field_type is not None:
                field["type"] = field_type
        if description is not None:
            field["description"] = description
        return field

    def _parse_edge(self, graph: Graph, edge_uri: URIRef | BNode) -> dict[str, Any]:
        source_uri = self._object(graph, edge_uri, ns.edgeSource)
        target_uri = self._object(graph, edge_uri, ns.edgeTarget)
        edge: dict[str, Any] = {
            "source": self._literal(graph, source_uri, ns.name) if source_uri else None,
            "target": self._literal(graph, target_uri, ns.name) if target_uri else None,
        }
        relation = self._literal(graph, edge_uri, ns.relation)
        if relation is not None:
            edge["relation"] = relation
        description = self._literal(graph, edge_uri, ns.description)
        if description is not None:
            edge["description"] = description

        payload = parse_json_literal(self._literal(graph, edge_uri, ns.edgePayload))
        if isinstance(payload, dict):
            edge.update(payload)
        identities = parse_json_literal(
            self._literal(graph, edge_uri, ns.edgeIdentities)
        )
        if isinstance(identities, list):
            edge["identities"] = identities
        edge_type = self._literal(graph, edge_uri, ns.edgeType)
        if edge_type is not None:
            edge["type"] = edge_type
        edge_by = self._literal(graph, edge_uri, ns.edgeBy)
        if edge_by is not None:
            edge["by"] = edge_by

        properties = self._ordered_nodes(
            graph,
            edge_uri,
            ns.hasField,
            self._parse_field,
        )
        if properties:
            edge["properties"] = properties

        return edge

    def _parse_database_profile(
        self, graph: Graph, profile_uri: URIRef | BNode
    ) -> dict[str, Any]:
        profile: dict[str, Any] = {}
        db_flavor_uri = self._object(graph, profile_uri, ns.dbFlavor)
        if db_flavor_uri is not None:
            db_flavor = reverse_enum(ns.ENUM_REGISTRIES["db_type"], db_flavor_uri)
            if db_flavor is not None:
                profile["db_flavor"] = db_flavor
        target_namespace = self._literal(graph, profile_uri, ns.targetNamespace)
        if target_namespace is not None:
            profile["target_namespace"] = target_namespace
        self._parse_profile_indexes(graph, profile_uri, profile)

        payload = parse_json_literal(
            self._literal(graph, profile_uri, ns.profilePayload)
        )
        if isinstance(payload, dict):
            profile.update(payload)
        return profile

    def _parse_profile_indexes(
        self,
        graph: Graph,
        profile_uri: URIRef | BNode,
        profile: dict[str, Any],
    ) -> None:
        vertex_indexes: dict[str, list[dict[str, Any]]] = {}
        for index_node in self._related_nodes(graph, profile_uri, ns.hasVertexIndex):
            index_payload = self._parse_index(graph, index_node)
            vertex_name = self._literal(graph, index_node, ns.profileVertexName)
            if vertex_name is None:
                continue
            vertex_indexes.setdefault(vertex_name, []).append(index_payload)
        if vertex_indexes:
            profile["vertex_indexes"] = vertex_indexes

        edge_specs: list[dict[str, Any]] = []
        for spec_node in self._related_nodes(graph, profile_uri, ns.hasEdgeSpec):
            spec_payload: dict[str, Any] = {
                "source": self._literal(graph, spec_node, ns.specSource),
                "target": self._literal(graph, spec_node, ns.specTarget),
            }
            relation = self._literal(graph, spec_node, ns.specRelation)
            if relation is not None:
                spec_payload["relation"] = relation
            purpose = self._literal(graph, spec_node, ns.specPurpose)
            if purpose is not None:
                spec_payload["purpose"] = purpose
            relation_name = self._literal(graph, spec_node, ns.specRelationName)
            if relation_name is not None:
                spec_payload["relation_name"] = relation_name
            indexes_mode = self._literal(graph, spec_node, ns.specIndexesMode)
            if indexes_mode is not None:
                spec_payload["indexes_mode"] = indexes_mode
            indexes = [
                self._parse_index(graph, index_node)
                for index_node in self._related_nodes(graph, spec_node, ns.hasIndex)
            ]
            if indexes:
                spec_payload["indexes"] = indexes
            edge_specs.append(spec_payload)
        if edge_specs:
            profile["edge_specs"] = edge_specs

        # Backward compatibility with legacy JSON payload predicates.
        if "vertex_indexes" not in profile:
            legacy_vertex_indexes = parse_json_literal(
                self._literal(graph, profile_uri, ns.vertexIndexes)
            )
            if isinstance(legacy_vertex_indexes, dict):
                profile["vertex_indexes"] = legacy_vertex_indexes
        if "edge_specs" not in profile:
            legacy_edge_specs = parse_json_literal(
                self._literal(graph, profile_uri, ns.edgeSpecs)
            )
            if isinstance(legacy_edge_specs, list):
                profile["edge_specs"] = legacy_edge_specs

    def _parse_index(self, graph: Graph, index_node: URIRef | BNode) -> dict[str, Any]:
        index: dict[str, Any] = {
            "fields": self._literals(graph, index_node, ns.indexField)
        }
        name = self._literal(graph, index_node, ns.indexName)
        if name is not None:
            index["name"] = name
        unique = self._literal(graph, index_node, ns.indexUnique)
        if unique is not None:
            index["unique"] = unique.lower() == "true"
        index_type = self._literal(graph, index_node, ns.indexType)
        if index_type is not None:
            index["type"] = index_type
        deduplicate = self._literal(graph, index_node, ns.indexDeduplicate)
        if deduplicate is not None:
            index["deduplicate"] = deduplicate.lower() == "true"
        sparse = self._literal(graph, index_node, ns.indexSparse)
        if sparse is not None:
            index["sparse"] = sparse.lower() == "true"
        exclude_edge_endpoints = self._literal(
            graph, index_node, ns.indexExcludeEdgeEndpoints
        )
        if exclude_edge_endpoints is not None:
            index["exclude_edge_endpoints"] = exclude_edge_endpoints.lower() == "true"
        return index

    def _parse_ingestion_model(
        self, graph: Graph, ingestion_uri: URIRef | BNode
    ) -> dict[str, Any]:
        model: dict[str, Any] = {}
        duplicate_uri = self._object(graph, ingestion_uri, ns.edgesOnDuplicate)
        if duplicate_uri is not None:
            duplicate = reverse_enum(
                ns.ENUM_REGISTRIES["edge_duplicate_policy"], duplicate_uri
            )
            if duplicate is not None:
                model["edges_on_duplicate"] = duplicate

        transforms = self._ordered_nodes(
            graph,
            ingestion_uri,
            ns.hasTransform,
            self._parse_proto_transform,
        )
        if transforms:
            model["transforms"] = transforms

        resources = self._ordered_nodes(
            graph,
            ingestion_uri,
            ns.hasResource,
            self._parse_resource,
        )
        if resources:
            model["resources"] = resources
        return model

    def _parse_proto_transform(
        self, graph: Graph, transform_uri: URIRef | BNode
    ) -> dict[str, Any]:
        transform: dict[str, Any] = {
            "name": self._literal(graph, transform_uri, ns.name),
            "module": self._literal(graph, transform_uri, ns.transformModule),
            "foo": self._literal(graph, transform_uri, ns.transformFunction),
            "input": self._literals(graph, transform_uri, ns.transformInput),
            "output": self._literals(graph, transform_uri, ns.transformOutput),
        }

        params = parse_json_literal(
            self._literal(graph, transform_uri, ns.transformParams)
        )
        if isinstance(params, dict):
            if "params" in params:
                transform["params"] = params["params"]
            if params.get("input_groups") is not None:
                transform["input_groups"] = params["input_groups"]
            if params.get("output_groups") is not None:
                transform["output_groups"] = params["output_groups"]
            if (
                "params" not in params
                and "input_groups" not in params
                and "output_groups" not in params
            ):
                transform["params"] = params

        target_uri = self._object(graph, transform_uri, ns.transformTarget)
        if target_uri is not None:
            target = reverse_enum(ns.ENUM_REGISTRIES["transform_target"], target_uri)
            if target is not None:
                transform["target"] = target

        dress_uri = self._object(graph, transform_uri, ns.hasDress)
        if dress_uri is not None:
            transform["dress"] = {
                "key": self._literal(graph, dress_uri, ns.dressKey),
                "value": self._literal(graph, dress_uri, ns.dressValue),
            }

        keys_uri = self._object(graph, transform_uri, ns.hasKeySelection)
        if keys_uri is not None:
            mode_uri = self._object(graph, keys_uri, ns.keySelectionMode)
            mode = (
                reverse_enum(ns.ENUM_REGISTRIES["key_selection_mode"], mode_uri)
                if mode_uri
                else "all"
            )
            transform["keys"] = {
                "mode": mode or "all",
                "names": self._literals(graph, keys_uri, ns.keySelectionName),
            }

        return {
            key: value
            for key, value in transform.items()
            if value not in (None, [], {})
        }

    def _parse_resource(
        self, graph: Graph, resource_uri: URIRef | BNode
    ) -> dict[str, Any]:
        resource: dict[str, Any] = {"name": self._literal(graph, resource_uri, ns.name)}

        payload = parse_json_literal(
            self._literal(graph, resource_uri, ns.resourcePayload)
        )
        if isinstance(payload, dict):
            resource.update(payload)

        steps = self._parse_pipeline_steps(graph, resource_uri)
        if steps:
            resource["pipeline"] = steps

        infer_only = [
            self._parse_edge_infer_spec(graph, spec_node)
            for spec_node in self._related_nodes(
                graph, resource_uri, ns.hasEdgeInferOnly
            )
        ]
        if infer_only:
            resource["infer_edge_only"] = infer_only

        infer_except = [
            self._parse_edge_infer_spec(graph, spec_node)
            for spec_node in self._related_nodes(
                graph, resource_uri, ns.hasEdgeInferExcept
            )
        ]
        if infer_except:
            resource["infer_edge_except"] = infer_except

        return resource

    def _parse_pipeline_steps(
        self,
        graph: Graph,
        resource_uri: URIRef | BNode,
    ) -> list[dict[str, Any]]:
        step_nodes = self._related_nodes(graph, resource_uri, ns.hasActor)
        indexed_steps: list[tuple[int, dict[str, Any]]] = []
        for step_node in step_nodes:
            index_literal = self._literal(graph, step_node, ns.stepIndex)
            index = int(index_literal) if index_literal is not None else 0
            payload = self._parse_actor_step(graph, step_node)
            if payload:
                indexed_steps.append((index, payload))
        indexed_steps.sort(key=lambda item: item[0])
        return [step for _, step in indexed_steps]

    def _parse_actor_step(
        self, graph: Graph, step_node: URIRef | BNode
    ) -> dict[str, Any]:
        payload = parse_json_literal(self._literal(graph, step_node, ns.stepPayload))
        if not isinstance(payload, dict):
            return {}

        actor_type = self._literal(graph, step_node, ns.actorType)
        if actor_type == "descend":
            nested_nodes = self._related_nodes(graph, step_node, ns.hasActor)
            nested_indexed: list[tuple[int, dict[str, Any]]] = []
            for nested_node in nested_nodes:
                nested_index_literal = self._literal(graph, nested_node, ns.stepIndex)
                nested_index = (
                    int(nested_index_literal) if nested_index_literal is not None else 0
                )
                nested_payload = self._parse_actor_step(graph, nested_node)
                if nested_payload:
                    nested_indexed.append((nested_index, nested_payload))
            nested_indexed.sort(key=lambda item: item[0])
            payload["pipeline"] = [item for _, item in nested_indexed]
        return payload

    def _parse_edge_infer_spec(
        self,
        graph: Graph,
        spec_node: URIRef | BNode,
    ) -> dict[str, Any]:
        payload = parse_json_literal(self._literal(graph, spec_node, ns.stepPayload))
        if isinstance(payload, dict):
            return payload
        return {}

    def _parse_bindings(
        self, graph: Graph, bindings_uri: URIRef | BNode
    ) -> dict[str, Any]:
        bindings: dict[str, Any] = {}
        connectors = self._ordered_nodes(
            graph,
            bindings_uri,
            ns.hasConnector,
            self._parse_connector,
        )
        if connectors:
            bindings["connectors"] = connectors

        resource_connector = []
        for binding_node in self._related_nodes(
            graph, bindings_uri, ns.bindsResourceToConnector
        ):
            resource_connector.append(
                {
                    "resource": self._literal(graph, binding_node, ns.resourceName),
                    "connector": self._literal(graph, binding_node, ns.connectorName),
                }
            )
        if resource_connector:
            bindings["resource_connector"] = resource_connector

        connector_connection = []
        for binding_node in self._related_nodes(
            graph, bindings_uri, ns.bindsConnectorToConnProxy
        ):
            connector_connection.append(
                {
                    "connector": self._literal(graph, binding_node, ns.connectorName),
                    "conn_proxy": self._literal(graph, binding_node, ns.connProxy),
                }
            )
        if connector_connection:
            bindings["connector_connection"] = connector_connection

        staging_proxy = []
        for binding_node in self._related_nodes(
            graph, bindings_uri, ns.hasStagingProxy
        ):
            staging_proxy.append(
                {
                    "name": self._literal(graph, binding_node, ns.name),
                    "conn_proxy": self._literal(graph, binding_node, ns.connProxy),
                }
            )
        if staging_proxy:
            bindings["staging_proxy"] = staging_proxy

        return bindings

    def _parse_connector(
        self, graph: Graph, connector_uri: URIRef | BNode
    ) -> dict[str, Any]:
        rdf_types = {str(value) for value in graph.objects(connector_uri, RDF.type)}
        connector_model = "FileConnector"
        for rdf_type, model_name in ns.CONNECTOR_CLASS_BY_RDF_TYPE.items():
            if str(rdf_type) in rdf_types:
                connector_model = model_name
                break

        connector: dict[str, Any] = {}
        name = self._literal(graph, connector_uri, ns.name)
        if name is not None:
            connector["name"] = name
        resource_name = self._literal(graph, connector_uri, ns.resourceName)
        if resource_name is not None:
            connector["resource_name"] = resource_name

        payload = parse_json_literal(
            self._literal(graph, connector_uri, ns.connectorPayload)
        )
        if isinstance(payload, dict):
            connector.update(payload)

        connector_cls = ns.CONNECTOR_MODELS[connector_model]
        validated = connector_cls.model_validate(connector)
        return validated.model_dump(
            mode="json", by_alias=True, exclude={"hash"}, exclude_none=True
        )

    def _ordered_nodes(
        self,
        graph: Graph,
        subject: URIRef | BNode | None,
        predicate: URIRef,
        parser: Any,
    ) -> list[Any]:
        if subject is None:
            return []
        indexed: list[tuple[int, Any]] = []
        for node in self._related_nodes(graph, subject, predicate):
            index_literal = self._literal(graph, node, ns.artifactIndex)
            index = int(index_literal) if index_literal is not None else 0
            indexed.append((index, parser(graph, node)))
        indexed.sort(key=lambda item: item[0])
        return [value for _, value in indexed]

    @staticmethod
    def _related_nodes(
        graph: Graph,
        subject: URIRef | BNode,
        predicate: URIRef,
    ) -> list[URIRef | BNode]:
        return [
            obj
            for obj in graph.objects(subject, predicate)
            if isinstance(obj, (URIRef, BNode))
        ]

    @staticmethod
    def _object(
        graph: Graph,
        subject: URIRef | BNode | None,
        predicate: URIRef,
    ) -> URIRef | BNode | None:
        if subject is None:
            return None
        for obj in graph.objects(subject, predicate):
            if isinstance(obj, (URIRef, BNode)):
                return obj
        return None

    @staticmethod
    def _objects(
        graph: Graph, subject: URIRef | BNode, predicate: URIRef
    ) -> list[URIRef]:
        return [
            obj for obj in graph.objects(subject, predicate) if isinstance(obj, URIRef)
        ]

    @staticmethod
    def _literal(
        graph: Graph,
        subject: URIRef | BNode | None,
        predicate: URIRef,
    ) -> str | None:
        if subject is None:
            return None
        for obj in graph.objects(subject, predicate):
            if isinstance(obj, Literal):
                return str(obj)
        return None

    @staticmethod
    def _literals(
        graph: Graph,
        subject: URIRef | BNode | None,
        predicate: URIRef,
    ) -> list[str]:
        if subject is None:
            return []
        return [
            str(obj)
            for obj in graph.objects(subject, predicate)
            if isinstance(obj, Literal)
        ]
