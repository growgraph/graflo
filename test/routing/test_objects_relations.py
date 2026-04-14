"""Integration tests for objects.csv and relations.csv with dynamic vertex/edge routing.

Uses schema from test/config/schema/objects-relations.yaml and
data from test/data/objects-relations/objects.csv and relations.csv.
Verifies vertex_router (type_map) and edge_router (type_map, relation_map) logic.
"""

from pathlib import Path

import pandas as pd
import pytest
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import GraphContainer


def _load_csv_as_dicts(csv_path: Path) -> list[dict]:
    """Load CSV as list of dicts (one per row)."""
    df = pd.read_csv(csv_path)
    return df.to_dict(orient="records")


@pytest.fixture
def schema_objects_relations():
    """Schema with vertex_router (objects) and edge_router (relations) resources."""
    manifest = GraphManifest.from_config(
        FileHandle.load("test.config.schema", "objects-relations.yaml")
    )
    manifest.finish_init()
    return manifest


@pytest.fixture
def objects_data(current_path):
    """Load objects.csv as list of dicts."""
    csv_path = Path(current_path) / "data" / "objects-relations" / "objects.csv"
    return _load_csv_as_dicts(csv_path)


@pytest.fixture
def relations_data(current_path):
    """Load relations.csv as list of dicts."""
    csv_path = Path(current_path) / "data" / "objects-relations" / "relations.csv"
    return _load_csv_as_dicts(csv_path)


class TestObjectsResource:
    """VertexRouterActor with type_map routes objects.csv rows to person/vehicle/institution."""

    def test_objects_resource_produces_vertices_by_type(
        self, schema_objects_relations, objects_data
    ):
        """Each objects row is routed to the correct vertex type via type_map."""
        ingestion_model = schema_objects_relations.require_ingestion_model()
        resource = ingestion_model.fetch_resource("objects")
        all_docs = [resource(doc) for doc in objects_data]
        graph = GraphContainer.from_docs_list(all_docs)

        assert "person" in graph.vertices
        assert "vehicle" in graph.vertices
        assert "institution" in graph.vertices

        assert len(graph.vertices["person"]) == 4
        assert len(graph.vertices["vehicle"]) == 3
        assert len(graph.vertices["institution"]) == 3

    def test_objects_vertex_has_expected_fields(
        self, schema_objects_relations, objects_data
    ):
        """Routed vertices retain expected fields from the source row."""
        ingestion_model = schema_objects_relations.require_ingestion_model()
        resource = ingestion_model.fetch_resource("objects")
        # First row is Person (Alice Martin)
        doc = objects_data[0]
        result = resource(doc)

        person_docs = result["person"]
        assert len(person_docs) >= 1
        alice_id = "ec3cd5f9-8a75-49af-adc8-654eab637ebc"
        alice = next(
            (d for d in person_docs if d.get("id") == alice_id),
            person_docs[0],
        )
        assert alice["name"] == "Alice Martin"
        assert alice["email"] == "alice@example.com"


class TestRelationsResource:
    """EdgeRouterActor routes relations.csv rows to dynamic edges with relation_map."""

    def test_relations_resource_produces_edges(
        self, schema_objects_relations, relations_data
    ):
        """Each relations row produces one edge with canonical relation names."""
        ingestion_model = schema_objects_relations.require_ingestion_model()
        resource = ingestion_model.fetch_resource("relations")
        all_docs = [resource(doc) for doc in relations_data]
        graph = GraphContainer.from_docs_list(all_docs)

        total_edges = sum(len(edocs) for edocs in graph.edges.values() if edocs)
        assert total_edges == len(relations_data), (
            f"Expected {len(relations_data)} edges (1 per row), got {total_edges}"
        )

    def test_relations_use_canonical_relation_names(
        self, schema_objects_relations, relations_data
    ):
        """relation_map translates EMPLOYED_BY -> employed_by etc."""
        ingestion_model = schema_objects_relations.require_ingestion_model()
        resource = ingestion_model.fetch_resource("relations")
        all_docs = [resource(doc) for doc in relations_data]
        graph = GraphContainer.from_docs_list(all_docs)

        # EMPLOYED_BY should become employed_by
        edge_keys = list(graph.edges.keys())
        employed_edges = [k for k in edge_keys if k[2] == "employed_by"]
        assert len(employed_edges) >= 1


class TestObjectsAndRelationsCombined:
    """Process both resources and verify merged graph."""

    def test_combined_objects_and_relations(
        self, schema_objects_relations, objects_data, relations_data
    ):
        """Processing both resources yields correct vertex and edge counts."""
        ingestion_model = schema_objects_relations.require_ingestion_model()
        objects_resource = ingestion_model.fetch_resource("objects")
        relations_resource = ingestion_model.fetch_resource("relations")

        all_docs = []
        for doc in objects_data:
            all_docs.append(objects_resource(doc))
        for doc in relations_data:
            all_docs.append(relations_resource(doc))

        graph = GraphContainer.from_docs_list(all_docs)

        # Vertices: 4 person + 3 vehicle + 3 institution from objects;
        # relations add vertex refs (id only) which extend the lists
        assert len(graph.vertices["person"]) >= 4
        assert len(graph.vertices["vehicle"]) >= 3
        assert len(graph.vertices["institution"]) >= 3

        # Edges: 7 from relations (1 per row)
        total_edges = sum(len(edocs) for edocs in graph.edges.values() if edocs)
        assert total_edges == 7


class TestRelationsResourceMixedTypeSources:
    """EdgeRouterActor supports one dynamic side and one explicit side."""

    @staticmethod
    def _build_manifest_for_resource(
        resource_name: str, edge_router_config: dict
    ) -> GraphManifest:
        manifest = GraphManifest.from_config(
            {
                "schema": {
                    "metadata": {"name": "mixed_edge_router_types"},
                    "core_schema": {
                        "vertex_config": {
                            "vertices": [
                                {
                                    "name": "person",
                                    "properties": ["id"],
                                    "identity": ["id"],
                                },
                                {
                                    "name": "vehicle",
                                    "properties": ["id"],
                                    "identity": ["id"],
                                },
                                {
                                    "name": "institution",
                                    "properties": ["id"],
                                    "identity": ["id"],
                                },
                            ]
                        },
                        "edge_config": {"edges": []},
                    },
                    "db_profile": {},
                },
                "ingestion_model": {
                    "resources": [
                        {
                            "name": resource_name,
                            "pipeline": [{"edge_router": edge_router_config}],
                        }
                    ]
                },
                "bindings": {},
            }
        )
        manifest.finish_init()
        return manifest

    def test_edge_router_with_explicit_source_and_dynamic_target(self):
        """source can be fixed while target comes from target_type_field."""
        manifest = self._build_manifest_for_resource(
            "relations_explicit_source",
            {
                "source": "person",
                "target_type_field": "target_type",
                "source_fields": {"id": "source_id"},
                "target_fields": {"id": "target_id"},
                "type_map": {"Vehicle": "vehicle", "Institution": "institution"},
                "relation_field": "relation_type",
                "relation_map": {"OWNS": "owns"},
            },
        )
        resource = manifest.require_ingestion_model().fetch_resource(
            "relations_explicit_source"
        )
        result = resource(
            {
                "source_id": "p1",
                "target_id": "v1",
                "target_type": "Vehicle",
                "relation_type": "OWNS",
            }
        )
        graph = GraphContainer.from_docs_list([result])

        assert "person" in graph.vertices
        assert "vehicle" in graph.vertices
        assert ("person", "vehicle", "owns") in graph.edges
        assert len(graph.edges[("person", "vehicle", "owns")]) == 1

    def test_edge_router_with_dynamic_source_and_explicit_target(self):
        """target can be fixed while source comes from source_type_field."""
        manifest = self._build_manifest_for_resource(
            "relations_explicit_target",
            {
                "source_type_field": "source_type",
                "target": "institution",
                "source_fields": {"id": "source_id"},
                "target_fields": {"id": "target_id"},
                "type_map": {"Person": "person", "Vehicle": "vehicle"},
                "relation_field": "relation_type",
                "relation_map": {"EMPLOYED_BY": "employed_by"},
            },
        )
        resource = manifest.require_ingestion_model().fetch_resource(
            "relations_explicit_target"
        )
        result = resource(
            {
                "source_id": "p1",
                "target_id": "i1",
                "source_type": "Person",
                "relation_type": "EMPLOYED_BY",
            }
        )
        graph = GraphContainer.from_docs_list([result])

        assert "person" in graph.vertices
        assert "institution" in graph.vertices
        assert ("person", "institution", "employed_by") in graph.edges
        assert len(graph.edges[("person", "institution", "employed_by")]) == 1
