"""Cards for individual graflo primitives: vertex, edge, resource, transform, connector, db profile, manifest."""

from __future__ import annotations

import pytest

from graflo.architecture.contract.bindings.connectors import (
    APIConnector,
    FileConnector,
    KafkaConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.contract.ingestion.resource import ResourceConfig
from graflo.architecture.contract.ingestion.transform import Transform
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.context.card import (
    BaseCard,
    ConnectorCard,
    DatabaseProfileCard,
    EdgeCard,
    ManifestCard,
    ResourceCard,
    SchemaCard,
    TransformCard,
    VertexCard,
    build_card,
    build_connector_card,
    build_database_profile_card,
    build_edge_card,
    build_manifest_card,
    build_resource_card,
    build_transform_card,
    build_vertex_card,
)
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def vertex_person():
    return Vertex(
        name="person",
        properties=[
            Field(name="email", type="string"),
            Field(name="name", type="string"),
            Field(name="age", type="int"),
        ],
        identity=["email"],
        description="A human being.",
    )


@pytest.fixture()
def edge_works_at():
    return Edge(
        source="person",
        target="company",
        relation="works_at",
        properties=[Field(name="since", type="datetime")],
        identities=[("since",)],
        description="Employment relation.",
    )


@pytest.fixture()
def resource_simple():
    return ResourceConfig(
        name="people",
        pipeline=[{"vertex": "person"}],
    )


@pytest.fixture()
def transform_rename():
    return Transform(rename={"old_name": "new_name"})


@pytest.fixture()
def transform_functional():
    return Transform(
        name="date_parser",
        module="graflo.util.transform",
        foo="parse_date_ibes",
        input=("raw_date",),
        output=("parsed_date",),
        strategy="single",
    )


@pytest.fixture()
def connector_file():
    return FileConnector(
        name="csv_loader",
        resource_name="people",
        regex=r".*\.csv",
    )


@pytest.fixture()
def connector_table():
    return TableConnector(
        name="pg_users",
        resource_name="users",
        table_name="public.users",
    )


@pytest.fixture()
def connector_sparql():
    return SparqlConnector(
        name="dbpedia",
        resource_name="entities",
        rdf_class="dbo:Person",
    )


@pytest.fixture()
def connector_api():
    return APIConnector(
        name="rest_orders",
        resource_name="orders",
        path="/api/orders",
    )


@pytest.fixture()
def connector_kafka():
    return KafkaConnector(
        name="events",
        resource_name="events",
        topics=["user-events", "order-events"],
        group_id="test-group",
    )


@pytest.fixture()
def db_profile():
    from graflo.architecture.graph_types import Index

    return DatabaseProfile(
        db_flavor="arango",
        vertex_indexes={"person": [Index(fields=["name"]), Index(fields=["age"])]},
        target_namespace="mydb",
    )


@pytest.fixture()
def vertex_company():
    return Vertex(
        name="company",
        properties=[Field(name="tax_id", type="string")],
        identity=["tax_id"],
    )


@pytest.fixture()
def manifest_full(
    vertex_person, vertex_company, edge_works_at, resource_simple, db_profile
):
    schema = Schema(
        metadata=GraphMetadata(name="test-manifest", version="2.0.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=[vertex_person, vertex_company]),
            edge_config=EdgeConfig(edges=[edge_works_at]),
        ),
        db_profile=db_profile,
    )
    from graflo.architecture.contract.ingestion.model import IngestionModel

    ingestion = IngestionModel(resources=[resource_simple])
    return GraphManifest(graph_schema=schema, ingestion_model=ingestion)


@pytest.fixture()
def manifest_schema_only(vertex_person, vertex_company, edge_works_at):
    schema = Schema(
        metadata=GraphMetadata(name="bare", version="0.1.0"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=[vertex_person, vertex_company]),
            edge_config=EdgeConfig(edges=[edge_works_at]),
        ),
    )
    return GraphManifest(graph_schema=schema)


# ---------------------------------------------------------------------------
# BaseCard
# ---------------------------------------------------------------------------


class TestBaseCard:
    def test_inheritance(self):
        assert issubclass(SchemaCard, BaseCard)
        assert issubclass(VertexCard, BaseCard)
        assert issubclass(EdgeCard, BaseCard)
        assert issubclass(ResourceCard, BaseCard)
        assert issubclass(TransformCard, BaseCard)
        assert issubclass(ConnectorCard, BaseCard)
        assert issubclass(DatabaseProfileCard, BaseCard)
        assert issubclass(ManifestCard, BaseCard)

    def test_id_defaults_to_none(self):
        card = VertexCard(name="x", identity_mode="natural", property_count=0)
        assert card.id is None

    def test_id_round_trips(self):
        card = VertexCard(
            id="550e8400-e29b-41d4-a716-446655440000",
            name="x",
            identity_mode="natural",
            property_count=0,
        )
        assert card.id == "550e8400-e29b-41d4-a716-446655440000"


# ---------------------------------------------------------------------------
# VertexCard
# ---------------------------------------------------------------------------


class TestVertexCard:
    def test_build(self, vertex_person):
        card = build_vertex_card(vertex_person)
        assert card.name == "person"
        assert card.identity_mode == "natural"
        assert card.property_count == 3
        assert card.identity_fields == ["email"]
        assert card.description == "A human being."
        assert card.estimated_tokens > 0

    def test_with_id(self, vertex_person):
        card = build_vertex_card(vertex_person, id="my-uuid")
        assert card.id == "my-uuid"

    def test_blank_vertex(self):
        v = Vertex(name="doc", properties=[Field(name="body")], blank=True)
        card = build_vertex_card(v)
        assert card.identity_mode == "blank"
        assert card.identity_fields == []


# ---------------------------------------------------------------------------
# EdgeCard
# ---------------------------------------------------------------------------


class TestEdgeCard:
    def test_build(self, edge_works_at):
        card = build_edge_card(edge_works_at)
        assert card.source == "person"
        assert card.target == "company"
        assert card.relation == "works_at"
        assert card.directed is True
        assert card.identity_count == 1
        assert card.property_count == 1
        assert card.description == "Employment relation."
        assert card.estimated_tokens > 0

    def test_undirected_edge(self):
        e = Edge(source="a", target="b", directed=False)
        card = build_edge_card(e)
        assert card.directed is False

    def test_relationless_edge(self):
        e = Edge(source="a", target="b")
        card = build_edge_card(e)
        assert card.relation is None


# ---------------------------------------------------------------------------
# ResourceCard
# ---------------------------------------------------------------------------


class TestResourceCard:
    def test_build(self, resource_simple):
        card = build_resource_card(resource_simple)
        assert card.name == "people"
        assert card.actor_count >= 1
        assert "person" in card.vertex_targets
        assert card.encoding == "utf-8"
        assert card.infer_edges is True
        assert card.estimated_tokens > 0

    def test_with_id(self, resource_simple):
        card = build_resource_card(resource_simple, id="res-uuid")
        assert card.id == "res-uuid"


# ---------------------------------------------------------------------------
# TransformCard
# ---------------------------------------------------------------------------


class TestTransformCard:
    def test_rename_transform(self, transform_rename):
        card = build_transform_card(transform_rename)
        assert card.functional is False
        assert card.module is None
        assert card.foo is None
        assert card.estimated_tokens > 0

    def test_functional_transform(self, transform_functional):
        card = build_transform_card(transform_functional)
        assert card.name == "date_parser"
        assert card.functional is True
        assert card.module == "graflo.util.transform"
        assert card.foo == "parse_date_ibes"
        assert card.input_count == 1
        assert card.output_count == 1
        assert card.strategy == "single"


# ---------------------------------------------------------------------------
# ConnectorCard
# ---------------------------------------------------------------------------


class TestConnectorCard:
    def test_file_connector(self, connector_file):
        card = build_connector_card(connector_file)
        assert card.type == "FileConnector"
        assert card.name == "csv_loader"
        assert card.resource_name == "people"
        assert card.summary == r".*\.csv"
        assert card.estimated_tokens > 0

    def test_table_connector(self, connector_table):
        card = build_connector_card(connector_table)
        assert card.type == "TableConnector"
        assert card.summary == "public.users"

    def test_sparql_connector(self, connector_sparql):
        card = build_connector_card(connector_sparql)
        assert card.type == "SparqlConnector"
        assert card.summary == "dbo:Person"

    def test_api_connector(self, connector_api):
        card = build_connector_card(connector_api)
        assert card.type == "APIConnector"
        assert card.summary == "/api/orders"

    def test_kafka_connector(self, connector_kafka):
        card = build_connector_card(connector_kafka)
        assert card.type == "KafkaConnector"
        assert card.summary == "user-events,order-events"

    def test_with_id(self, connector_file):
        card = build_connector_card(connector_file, id="conn-uuid")
        assert card.id == "conn-uuid"


# ---------------------------------------------------------------------------
# DatabaseProfileCard
# ---------------------------------------------------------------------------


class TestDatabaseProfileCard:
    def test_build(self, db_profile):
        card = build_database_profile_card(db_profile)
        assert card.db_flavor == "arango"
        assert card.vertex_index_count == 2
        assert card.target_namespace == "mydb"
        assert card.estimated_tokens > 0

    def test_empty_profile(self):
        profile = DatabaseProfile()
        card = build_database_profile_card(profile)
        assert card.vertex_index_count == 0
        assert card.edge_spec_count == 0
        assert card.target_namespace is None


# ---------------------------------------------------------------------------
# ManifestCard
# ---------------------------------------------------------------------------


class TestManifestCard:
    def test_full_manifest(self, manifest_full):
        card = build_manifest_card(manifest_full)
        assert card.name == "test-manifest"
        assert card.version == "2.0.0"
        assert card.has_schema is True
        assert card.has_ingestion is True
        assert card.has_bindings is False
        assert card.vertex_count == 2
        assert card.edge_count == 1
        assert card.resource_count == 1
        assert card.estimated_tokens > 0

    def test_schema_only(self, manifest_schema_only):
        card = build_manifest_card(manifest_schema_only)
        assert card.has_schema is True
        assert card.has_ingestion is False
        assert card.has_bindings is False
        assert card.vertex_count == 2
        assert card.resource_count is None

    def test_with_id(self, manifest_full):
        card = build_manifest_card(manifest_full, id="manifest-uuid")
        assert card.id == "manifest-uuid"


# ---------------------------------------------------------------------------
# SchemaCard — id field (new)
# ---------------------------------------------------------------------------


class TestSchemaCardId:
    def test_build_card_accepts_id(self, context_schema):
        card = build_card(context_schema, id="schema-uuid")
        assert card.id == "schema-uuid"
        assert card.name == "context-fixture"

    def test_build_card_id_defaults_to_none(self, context_schema):
        card = build_card(context_schema)
        assert card.id is None


# ---------------------------------------------------------------------------
# Cross-cutting: every card serializes cleanly
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_vertex_card_round_trips(self, vertex_person):
        card = build_vertex_card(vertex_person)
        d = card.to_minimal_canonical_dict()
        assert isinstance(d, dict)
        assert d["name"] == "person"

    def test_manifest_card_round_trips(self, manifest_full):
        card = build_manifest_card(manifest_full)
        d = card.to_minimal_canonical_dict()
        assert "has_schema" in d

    def test_all_cards_have_positive_tokens(
        self,
        vertex_person,
        edge_works_at,
        resource_simple,
        transform_rename,
        connector_file,
        db_profile,
        manifest_full,
    ):
        cards = [
            build_vertex_card(vertex_person),
            build_edge_card(edge_works_at),
            build_resource_card(resource_simple),
            build_transform_card(transform_rename),
            build_connector_card(connector_file),
            build_database_profile_card(db_profile),
            build_manifest_card(manifest_full),
        ]
        for card in cards:
            assert card.estimated_tokens > 0, (
                f"{card.__class__.__name__} has zero tokens"
            )
