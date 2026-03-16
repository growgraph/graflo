"""Example 6: Ingest data from RDF / Turtle files into a graph database.

This example demonstrates:
- Inferring a Schema from an OWL ontology (TBox)
- Creating Bindings with explicit SparqlConnector resource mapping
- Ingesting RDF instance data (ABox) into a graph database (ArangoDB/Neo4j)

The dataset models a small academic knowledge graph with Researchers,
Publications, and Institutions connected by authorOf, affiliatedWith,
and cites relationships.

Prerequisites:
- Target graph database (ArangoDB or Neo4j) running
- Environment variables or .env files configured for the target database
- graflo[sparql] extra installed:  pip install graflo[sparql]
"""

import logging
from pathlib import Path

from graflo.db import ArangoConfig
from graflo.hq import GraphEngine, IngestionParams
from graflo.architecture.bindings import Bindings, SparqlConnector
from graflo.architecture.manifest import GraphManifest
from suthing import FileHandle


logger = logging.getLogger(__name__)

# Configure logging: INFO level for graflo module, WARNING for others
logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler()])
logging.getLogger("graflo").setLevel(logging.DEBUG)

# ---------------------------------------------------------------------------
# Paths to RDF files (relative to this script)
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
ONTOLOGY_FILE = DATA_DIR / "ontology.ttl"
DATA_FILE = DATA_DIR / "data.ttl"

# ---------------------------------------------------------------------------
# Step 1: Connect to target graph database
# ---------------------------------------------------------------------------
# Load config from docker/arango/.env (recommended)
# This automatically reads ARANGO_URI, ARANGO_USERNAME, ARANGO_PASSWORD, etc.
conn_conf = ArangoConfig.from_docker_env()

# Alternative targets – uncomment the one you need:
# from graflo.db import Neo4jConfig, TigergraphConfig, FalkordbConfig
# conn_conf = Neo4jConfig.from_docker_env()
# conn_conf = TigergraphConfig.from_docker_env()
# conn_conf = FalkordbConfig.from_docker_env()

# Or specify directly:
# conn_conf = ArangoConfig(
#     uri="http://localhost:8535",
#     username="root",
#     password="123",
#     database="_system",
# )

# Determine DB type from connection config
db_type = conn_conf.connection_type

# ---------------------------------------------------------------------------
# Step 2: Infer Schema from the OWL ontology
# ---------------------------------------------------------------------------
# GraphEngine reads the TBox (owl:Class, owl:DatatypeProperty,
# owl:ObjectProperty) and builds vertices, fields, and edges automatically.
engine = GraphEngine(target_db_flavor=db_type)

schema, ingestion_model = engine.infer_schema_from_rdf(
    source=ONTOLOGY_FILE,
    schema_name="academic_kg",
)


schema_output_file = Path(__file__).parent / "generated-manifest.yaml"

FileHandle.dump(schema.model_dump(exclude_defaults=True), schema_output_file)


logger.info(
    "Inferred schema: %d vertices, %d edges",
    len(schema.graph.vertex_config.vertices),
    len(list(schema.graph.edge_config.values())),
)

# ---------------------------------------------------------------------------
# Step 3: Build Bindings with EXPLICIT resource mapping
# ---------------------------------------------------------------------------
# Instead of engine.create_bindings_from_rdf() we construct each
# SparqlConnector by hand, pointing at the local data file and specifying
# the rdf:Class URI that each resource should fetch.
bindings = Bindings()

bindings.add_sparql_connector(
    "Researcher",
    SparqlConnector(
        rdf_class="http://example.org/Researcher",
        rdf_file=DATA_FILE,
        resource_name="Researcher",
    ),
)

bindings.add_sparql_connector(
    "Publication",
    SparqlConnector(
        rdf_class="http://example.org/Publication",
        rdf_file=DATA_FILE,
        resource_name="Publication",
    ),
)

bindings.add_sparql_connector(
    "Institution",
    SparqlConnector(
        rdf_class="http://example.org/Institution",
        rdf_file=DATA_FILE,
        resource_name="Institution",
    ),
)

# Alternative: point connectors at a remote SPARQL endpoint instead of a file
# bindings.add_sparql_connector(
#     "Researcher",
#     SparqlConnector(
#         rdf_class="http://example.org/Researcher",
#         endpoint_url="http://localhost:3030/dataset/sparql",
#         resource_name="Researcher",
#     ),
# )

# ---------------------------------------------------------------------------
# Step 4: Define schema and ingest in one operation
# ---------------------------------------------------------------------------
engine.define_and_ingest(
    manifest=GraphManifest(
        graph_schema=schema,
        ingestion_model=ingestion_model,
        bindings=bindings,
    ),
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)

print("\n" + "=" * 80)
print("Ingestion complete!")
print("=" * 80)
print(f"\nSchema: {schema.metadata.name}")
print(f"Vertices: {len(schema.graph.vertex_config.vertices)}")
print(f"Edges: {len(list(schema.graph.edge_config.values()))}")
print(f"Resources: {len(ingestion_model.resources)}")
print("=" * 80)

# View the ingested data in your graph database's web interface:
# - ArangoDB: http://localhost:8535 (check ARANGO_PORT in docker/arango/.env)
# - Neo4j: http://localhost:7475 (check NEO4J_PORT in docker/neo4j/.env)
# - TigerGraph: http://localhost:14241 (check TG_WEB in docker/tigergraph/.env)
# - FalkorDB: http://localhost:3001 (check FALKORDB_BROWSER_PORT in docker/falkordb/.env)
