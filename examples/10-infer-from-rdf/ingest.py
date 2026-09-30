"""How do I turn an OWL ontology and RDF data into a property graph?

Infers a schema and one resource per class from ``data/ontology.ttl``, saves the
schema to ``generated-manifest.yaml``, connects each resource to the instances of
its class in ``data/data.ttl`` and writes the graph to ArangoDB. Run it from this
directory:

    uv run python ingest.py
"""

from pathlib import Path

from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.contract.bindings import Bindings, SparqlConnector
from graflo.connections import ArangoConfig
from graflo.hq import GraphEngine, IngestionParams

EXAMPLE_DIR = Path(__file__).resolve().parent
ONTOLOGY_FILE = EXAMPLE_DIR / "data" / "ontology.ttl"
DATA_FILE = EXAMPLE_DIR / "data" / "data.ttl"
NAMESPACE = "http://example.org/"

# 1. Infer the schema and the resources from the ontology.
conn_conf = ArangoConfig.from_docker_env()
engine = GraphEngine(target_db_flavor=conn_conf.connection_type)
schema, ingestion_model = engine.infer_schema_from_rdf(
    source=ONTOLOGY_FILE, schema_name="academic_kg"
)

# 2. Save what was inferred, to read or edit.
FileHandle.dump(
    schema.model_dump(exclude_defaults=True), EXAMPLE_DIR / "generated-manifest.yaml"
)

# 3. Read the instances of each class from the data file.
bindings = Bindings()
for resource in ingestion_model.resources:
    connector = SparqlConnector(rdf_class=NAMESPACE + resource.name, rdf_file=DATA_FILE)
    bindings.add_connector(connector)
    bindings.bind_resource(resource.name, connector)

# 4. Write the graph.
engine.define_and_ingest(
    manifest=GraphManifest(
        graph_schema=schema, ingestion_model=ingestion_model, bindings=bindings
    ),
    target_db_config=conn_conf,
    ingestion_params=IngestionParams(clear_data=True),
    recreate_schema=True,
)
