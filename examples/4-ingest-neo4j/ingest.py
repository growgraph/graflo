from suthing import FileHandle
from graflo import Caster, Patterns, Schema
from graflo.backend import ConfigFactory


schema = Schema.from_dict(FileHandle.load("schema.yaml"))

conn_conf = ConfigFactory.create_config(
    {
        "protocol": "bolt",
        "hostname": "localhost",
        "port": 7688,
        "username": "neo4j",
        "password": "test!passfortesting",
    }
)

patterns = Patterns.from_dict(
    {
        "patterns": {
            "package": {"regex": r"^package\.meta.*\.json(?:\.gz)?$"},
            "bugs": {"regex": r"^bugs.head.*\.json(?:\.gz)?$"},
        }
    }
)

caster = Caster(schema)

caster.ingest_files(
    path="./data",
    conn_conf=conn_conf,
    patterns=patterns,
    clean_start=True,
    # max_items=5,
)
