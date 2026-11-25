"""Drivers are optional extras: nothing imports one until a connection needs it.

Each check runs in a subprocess, since in-process ``sys.modules`` holds whatever
earlier tests loaded.
"""

from __future__ import annotations

import subprocess
import sys

DRIVERS = (
    "SPARQLWrapper",
    "arango",
    "boto3",
    "botocore",
    "confluent_kafka",
    "falkordb",
    "grafeo",
    "mgclient",
    "nebula3",
    "nebulagraph_python",
    "neo4j",
    "psycopg2",
    "redis",
    "sqlalchemy",
)


def _run(code: str) -> str:
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    return out.stdout.strip()


def test_importing_every_backend_and_source_loads_no_driver() -> None:
    code = (
        "import importlib, pkgutil, sys\n"
        "import graflo, graflo.db, graflo.data_source, graflo.hq, graflo.cli.main\n"
        "import graflo.db.manager, graflo.hq.discovery, graflo.object_storage.bucket\n"
        "for pkg in (graflo.db, graflo.data_source):\n"
        "    for m in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + '.'):\n"
        "        importlib.import_module(m.name)\n"
        "from graflo.db.conn import ConnectionCapability\n"
        "from graflo.db.manager import ConnectionManager\n"
        "for c in ConnectionCapability:\n"
        "    ConnectionManager.flavors_supporting(c)\n"
        f"print(sorted(d for d in {DRIVERS!r} if d in sys.modules))\n"
    )
    assert _run(code) == "[]"


def test_a_connection_without_its_extra_names_the_extra() -> None:
    code = (
        "import sys\n"
        "sys.modules['neo4j'] = None\n"
        "from graflo.connections.onto import Neo4jConfig\n"
        "from graflo.db.neo4j.conn import Neo4jConnection\n"
        "from graflo.util.extras import MissingExtraError\n"
        "try:\n"
        "    Neo4jConnection(Neo4jConfig(uri='bolt://localhost:7687'))\n"
        "except MissingExtraError as e:\n"
        "    print(e.extra, 'graflo[neo4j]' in str(e))\n"
    )
    assert _run(code) == "neo4j True"
