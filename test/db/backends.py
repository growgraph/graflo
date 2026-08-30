"""One registry of the backends the cross-backend suites run against.

Each cross-backend suite used to carry its own ``BACKENDS`` list *and* its own
flavor-to-config ``if/elif`` chain. Five near-identical copies meant a new
backend had to be added in five places, and a fix to one backend's setup --
Postgres needing an isolated ``schema_name``, Nebula needing an explicit
``uri`` -- silently applied to whichever copies the author remembered.

So the knobs live here and a suite declares only *which* backends it covers,
keeping its own reason for any it leaves out. What differs between suites is
the namespace, which is why :func:`config_for` takes ``space`` rather than
reading a module-level constant.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from graflo.onto import DBType

#: Every flavor a cross-backend suite can address, in the order the suites
#: list them. Not every suite covers all of these -- see each suite's own
#: BACKENDS for what it excludes and why.
ALL_BACKENDS: tuple[str, ...] = (
    "neo4j",
    "arango",
    "memgraph",
    "falkordb",
    "postgres",
    "nebula",
    "tigergraph",
    "graflo_backend",
)

#: Backends whose live tests are opt-in, and the marker that gates them.
#: Nebula needs a running multi-container cluster; TigerGraph's GSQL DDL costs
#: 15-40s per graph. Both are skipped unless their ``--run-*`` flag is passed.
OPT_IN_MARKS: dict[str, Any] = {
    "nebula": pytest.mark.nebula,
    "tigergraph": pytest.mark.tigergraph,
}


def backend_params(flavors: Sequence[str]) -> list[Any]:
    """Build the ``pytest.param`` list for *flavors*, applying opt-in marks.

    Marking is centralised so a suite cannot accidentally run a gated backend
    unmarked -- which would make a default ``./run-tests.sh`` hang on a cluster
    that is not expected to be up.
    """
    unknown = [f for f in flavors if f not in ALL_BACKENDS]
    if unknown:
        raise ValueError(f"unknown backend flavor(s): {unknown!r}")
    return [
        pytest.param(flavor, id=flavor, marks=OPT_IN_MARKS.get(flavor, ()))
        for flavor in flavors
    ]


#: Which config field carries the namespace, per flavor. PostgreSQL's schema
#: and Nebula's space live on ``schema_name``; everything else uses
#: ``database``. Assigning ``database`` on the first two is *accepted and then
#: ignored* — a silent no-op that is exactly what this mapping prevents.
_NAMESPACE_FIELD: dict[DBType, str] = {
    DBType.POSTGRES: "schema_name",
    DBType.NEBULA: "schema_name",
}


def set_target_namespace(config: Any, name: str) -> None:
    """Point *config* at namespace *name*, on whatever field its flavor reads.

    Neo4j and Memgraph have no per-suite namespace on the community edition, so
    they are left alone rather than given a name that would not take effect.
    """
    flavor = config.connection_type
    if flavor in (DBType.NEO4J, DBType.MEMGRAPH):
        return
    setattr(config, _NAMESPACE_FIELD.get(flavor, "database"), name)


def config_for(
    flavor: str,
    *,
    space: str,
    tmp_path_factory: pytest.TempPathFactory | None = None,
    output_dir: Path | None = None,
) -> Any:
    """Build a live connection config for *flavor*, namespaced to *space*.

    ``space`` is the per-suite namespace: an ArangoDB/FalkorDB database, a
    TigerGraph graph, a Nebula space, a PostgreSQL schema. Isolating on it is
    not cosmetic -- several suites share one docker instance, and
    ``test/db/postgres/test_introspection.py`` asserts exact table counts, so a
    suite that ingests into ``public`` breaks it.

    Neo4j and Memgraph take no namespace: neither has a per-suite database on
    the community edition, which is ``CORE-NEO4J-NS-001`` and the reason the
    cross-backend suites are run serially.

    Args:
        flavor: One of :data:`ALL_BACKENDS`.
        space: Namespace to isolate this suite in.
        tmp_path_factory: Required for ``graflo_backend`` unless *output_dir*
            is given; ignored by every other flavor.
        output_dir: Explicit on-disk root for ``graflo_backend``.

    Returns:
        A ``DBConfig`` subclass instance for *flavor*.
    """
    from graflo.connections.onto import (
        ArangoConfig,
        FalkordbConfig,
        MemgraphConfig,
        NebulaConfig,
        Neo4jConfig,
        PostgresConfig,
        TigergraphConfig,
    )

    if flavor == "neo4j":
        config = Neo4jConfig.from_docker_env()
        config.database = config.database or "_system"
        return config
    if flavor == "arango":
        config = ArangoConfig.from_docker_env()
        config.database = space
        return config
    if flavor == "memgraph":
        return MemgraphConfig.from_docker_env()
    if flavor == "falkordb":
        config = FalkordbConfig.from_docker_env()
        config.database = space
        return config
    if flavor == "postgres":
        config = PostgresConfig.from_docker_env()
        config.database = config.database or "postgres"
        config.schema_name = space
        return config
    if flavor == "nebula":
        config = NebulaConfig.from_docker_env()
        # from_docker_env leaves the scheme off, which the driver needs.
        config.uri = f"nebula://localhost:{config.port}"
        config.schema_name = space
        return config
    if flavor == "tigergraph":
        config = TigergraphConfig.from_docker_env()
        config.database = space
        return config
    if flavor == "graflo_backend":
        from graflo.connections.graflo_backend import GraFloBackendConfig

        if output_dir is None:
            if tmp_path_factory is None:
                raise ValueError(
                    "graflo_backend needs either output_dir or tmp_path_factory"
                )
            output_dir = tmp_path_factory.mktemp(space)
        return GraFloBackendConfig(output_dir=output_dir)

    raise ValueError(f"unknown backend flavor: {flavor!r}")
