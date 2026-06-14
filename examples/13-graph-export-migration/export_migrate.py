"""Graph export and migration demo (Neo4j / ArangoDB source).

Usage:
    uv run python examples/13-graph-export-migration/export_migrate.py export
    uv run python examples/13-graph-export-migration/export_migrate.py migrate-arango --recreate-schema
    uv run python examples/13-graph-export-migration/export_migrate.py migrate-postgres --recreate-schema
"""

from __future__ import annotations

import argparse
from pathlib import Path

from graflo import DBType, GraphEngine
from graflo.db import ArangoConfig, Neo4jConfig, PostgresConfig


def _neo4j_config() -> Neo4jConfig:
    try:
        return Neo4jConfig.from_docker_env()
    except Exception:
        return Neo4jConfig.from_env()


def _arango_config() -> ArangoConfig:
    try:
        return ArangoConfig.from_docker_env()
    except Exception:
        return ArangoConfig.from_env()


def _postgres_config() -> PostgresConfig:
    try:
        return PostgresConfig.from_docker_env()
    except Exception:
        return PostgresConfig.from_env()


def cmd_export(args: argparse.Namespace) -> None:
    neo4j = _neo4j_config()
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    output = engine.export_graph(
        neo4j,
        sample_limit=args.sample_limit,
        data_limit=args.data_limit,
    )
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_yaml(str(out_path))
    vertex_types = list(output.data.vertices.keys())
    edge_types = list(output.data.edges.keys())
    print(
        f"Wrote {out_path} ({len(vertex_types)} vertex types, {len(edge_types)} edge types)"
    )


def cmd_migrate_arango(args: argparse.Namespace) -> None:
    neo4j = _neo4j_config()
    arango = _arango_config()
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    engine.migrate_graph(
        neo4j,
        arango,
        recreate_schema=args.recreate_schema,
        clear_data=args.clear_data,
        sample_limit=args.sample_limit,
        data_limit=args.data_limit,
    )
    print("Neo4j → Arango migration complete")


def cmd_migrate_postgres(args: argparse.Namespace) -> None:
    neo4j = _neo4j_config()
    postgres = _postgres_config()
    engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
    engine.migrate_graph(
        neo4j,
        postgres,
        recreate_schema=args.recreate_schema,
        clear_data=args.clear_data,
        sample_limit=args.sample_limit,
        data_limit=args.data_limit,
    )
    print("Neo4j → PostgreSQL migration complete")


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=100,
        help="Property samples per type during schema inference",
    )
    parser.add_argument(
        "--data-limit",
        type=int,
        default=None,
        help="Optional cap on rows fetched per vertex/edge type",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Graph export and migration demo")
    sub = parser.add_subparsers(dest="command", required=True)

    export_p = sub.add_parser("export", help="Export Neo4j to GraFloOutput YAML")
    export_p.add_argument(
        "--output",
        default="artifacts/neo4j-export.yaml",
        help="Output YAML path",
    )
    _add_common_args(export_p)
    export_p.set_defaults(func=cmd_export)

    arango_p = sub.add_parser("migrate-arango", help="Migrate Neo4j → ArangoDB")
    arango_p.add_argument("--recreate-schema", action="store_true")
    arango_p.add_argument("--clear-data", action="store_true")
    _add_common_args(arango_p)
    arango_p.set_defaults(func=cmd_migrate_arango)

    pg_p = sub.add_parser("migrate-postgres", help="Migrate Neo4j → PostgreSQL")
    pg_p.add_argument("--recreate-schema", action="store_true")
    pg_p.add_argument("--clear-data", action="store_true")
    _add_common_args(pg_p)
    pg_p.set_defaults(func=cmd_migrate_postgres)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
