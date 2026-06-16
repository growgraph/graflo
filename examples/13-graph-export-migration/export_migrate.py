"""Graph export, migration, and file-backend ingest demo.

Usage:
    uv run python examples/13-graph-export-migration/export_migrate.py export-backend
    uv run python examples/13-graph-export-migration/export_migrate.py ingest-backend
    uv run python examples/13-graph-export-migration/export_migrate.py migrate-arango --from-backend artifacts/neo4j-backend
    uv run python examples/13-graph-export-migration/export_migrate.py migrate-postgres --from-backend artifacts/neo4j-backend
"""

from __future__ import annotations

import argparse
from pathlib import Path

from suthing import FileHandle

from graflo import DBType, GraphManifest, GraphEngine
from graflo.db import ArangoConfig, Neo4jConfig, PostgresConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.hq.caster import IngestionParams

EXAMPLE_DIR = Path(__file__).resolve().parent


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


def _backend_config(output_dir: str | Path) -> GraFloBackendConfig:
    return GraFloBackendConfig(output_dir=Path(output_dir))


def _source_config(args: argparse.Namespace) -> Neo4jConfig | GraFloBackendConfig:
    if args.from_backend:
        return _backend_config(args.from_backend)
    return _neo4j_config()


def cmd_export_backend(args: argparse.Namespace) -> None:
    neo4j = _neo4j_config()
    backend = _backend_config(args.output_dir)
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    engine.migrate_graph(
        neo4j,
        backend,
        recreate_schema=True,
        clear_data=args.clear_data,
        sample_limit=args.sample_limit,
        data_limit=args.data_limit,
    )
    print(f"Neo4j → GraFlo file backend: {backend.output_dir}")


def cmd_ingest_backend(args: argparse.Namespace) -> None:
    import os

    manifest = GraphManifest.from_config(FileHandle.load(EXAMPLE_DIR / "manifest.yaml"))
    manifest.finish_init()
    backend = _backend_config(args.output_dir)
    engine = GraphEngine(target_db_flavor=DBType.GRAFLO_BACKEND)
    previous_cwd = os.getcwd()
    try:
        os.chdir(EXAMPLE_DIR)
        engine.define_and_ingest(
            manifest=manifest,
            target_db_config=backend,
            ingestion_params=IngestionParams(clear_data=True),
            recreate_schema=True,
        )
    finally:
        os.chdir(previous_cwd)
    print(f"CSV resources → GraFlo file backend: {backend.output_dir}")


def cmd_migrate_arango(args: argparse.Namespace) -> None:
    source = _source_config(args)
    arango = _arango_config()
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    engine.migrate_graph(
        source,
        arango,
        recreate_schema=args.recreate_schema,
        clear_data=args.clear_data,
        sample_limit=args.sample_limit,
        data_limit=args.data_limit,
    )
    print("Source → Arango migration complete")


def cmd_migrate_postgres(args: argparse.Namespace) -> None:
    source = _source_config(args)
    postgres = _postgres_config()
    engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
    engine.migrate_graph(
        source,
        postgres,
        recreate_schema=args.recreate_schema,
        clear_data=args.clear_data,
        sample_limit=args.sample_limit,
        data_limit=args.data_limit,
    )
    print("Source → PostgreSQL migration complete")


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


def _add_source_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--from-backend",
        default=None,
        help="Read graph from a GraFlo file backend directory instead of Neo4j",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Graph export, migration, and file-backend ingest demo"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    export_p = sub.add_parser(
        "export-backend",
        help="Migrate Neo4j into a chunked GraFlo file backend directory",
    )
    export_p.add_argument(
        "--output-dir",
        default="artifacts/neo4j-backend",
        help="GraFlo file backend root directory",
    )
    export_p.add_argument("--clear-data", action="store_true")
    _add_common_args(export_p)
    export_p.set_defaults(func=cmd_export_backend)

    ingest_p = sub.add_parser(
        "ingest-backend",
        help="Ingest manifest CSV resources into a GraFlo file backend directory",
    )
    ingest_p.add_argument(
        "--output-dir",
        default="artifacts/csv-backend",
        help="GraFlo file backend root directory",
    )
    ingest_p.set_defaults(func=cmd_ingest_backend)

    arango_p = sub.add_parser("migrate-arango", help="Migrate source → ArangoDB")
    arango_p.add_argument("--recreate-schema", action="store_true")
    arango_p.add_argument("--clear-data", action="store_true")
    _add_source_args(arango_p)
    _add_common_args(arango_p)
    arango_p.set_defaults(func=cmd_migrate_arango)

    pg_p = sub.add_parser("migrate-postgres", help="Migrate source → PostgreSQL")
    pg_p.add_argument("--recreate-schema", action="store_true")
    pg_p.add_argument("--clear-data", action="store_true")
    _add_source_args(pg_p)
    _add_common_args(pg_p)
    pg_p.set_defaults(func=cmd_migrate_postgres)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
