"""
Replay a GraFlo file backend into ArangoDB or PostgreSQL.

    cd examples/13-graph-export-migration
    uv run python migrate.py arango --from-backend artifacts/csv-backend --recreate-schema
    uv run python migrate.py postgres --from-backend artifacts/neo4j-backend --recreate-schema
"""

from __future__ import annotations

from pathlib import Path

import click

from graflo import DBType, GraphEngine

from _common import (
    add_migration_limits_options,
    arango_config,
    postgres_config,
    resolve_source_backend,
)


@click.group()
def cli() -> None:
    """Migrate a GraFlo file backend (or Neo4j) into a live target database."""


@cli.command("arango")
@click.option(
    "--from-backend",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Read graph from a GraFlo file backend instead of Neo4j.",
)
@click.option("--recreate-schema", is_flag=True)
@click.option("--clear-data", is_flag=True)
@add_migration_limits_options
def migrate_arango(
    from_backend: Path | None,
    recreate_schema: bool,
    clear_data: bool,
    sample_limit: int,
    data_limit: int | None,
) -> None:
    """Migrate source → ArangoDB."""
    source = resolve_source_backend(from_backend)
    arango = arango_config()
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    engine.migrate_graph(
        source,
        arango,
        recreate_schema=recreate_schema,
        clear_data=clear_data,
        sample_limit=sample_limit,
        data_limit=data_limit,
    )
    click.echo("Source → Arango migration complete")


@cli.command("postgres")
@click.option(
    "--from-backend",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Read graph from a GraFlo file backend instead of Neo4j.",
)
@click.option("--recreate-schema", is_flag=True)
@click.option("--clear-data", is_flag=True)
@add_migration_limits_options
def migrate_postgres(
    from_backend: Path | None,
    recreate_schema: bool,
    clear_data: bool,
    sample_limit: int,
    data_limit: int | None,
) -> None:
    """Migrate source → PostgreSQL."""
    source = resolve_source_backend(from_backend)
    postgres = postgres_config()
    engine = GraphEngine(target_db_flavor=DBType.POSTGRES)
    engine.migrate_graph(
        source,
        postgres,
        recreate_schema=recreate_schema,
        clear_data=clear_data,
        sample_limit=sample_limit,
        data_limit=data_limit,
    )
    click.echo("Source → PostgreSQL migration complete")


if __name__ == "__main__":
    cli()
