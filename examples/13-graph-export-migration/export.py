"""
Export a live graph database (Neo4j by default) into a GraFlo file backend.

Requires Neo4j (or set connection env vars / ``docker/neo4j/.env``):

    cd examples/13-graph-export-migration
    uv run python export.py --output-dir artifacts/neo4j-backend
"""

from __future__ import annotations

from pathlib import Path

import click

from graflo import DBType, GraphEngine

from _common import (
    DEFAULT_NEO4J_BACKEND_DIR,
    add_migration_limits_options,
    backend_config,
    neo4j_config,
)


@click.command()
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_NEO4J_BACKEND_DIR,
    show_default=True,
    help="GraFlo file backend root directory.",
)
@click.option(
    "--clear-data", is_flag=True, help="Clear target backend data before export."
)
@add_migration_limits_options
def main(
    output_dir: Path,
    clear_data: bool,
    sample_limit: int,
    data_limit: int | None,
) -> None:
    """Migrate Neo4j into a chunked GraFlo file backend directory."""
    neo4j = neo4j_config()
    backend = backend_config(output_dir)
    engine = GraphEngine(target_db_flavor=DBType.ARANGO)
    engine.migrate_graph(
        neo4j,
        backend,
        recreate_schema=True,
        clear_data=clear_data,
        sample_limit=sample_limit,
        data_limit=data_limit,
    )
    click.echo(f"Neo4j → GraFlo file backend: {backend.output_dir}")


if __name__ == "__main__":
    main()
