"""Load a graph from files into ArangoDB.

Reads a file backend directory (``artifacts/csv-backend`` unless you name
another) and writes its schema and records to the ArangoDB container started
from ``docker/arango``. Run it from this directory:

    uv run python migrate.py
    uv run python migrate.py artifacts/neo4j-backend
"""

from pathlib import Path

import click

from graflo.connections import ArangoConfig, GraFloBackendConfig
from graflo.hq import GraphEngine


@click.command()
@click.argument(
    "backend_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default="artifacts/csv-backend",
)
def main(backend_dir: Path) -> None:
    """Replace the graph in ArangoDB with the one stored in BACKEND_DIR."""
    source = GraFloBackendConfig(output_dir=backend_dir)
    target = ArangoConfig.from_docker_env()

    engine = GraphEngine(target_db_flavor=target.connection_type)
    engine.migrate_graph(source, target)
    click.echo(f"Loaded {backend_dir} into ArangoDB")


if __name__ == "__main__":
    main()
