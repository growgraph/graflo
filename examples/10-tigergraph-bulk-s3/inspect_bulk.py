"""
Check what bulk staging produced and what TigerGraph actually loaded.

Bulk ingest can report success and leave the graph empty — the LOADING JOB reads
the staged files itself, so an S3 URL the TigerGraph process cannot resolve is
not an error GraFlo ever sees. Run this after ingest.py:

    uv run python inspect_bulk.py            # staged files + loaded graph
    uv run python inspect_bulk.py --staged-only    # no database connection

Expected for this example: 3 companies (Acme, Beta, Gamma — the duplicate Acme
row upserts) and 2 relations, staged as company.csv and edge_relates.csv.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import click
from _common import (
    example_workdir,
    latest_session_dir,
    physical_names,
    tigergraph_config,
)

from graflo.architecture.graph_types import EdgeDirection
from graflo.db import ConnectionManager

EXPECTED_VERTICES = 3
EXPECTED_EDGES = 2


def _row_count(path: Path) -> int:
    """Data rows, excluding the header the staging writer emits."""
    with path.open(encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in csv.reader(handle)) - 1, 0)


def _report_staged(vertex_type: str, edge_type: str) -> bool:
    session = latest_session_dir()
    if session is None:
        click.echo("staged: nothing — bulk_staging/ has no session directory.")
        click.echo("        Run ingest.py first.")
        return False

    click.echo(f"staged: {session.name}/")
    staged = sorted(session.glob("*.csv"))
    for path in staged:
        click.echo(f"        {path.name:<24} {_row_count(path)} rows")
    if not staged:
        click.echo("        (empty)")

    names = {path.name for path in staged}
    vertex_file = f"{vertex_type}.csv"
    edge_file = f"edge_{edge_type}.csv"
    complete = True
    if vertex_file not in names:
        click.echo(f"        MISSING {vertex_file} — no vertices were staged.")
        complete = False
    if edge_file not in names:
        click.echo(
            f"        MISSING {edge_file} — vertices staged but edges did not. "
            "The relation is extracted per row, so an empty edge batch means the "
            "pipeline's edge step never fired."
        )
        complete = False
    return complete


def _report_graph(vertex_type: str, edge_type: str) -> bool:
    conn_conf = tigergraph_config()
    with ConnectionManager(connection_config=conn_conf) as db_client:
        vertices = db_client.fetch_docs(vertex_type)
        edges = db_client.fetch_edges(
            from_type=vertex_type,
            from_id="Acme",
            edge_type=edge_type,
            direction=EdgeDirection.ANY,
        )

    names = sorted(str(doc.get("name", "?")) for doc in vertices)
    click.echo(f"loaded: {len(vertices)} {vertex_type} ({', '.join(names) or '-'})")
    click.echo(f"        {len(edges)} {edge_type} incident to Acme")

    if not vertices:
        click.echo(
            "\nThe graph is empty. The LOADING JOB ran but read nothing — usually "
            "an s3:// URL that TigerGraph itself cannot reach. Set "
            "MINIO_LOADER_ENDPOINT to an address valid inside the TigerGraph "
            "container, or re-run with BULK_USE_S3=0 so the job reads local paths."
        )
        return False
    return len(vertices) == EXPECTED_VERTICES


@click.command()
@click.option(
    "--staged-only",
    is_flag=True,
    help="Only inspect the staged CSV files; do not connect to TigerGraph.",
)
def main(staged_only: bool) -> None:
    """Report staged CSVs and the loaded graph, and fail loudly on either gap."""
    with example_workdir():
        vertex_type, edge_type = physical_names()
        staged_ok = _report_staged(vertex_type, edge_type)
        if staged_only:
            sys.exit(0 if staged_ok else 1)
        click.echo("")
        graph_ok = _report_graph(vertex_type, edge_type)
    sys.exit(0 if staged_ok and graph_ok else 1)


if __name__ == "__main__":
    main()
