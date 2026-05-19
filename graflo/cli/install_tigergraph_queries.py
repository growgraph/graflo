"""Install GSQL queries from a directory into TigerGraph.

Reads ``.gsql`` files, uploads each query definition to the target graph, then runs
``INSTALL QUERY`` so queries are compiled and ready to run.

Connection settings are loaded from environment variables via
:class:`~graflo.db.TigergraphConfig` (default ``TIGERGRAPH_*``; use ``--prefix`` for
qualified names such as ``USER_TIGERGRAPH_URI``).

Example:
    $ export TIGERGRAPH_URI=http://localhost:14240
    $ export TIGERGRAPH_USERNAME=tigergraph
    $ export TIGERGRAPH_PASSWORD=tigergraph
    $ export TIGERGRAPH_DATABASE=my_graph
    $ uv run install_tigergraph_queries --queries-dir ./queries/
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import click

from graflo.db import TigergraphConfig
from graflo.db.tigergraph.conn import TigerGraphConnection

logger = logging.getLogger(__name__)

_CREATE_QUERY_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?QUERY\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


def query_name_from_gsql(content: str, *, fallback: str | None = None) -> str | None:
    """Return the query name from a GSQL ``CREATE [OR REPLACE] QUERY`` statement."""
    match = _CREATE_QUERY_RE.search(content)
    if match:
        return match.group(1)
    return fallback


def _gsql_response_indicates_error(response: str) -> bool:
    """Heuristic: TigerGraph often includes ``error`` in failure messages."""
    lower = response.lower()
    return "error" in lower


def _resolve_graph_name(config: TigergraphConfig, graph: str | None) -> str:
    if graph:
        return graph
    name = config.database or config.schema_name
    if not name:
        raise click.ClickException(
            "Graph name is required: pass --graph or set TIGERGRAPH_DATABASE "
            "(or TIGERGRAPH_SCHEMA_NAME)."
        )
    return name


def install_queries_from_directory(
    conn: TigerGraphConnection,
    *,
    graph_name: str,
    queries_dir: Path,
    pattern: str = "*.gsql",
) -> None:
    """Upload and install every GSQL file matching *pattern* under *queries_dir*."""
    paths = sorted(queries_dir.glob(pattern))
    if not paths:
        raise click.ClickException(f"No files matching {pattern!r} in {queries_dir}")

    for path in paths:
        content = path.read_text(encoding="utf-8")
        query_name = query_name_from_gsql(content, fallback=path.stem)
        if not query_name:
            raise click.ClickException(
                f"Could not determine query name for {path} "
                "(expected CREATE QUERY name(...) in file)"
            )

        logger.info("Uploading query %r from %s", query_name, path.name)
        upload_cmd = f"USE GRAPH {graph_name}\n{content}"
        upload_res = conn._execute_gsql(upload_cmd)
        click.echo(f"\n--- Upload: {path.name} ({query_name}) ---")
        click.echo(upload_res)

        if _gsql_response_indicates_error(upload_res):
            raise click.ClickException(
                f"Upload failed for {path.name} ({query_name}); see output above."
            )

        logger.info("Installing query %r (may take several minutes)", query_name)
        install_cmd = f"USE GRAPH {graph_name}\nINSTALL QUERY {query_name}"
        install_res = conn._execute_gsql(install_cmd)
        click.echo(f"\n--- Install: {query_name} ---")
        click.echo(install_res)

        if _gsql_response_indicates_error(install_res):
            raise click.ClickException(
                f"Install failed for {query_name}; see output above."
            )


@click.command()
@click.option(
    "--queries-dir",
    "-d",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Directory containing .gsql query definition files.",
)
@click.option(
    "--graph",
    "-g",
    default=None,
    help="Target graph name (overrides TIGERGRAPH_DATABASE / TIGERGRAPH_SCHEMA_NAME).",
)
@click.option(
    "--prefix",
    default=None,
    help=(
        "Env prefix for TigerGraph config, e.g. USER reads USER_TIGERGRAPH_URI, "
        "USER_TIGERGRAPH_USERNAME, ..."
    ),
)
@click.option(
    "--pattern",
    default="*.gsql",
    show_default=True,
    help="Glob pattern for query files inside --queries-dir.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable debug logging.",
)
def install_tigergraph_queries(
    queries_dir: Path,
    graph: str | None,
    prefix: str | None,
    pattern: str,
    verbose: bool,
) -> None:
    """Upload and install GSQL queries from a directory into TigerGraph."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    config = TigergraphConfig.from_env(prefix=prefix)
    graph_name = _resolve_graph_name(config, graph)
    conn = TigerGraphConnection(config)

    click.echo(f"Target: {config.uri} graph={graph_name!r} queries_dir={queries_dir}")

    try:
        install_queries_from_directory(
            conn,
            graph_name=graph_name,
            queries_dir=queries_dir,
            pattern=pattern,
        )
    except click.ClickException:
        sys.exit(1)

    click.echo("\nAll queries installed successfully.")
