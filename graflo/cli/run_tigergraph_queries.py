"""Run installed TigerGraph queries with JSON parameters.

Reads a JSON spec, runs each parameter set against an installed query via REST++,
and writes structured JSON results.

Connection settings are loaded from environment variables via
:class:`~graflo.db.TigergraphConfig` (default ``TIGERGRAPH_*``; use ``--prefix`` for
qualified names such as ``USER_TIGERGRAPH_URI``).

Input formats (root object or array of objects):

Single query, multiple parameter sets::

    {
      "query": "my_installed_query",
      "params": [
        {"limit": 10},
        {"limit": 20}
      ]
    }

Multiple queries::

    {
      "runs": [
        {"query": "q1", "params": [{}]},
        {"query": "q2", "params": [{"id": "x"}]}
      ]
    }

``params`` may be omitted (runs once with no parameters), a single object (one run),
or aliased as ``payload`` / ``payloads``.

Example:
    $ export TIGERGRAPH_URI=http://localhost:14240
    $ export TIGERGRAPH_USERNAME=tigergraph
    $ export TIGERGRAPH_PASSWORD=tigergraph
    $ export TIGERGRAPH_DATABASE=my_graph
    $ uv run run_tigergraph_queries -i runs.json -o results.json
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any, cast

import click
from pydantic import BaseModel

from graflo.db import TigergraphConfig
from graflo.db.tigergraph.conn import TigerGraphConnection
from graflo.db.util import json_serializer

logger = logging.getLogger(__name__)


class QueryRunSpec(BaseModel):
    """One installed query and zero or more parameter payloads.

    ``params is None`` means the key was omitted — run once with no parameters.
    An empty list means run zero times.
    """

    query: str
    params: list[dict[str, Any]] | None = None


class QueryRunResult(BaseModel):
    """Outcome for a single parameter set."""

    index: int
    params: dict[str, Any]
    result: dict[str, Any] | list[dict[str, Any]] | str | int | float | bool | None = (
        None
    )
    error: str | None = None


class QueryBatchResult(BaseModel):
    """Aggregated results for one query name."""

    query: str
    results: list[QueryRunResult]


class RunOutput(BaseModel):
    """Full CLI output document."""

    graph: str
    runs: list[QueryBatchResult]


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


def _param_list_from_item(
    item: dict[str, Any], *, context: str
) -> list[dict[str, Any]] | None:
    if "params" in item:
        raw = item["params"]
    elif "payloads" in item:
        raw = item["payloads"]
    elif "payload" in item:
        raw = item["payload"]
    else:
        return None

    if raw is None:
        return []
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        if not all(isinstance(entry, dict) for entry in raw):
            raise ValueError(
                f"{context}: params/payload(s) must be objects or a list of objects"
            )
        return raw
    raise ValueError(
        f"{context}: params/payload(s) must be an object, a list of objects, or omitted"
    )


def _coerce_run_spec(item: object, *, context: str) -> QueryRunSpec:
    if not isinstance(item, dict):
        raise ValueError(f"{context}: expected an object")
    record = cast(dict[str, Any], item)
    query = record.get("query")
    if not query or not isinstance(query, str):
        raise ValueError(f"{context}: missing or invalid 'query' string")
    return QueryRunSpec(
        query=query, params=_param_list_from_item(record, context=context)
    )


def parse_query_runs(data: object) -> list[QueryRunSpec]:
    """Parse JSON input into one or more query run specifications."""
    if isinstance(data, list):
        return [
            _coerce_run_spec(item, context=f"runs[{i}]") for i, item in enumerate(data)
        ]

    if not isinstance(data, dict):
        raise ValueError("JSON root must be an object or array of run objects")

    root = cast(dict[str, Any], data)
    if "runs" in root:
        runs = root["runs"]
        if not isinstance(runs, list):
            raise ValueError("'runs' must be an array")
        return [
            _coerce_run_spec(item, context=f"runs[{i}]") for i, item in enumerate(runs)
        ]

    if "query" in root:
        return [_coerce_run_spec(root, context="root")]

    raise ValueError("JSON object must contain 'query' or 'runs'")


def _is_tigergraph_error(result: object) -> str | None:
    if not isinstance(result, dict):
        return None
    payload = cast(dict[str, Any], result)
    if payload.get("error") is True:
        message = payload.get(
            "message", payload.get("details", "TigerGraph query failed")
        )
        return str(message)
    return None


def run_query_batch(
    conn: TigerGraphConnection,
    *,
    graph_name: str,
    specs: list[QueryRunSpec],
) -> RunOutput:
    """Execute all specs and collect structured results."""
    batches: list[QueryBatchResult] = []

    for spec in specs:
        run_results: list[QueryRunResult] = []
        if spec.params is None:
            param_sets = [{}]
        else:
            param_sets = spec.params

        for index, params in enumerate(param_sets):
            try:
                raw = conn._run_installed_query(
                    spec.query,
                    graph_name=graph_name,
                    **params,
                )
                error = _is_tigergraph_error(raw)
                run_results.append(
                    QueryRunResult(
                        index=index,
                        params=params,
                        result=raw if error is None else None,
                        error=error,
                    )
                )
            except Exception as exc:
                logger.exception("Query %r param set %d failed", spec.query, index)
                run_results.append(
                    QueryRunResult(
                        index=index,
                        params=params,
                        error=str(exc),
                    )
                )

        batches.append(QueryBatchResult(query=spec.query, results=run_results))

    return RunOutput(graph=graph_name, runs=batches)


def _load_input_json(input_path: Path | None, inline_json: str | None) -> object:
    if inline_json is not None:
        return json.loads(inline_json)
    if input_path is None:
        raise click.ClickException("Provide --input/-i or --json")

    text = (
        input_path.read_text(encoding="utf-8")
        if input_path != Path("-")
        else sys.stdin.read()
    )
    return json.loads(text)


@click.command()
@click.option(
    "--input",
    "-i",
    "input_path",
    type=click.Path(path_type=Path),
    default=None,
    help="JSON spec file (use '-' for stdin).",
)
@click.option(
    "--json",
    "inline_json",
    default=None,
    help="Inline JSON spec (alternative to --input).",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write results JSON to this file (default: stdout).",
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
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable debug logging.",
)
def run_tigergraph_queries(
    input_path: Path | None,
    inline_json: str | None,
    output_path: Path | None,
    graph: str | None,
    prefix: str | None,
    verbose: bool,
) -> None:
    """Run installed TigerGraph queries from a JSON parameter spec."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if input_path is not None and inline_json is not None:
        raise click.ClickException("Use only one of --input and --json")

    try:
        data = _load_input_json(input_path, inline_json)
        specs = parse_query_runs(data)
    except (json.JSONDecodeError, ValueError) as exc:
        raise click.ClickException(f"Invalid input JSON: {exc}") from exc

    if not specs:
        raise click.ClickException("No queries to run")

    config = TigergraphConfig.from_env(prefix=prefix)
    graph_name = _resolve_graph_name(config, graph)
    conn = TigerGraphConnection(config)

    click.echo(
        f"Target: {config.uri} graph={graph_name!r} "
        f"queries={[s.query for s in specs]!r}",
        err=True,
    )

    output = run_query_batch(conn, graph_name=graph_name, specs=specs)
    payload = output.model_dump()
    text = json.dumps(payload, indent=2, default=json_serializer)

    if output_path:
        output_path.write_text(text + "\n", encoding="utf-8")
        click.echo(f"Wrote results to {output_path}", err=True)
    else:
        click.echo(text)

    if any(r.error for batch in output.runs for r in batch.results):
        sys.exit(1)


if __name__ == "__main__":
    run_tigergraph_queries()
