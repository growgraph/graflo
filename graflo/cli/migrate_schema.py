"""Schema migration CLI."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click
from suthing import FileHandle

from graflo.connections.onto import DBConfig
from graflo.migrate.diff import SchemaDiff
from graflo.migrate.executor import MigrationExecutor
from graflo.migrate.io import load_schema, plan_to_json_serializable, schema_hash
from graflo.migrate.planner import MigrationPlanner
from graflo.migrate.store import FileMigrationStore

logger = logging.getLogger(__name__)


def _build_plan(
    *,
    from_schema_path: str | Path,
    to_schema_path: str | Path,
    allow_high_risk: bool,
):
    schema_old = load_schema(from_schema_path)
    schema_new = load_schema(to_schema_path)
    diff = SchemaDiff(schema_old=schema_old, schema_new=schema_new)
    diff_result = diff.compare()
    plan = MigrationPlanner(allow_high_risk=allow_high_risk).build(diff_result)
    return schema_new, diff_result, plan


@click.group()
def migrate_schema() -> None:
    """Plan and execute graflo schema migrations."""


@migrate_schema.command("plan")
@click.option(
    "--from-schema-path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option(
    "--to-schema-path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--allow-high-risk", is_flag=True, default=False)
@click.option("--output-format", type=click.Choice(["text", "json"]), default="text")
@click.option(
    "--output-path",
    type=click.Path(path_type=Path),
    default=None,
    help="Optional path to write plan output.",
)
def plan_cmd(
    from_schema_path: Path,
    to_schema_path: Path,
    allow_high_risk: bool,
    output_format: str,
    output_path: Path | None,
) -> None:
    """Generate a migration plan from two schema YAML files."""
    _, diff_result, plan = _build_plan(
        from_schema_path=from_schema_path,
        to_schema_path=to_schema_path,
        allow_high_risk=allow_high_risk,
    )

    if output_format == "json":
        payload = {
            "diff": diff_result.model_dump(),
            "plan": plan_to_json_serializable(plan),
        }
        serialized = json.dumps(payload, indent=2, sort_keys=True)
    else:
        lines = []
        lines.append("Migration Plan")
        lines.append("================")
        lines.append(f"Operations: {len(plan.operations)}")
        lines.append(f"Blocked: {len(plan.blocked_operations)}")
        lines.append("")
        if plan.operations:
            lines.append("Runnable operations:")
            for op in plan.operations:
                lines.append(f"- {op.op_type} {op.target} [{op.risk.value}]")
        if plan.blocked_operations:
            lines.append("")
            lines.append("Blocked operations:")
            for op in plan.blocked_operations:
                lines.append(f"- {op.op_type} {op.target} [{op.risk.value}]")
        if plan.warnings:
            lines.append("")
            lines.append("Warnings:")
            for warning in plan.warnings:
                lines.append(f"- {warning}")
        serialized = "\n".join(lines)

    if output_path is not None:
        output_path.write_text(serialized, encoding="utf-8")
    click.echo(serialized)


@migrate_schema.command("apply")
@click.option(
    "--from-schema-path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option(
    "--to-schema-path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option(
    "--db-config-path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--revision", type=str, required=True)
@click.option("--allow-high-risk", is_flag=True, default=False)
@click.option("--dry-run/--no-dry-run", default=True)
@click.option(
    "--store-path", type=click.Path(path_type=Path), default=".graflo/migrations.json"
)
def apply_cmd(
    from_schema_path: Path,
    to_schema_path: Path,
    db_config_path: Path,
    revision: str,
    allow_high_risk: bool,
    dry_run: bool,
    store_path: Path,
) -> None:
    """Apply a migration plan against a target backend."""
    target_schema, _, plan = _build_plan(
        from_schema_path=from_schema_path,
        to_schema_path=to_schema_path,
        allow_high_risk=allow_high_risk,
    )
    conn_conf = DBConfig.from_dict(FileHandle.load(db_config_path))
    executor = MigrationExecutor(
        allow_high_risk=allow_high_risk,
        store=FileMigrationStore(store_path),
    )
    report = executor.execute_plan(
        revision=revision,
        schema_hash=schema_hash(target_schema),
        target_schema=target_schema,
        plan=plan,
        conn_conf=conn_conf,
        dry_run=dry_run,
    )
    click.echo(
        json.dumps(
            {
                "applied": report.applied,
                "skipped": report.skipped,
                "blocked": report.blocked,
                "dry_run": dry_run,
            },
            indent=2,
            sort_keys=True,
        )
    )


@migrate_schema.command("status")
@click.option(
    "--store-path", type=click.Path(path_type=Path), default=".graflo/migrations.json"
)
@click.option("--backend", type=str, default=None)
def status_cmd(store_path: Path, backend: str | None) -> None:
    """Show latest applied migration revision."""
    store = FileMigrationStore(store_path)
    latest = store.latest(backend=backend)
    payload = latest.model_dump() if latest is not None else {"latest": None}
    click.echo(json.dumps(payload, indent=2, sort_keys=True))


@migrate_schema.command("history")
@click.option(
    "--store-path", type=click.Path(path_type=Path), default=".graflo/migrations.json"
)
@click.option("--backend", type=str, default=None)
def history_cmd(store_path: Path, backend: str | None) -> None:
    """Show migration history."""
    store = FileMigrationStore(store_path)
    records = store.history()
    if backend is not None:
        records = [record for record in records if record.backend == backend]
    click.echo(
        json.dumps(
            [record.model_dump() for record in records], indent=2, sort_keys=True
        )
    )


if __name__ == "__main__":
    migrate_schema()
