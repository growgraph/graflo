"""``graflo canonical-check`` -- a canonical map against the manifest it maps.

A canonical map is authored against one schema long before it is composed
against another, and until now the only thing that would check it was a full
``graflo compose``: two manifests, an op, and a refusal naming one entry at a
time. A map of hundreds of entries is not authored that way.

This verb is that check on its own. With one manifest it classifies every entry
directly; with both it runs the compose *preview*, which puts each declaration
through the rules compose uses without composing, so one bad entry does not
hide the rest. ``--trim`` writes the map narrowed to what the manifest actually
declares -- an authoring step with a diff, rather than something compose does
silently at the far end of a pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path

import click
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.canonical import (
    CanonicalMap,
    DanglingEntry,
    Scope,
    dangling_entries,
    trim_canonical_map,
)
from graflo.architecture.evolution.ops import ComposeManifestsOp
from graflo.architecture.evolution.preview import preview_compose
from graflo.cli.io import load_manifest, load_mapping

#: The map does not fit the manifest. Distinct from 2 (bad invocation, file
#: that will not read): the former is a statement about the map, the latter
#: about the command -- a CI job has to be able to tell them apart.
EXIT_DANGLING = 1

_SCOPES: tuple[Scope, ...] = ("left", "right", "both")


class _CanonicalSetupError(click.ClickException):
    """The check could not be run at all. Exits 2, never 1."""

    exit_code = 2


def _entry_payload(entry: DanglingEntry) -> dict[str, str | None]:
    """One dangling entry as JSON, keyed as the map spells it."""
    return {
        "side": entry.side,
        "kind": entry.kind,
        "source": entry.source,
        "target": entry.target,
        "suggestion": entry.suggestion.lstrip("; ") or None,
    }


def _report_entries(entries: tuple[DanglingEntry, ...], *, as_json: bool) -> None:
    if as_json:
        click.echo(
            json.dumps(
                {
                    "dangling": [_entry_payload(entry) for entry in entries],
                    "count": len(entries),
                },
                indent=2,
            )
        )
        return
    if not entries:
        click.echo("ok: every entry matches the manifest")
        return
    noun = "entry" if len(entries) == 1 else "entries"
    click.echo(f"{len(entries)} {noun} match nothing in the manifest:")
    for entry in entries:
        click.echo(f"  {entry.describe()}{entry.suggestion}")


def _report_preview(left: GraphManifest, right: GraphManifest, cm, scope, as_json):
    """Both manifests given: the full declaration report, not composed."""
    preview = preview_compose(
        left,
        right,
        ComposeManifestsOp(canonical_maps={scope: cm}),
        attempt=False,
    )
    if as_json:
        click.echo(json.dumps(preview.model_dump(mode="json"), indent=2))
    else:
        for finding in preview.findings:
            click.echo(f"{finding.severity}: {finding.kind}: {finding.message}")
        if not preview.findings:
            click.echo("ok: every entry matches the manifests")
    return [f for f in preview.findings if f.kind == "dangling"]


@click.command("canonical-check")
@click.argument(
    "map_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--left",
    "left_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="The left manifest the map is checked against.",
)
@click.option(
    "--right",
    "right_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="The right manifest the map is checked against.",
)
@click.option(
    "--scope",
    type=click.Choice(_SCOPES),
    default="left",
    show_default=True,
    help="Which side the map is scoped to, as on a compose op.",
)
@click.option(
    "--trim",
    "trim_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Write the map narrowed to the entries that match, dropping the rest. "
        "Needs a single manifest to narrow against, so --scope must name one."
    ),
)
@click.option("--json", "as_json", is_flag=True, help="Emit the report as JSON.")
@click.option(
    "--exit-zero",
    is_flag=True,
    help="Report, but always exit 0.",
)
def canonical_check(
    map_path: Path,
    left_path: Path | None,
    right_path: Path | None,
    scope: Scope,
    trim_path: Path | None,
    as_json: bool,
    exit_zero: bool,
) -> None:
    """Check the canonical map at MAP_PATH against the manifest(s) it maps."""
    if left_path is None and right_path is None:
        raise click.UsageError("at least one of --left / --right is required")

    try:
        cm = CanonicalMap.model_validate(load_mapping(map_path))
    except ValueError as exc:
        raise _CanonicalSetupError(
            f"{map_path}: not a valid canonical map -- {type(exc).__name__}: {exc}"
        ) from exc

    try:
        left = load_manifest(left_path) if left_path is not None else None
        right = load_manifest(right_path) if right_path is not None else None
    except (ValueError, TypeError, KeyError) as exc:
        raise _CanonicalSetupError(
            f"could not read a manifest -- {type(exc).__name__}: {exc}"
        ) from exc

    entries: tuple[DanglingEntry, ...] = ()
    if left is not None and right is not None:
        found = _report_preview(left, right, cm, scope, as_json)
        dangling_found = bool(found)
    else:
        side = "right" if right is not None else "left"
        manifest = right if right is not None else left
        assert manifest is not None  # one of the two, checked above
        entries = dangling_entries(cm, manifest, side=side)
        _report_entries(entries, as_json=as_json)
        dangling_found = bool(entries)

    if trim_path is not None:
        _write_trimmed(cm, left, right, scope, trim_path)

    if exit_zero:
        return
    if dangling_found:
        raise SystemExit(EXIT_DANGLING)


def _write_trimmed(
    cm: CanonicalMap,
    left: GraphManifest | None,
    right: GraphManifest | None,
    scope: Scope,
    trim_path: Path,
) -> None:
    """Narrow the map to one manifest and write it, announcing what went."""
    if scope == "both":
        raise click.UsageError(
            "--trim narrows a map against one manifest: use --scope left or "
            "--scope right to say which"
        )
    manifest = left if scope == "left" else right
    if manifest is None:
        raise click.UsageError(f"--trim with --scope {scope} needs --{scope}")
    trimmed, dropped = trim_canonical_map(cm, manifest, side=scope)
    trim_path.parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(trimmed.to_dict(skip_defaults=True), trim_path)
    click.echo(f"written: {trim_path} ({len(dropped)} dropped)")


__all__ = ["canonical_check"]
