"""``graflo compose`` -- the binary compose of two manifests, from the shell.

This verb is ``examples/19-union-canonical-equivalence/build_union.py``
generalised. That script spells out the recipe compose actually requires --
validate and complete the canonical map *against the op* before composing, so
an equivalence written in a stale pre-canonical name fails loudly instead of
silently matching nothing -- and every caller needs the same three steps. They
live here now, and the example points at the verb.

Either side may carry no ``schema`` block: a manifest with only an
``ingestion_model`` and/or ``bindings`` is a new source wired onto an existing
type vocabulary, and composing it is the point of the overlay shape.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.alignment import AlignmentConflictError
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.canonical import (
    CanonicalMap,
    ComposeCanonicalConflictError,
    canonical_map_to_ops,
    validate_and_complete_canonical_map,
)
from graflo.architecture.evolution.compose import (
    ComposeIdentityError,
    ComposeNameConflictError,
    compose_manifests,
)
from graflo.architecture.evolution.equivalence import ClusterConflictError, Side
from graflo.architecture.evolution.ops import ComposeManifestsOp
from graflo.architecture.profile import check_manifest
from graflo.cli.io import dump_manifest, load_manifest, load_mapping

#: Compose refused the inputs -- a name collision, a cluster conflict, a stale
#: canonical name. Distinct from 2 (bad invocation, unreadable file): the
#: former is a statement about the manifests, the latter about the command.
EXIT_REFUSED = 1


class _ComposeSetupError(click.ClickException):
    """The compose could not be attempted at all. Exits 2, never 1."""

    exit_code = 2


def _parse_canonical_map_option(values: tuple[str, ...]) -> list[tuple[Side, Path]]:
    """``--canonical-map SIDE=PATH`` pairs, validated on the side token."""
    parsed: list[tuple[Side, Path]] = []
    for value in values:
        side, sep, raw_path = value.partition("=")
        if not sep or side not in ("left", "right"):
            raise click.UsageError(
                f"--canonical-map expects SIDE=PATH with SIDE in "
                f"{{left, right}}, got {value!r}"
            )
        path = Path(raw_path)
        if not path.is_file():
            raise click.UsageError(f"--canonical-map {side}: no such file: {raw_path}")
        parsed.append((side, path))
    return parsed


@click.command("compose")
@click.argument("left", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("right", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--op",
    "op_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=(
        "ComposeManifestsOp document: vertex/property/relation equivalences "
        "and identity alignments. Omitted composes a disjoint union."
    ),
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Where to write the composed manifest. Omitted prints a summary only.",
)
@click.option(
    "--canonical-map",
    "canonical_map_options",
    multiple=True,
    metavar="SIDE=PATH",
    help=(
        "Canonical map for one side, repeatable. Validated against the op "
        "before composing, so an equivalence naming a pre-canonical class "
        "fails rather than matching nothing."
    ),
)
@click.option(
    "--name-conflict",
    type=click.Choice(["error", "prefix_right", "fuse_right"]),
    default=None,
    help="Override the op's name_conflict policy.",
)
@click.option(
    "--bump-version",
    type=click.Choice(["minor", "none"]),
    default="minor",
    show_default=True,
    help="Bump the composed schema version. 'none' leaves the left's.",
)
@click.option(
    "--strict-references",
    is_flag=True,
    help="Fail on ingestion/bindings references the composed schema lacks.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Compose and report, but write nothing.",
)
@click.option(
    "--check-profile",
    "profile_name",
    default=None,
    help=(
        "Also check the composed manifest against this conformance profile "
        "and print the report. Findings do not change the exit code."
    ),
)
def compose(
    left: Path,
    right: Path,
    op_path: Path | None,
    output: Path | None,
    canonical_map_options: tuple[str, ...],
    name_conflict: str | None,
    bump_version: str,
    strict_references: bool,
    dry_run: bool,
    profile_name: str | None,
) -> None:
    """Compose LEFT and RIGHT into one manifest."""
    canonical_map_paths = _parse_canonical_map_option(canonical_map_options)

    try:
        left_manifest = load_manifest(left)
        right_manifest = load_manifest(right)
    except (ValueError, TypeError) as exc:
        raise _ComposeSetupError(
            f"not a valid manifest -- {type(exc).__name__}: {exc}"
        ) from exc

    payload: dict[str, Any] = load_mapping(op_path) if op_path is not None else {}
    if name_conflict is not None:
        payload["name_conflict"] = name_conflict
    try:
        # `op` is a Literal with a default, so a document carrying
        # `op: compose_manifests` validates as written -- no key to strip.
        op = ComposeManifestsOp.model_validate(payload)
    except ValueError as exc:
        raise _ComposeSetupError(f"{op_path}: invalid compose op -- {exc}") from exc

    canonical_maps = [
        (side, CanonicalMap.model_validate(load_mapping(path)))
        for side, path in canonical_map_paths
    ]

    try:
        # Step 1 -- canonicalize each mapped side standalone. The op is
        # authored in canonical names, so the membership check inside compose
        # is against post-canonical classes; skipping this makes an
        # equivalence naming the canonical class fail as "not in manifest".
        for side, canonical_map in canonical_maps:
            ops = canonical_map_to_ops(
                canonical_map,
                allow_self_relations=op.allow_self_relations,
                allow_observation_fusion=op.allow_observation_fusion,
            )
            if side == "left":
                left_manifest = apply_evolution(left_manifest, ops)
            else:
                right_manifest = apply_evolution(right_manifest, ops)

        # Step 2 -- validate and complete the map against the op, before
        # composing: a stale pre-canonical name fails loudly here rather than
        # matching nothing later.
        if canonical_maps:
            validate_and_complete_canonical_map(
                op,
                left=left_manifest,
                right=right_manifest,
                canonical_maps=canonical_maps,
            )
        composed = compose_manifests(
            left_manifest,
            right_manifest,
            op,
            bump_version="minor" if bump_version == "minor" else False,
            strict_references=strict_references,
            canonical_maps=canonical_maps,
        )
    except (
        AlignmentConflictError,
        ClusterConflictError,
        ComposeCanonicalConflictError,
        ComposeIdentityError,
        ComposeNameConflictError,
        ValueError,
    ) as exc:
        # Every one of these carries what to declare next; a traceback would
        # bury it.
        click.echo(f"compose refused: {type(exc).__name__}: {exc}", err=True)
        raise SystemExit(EXIT_REFUSED)

    for line in _summary(composed):
        click.echo(line)

    if profile_name is not None:
        # The model, not a re-serialization of it. A composed manifest has no
        # authored document -- and neither serialization is a substitute:
        # `skip_defaults=True` drops a `directed: true` the author *did* write
        # (it equals the default), while `skip_defaults=False` writes one they
        # did not. Both would answer the two declaration assertions with
        # confident nonsense. `check_manifest` degrades them to a warning that
        # says exactly this, which is the honest report for a composed result.
        report = check_manifest(
            composed, profile=profile_name, subject=f"{left} + {right}"
        )
        for line in report.to_lines():
            click.echo(line)

    if dry_run:
        click.echo("dry run: nothing written")
        return
    dump_manifest(composed, output)


def _summary(manifest: GraphManifest) -> list[str]:
    lines: list[str] = []
    schema = manifest.graph_schema
    if schema is None:
        lines.append("schema: none (both sides schema-less)")
    else:
        core = schema.core_schema
        lines.append(
            f"schema: {len(core.vertex_config.vertices)} vertices, "
            f"{len(core.edge_config.edges)} edges, version {schema.metadata.version}"
        )
    if manifest.ingestion_model is not None:
        lines.append(f"resources: {len(manifest.ingestion_model.resources)}")
    if manifest.bindings is not None:
        lines.append(f"connectors: {len(manifest.bindings.connectors)}")
    return lines


__all__ = ["compose"]
