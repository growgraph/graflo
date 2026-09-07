"""``graflo check`` -- validate a manifest against a named conformance profile.

Distinct from ``graflo verify``, which checks that a stored history replays to
the hash it claims. This checks the *content* of one manifest against a profile
-- a versioned list of assertions saying what it takes for a schema to be a
world model rather than a table dump with edges.

The manifest is read as written, not round-tripped through the model first: two
assertions ask what the author declared, and the parsed model has already
filled in the defaults that would hide the answer.
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from graflo.architecture.profile import (
    ProfileWaivers,
    check_manifest_config,
    list_profiles,
)
from graflo.cli.io import load_mapping

#: Non-conformant. Distinct from 2 (could not read the file) on purpose -- a CI
#: job has to tell "your model is wrong" from "I could not run the check".
EXIT_NONCONFORMANT = 1


class _CheckSetupError(click.ClickException):
    """The check could not be run at all. Exits 2, never 1.

    ``click.ClickException`` exits 1, which is the code this verb already uses
    for "checked, and it failed". Collapsing the two would leave a CI job
    unable to tell a non-conformant model from an unreadable file.
    """

    exit_code = 2


@click.command("check")
@click.argument(
    "manifest",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
)
@click.option(
    "--profile",
    "profile_name",
    default="world-model",
    show_default=True,
    help="Conformance profile to check against.",
)
@click.option(
    "--waivers",
    "waivers_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Sidecar document of operator waivers. A waived assertion is reported "
        "as waived with its reason, never as a pass."
    ),
)
@click.option("--json", "as_json", is_flag=True, help="Emit the report as JSON.")
@click.option(
    "--warnings-as-errors",
    is_flag=True,
    help="Treat advisory findings as failures.",
)
@click.option(
    "--exit-zero",
    is_flag=True,
    help="Always exit 0. For an advisory CI run that should report, not gate.",
)
@click.option(
    "--list-profiles",
    "do_list",
    is_flag=True,
    help="List the known profiles and exit.",
)
def check(
    manifest: Path | None,
    profile_name: str,
    waivers_path: Path | None,
    as_json: bool,
    warnings_as_errors: bool,
    exit_zero: bool,
    do_list: bool,
) -> None:
    """Check MANIFEST against a conformance profile."""
    if do_list:
        for name, version in list_profiles():
            click.echo(f"{name}  v{version}")
        return

    if manifest is None:
        raise click.UsageError("MANIFEST is required unless --list-profiles is given")

    config = load_mapping(manifest)
    waivers = (
        ProfileWaivers.model_validate(load_mapping(waivers_path))
        if waivers_path is not None
        else None
    )

    try:
        report = check_manifest_config(
            config,
            profile=profile_name,
            waivers=waivers,
            subject=str(manifest),
        )
    except KeyError as exc:
        # `get_profile` names the profiles that do exist; that message is the
        # useful half, and ClickException prints it without a traceback.
        raise _CheckSetupError(str(exc).strip("\"'")) from exc
    except (ValueError, TypeError) as exc:
        raise _CheckSetupError(
            f"{manifest}: not a valid manifest -- {type(exc).__name__}: {exc}"
        ) from exc

    if as_json:
        click.echo(json.dumps(report.model_dump(mode="json"), indent=2))
    else:
        for line in report.to_lines():
            click.echo(line)

    if exit_zero:
        return
    if not report.ok or (warnings_as_errors and report.warnings()):
        raise SystemExit(EXIT_NONCONFORMANT)


__all__ = ["check"]
