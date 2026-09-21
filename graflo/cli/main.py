"""The ``graflo`` command: one umbrella over the verbs.

The package shipped ten flat console scripts (``ingest``, ``migrate_schema``,
``plot_manifest``, …) with no shared entry point, so there was nowhere for a
git-shaped verb to live -- ``graflo commit`` had no ``graflo`` to hang off.

This group is that entry point. Existing scripts keep working exactly as they
did: they are still declared in ``pyproject.toml`` and are mounted here as
subcommands, so ``ingest ...`` and ``graflo ingest ...`` are the same code. New
verbs are added here only.
"""

from __future__ import annotations

import click

from graflo.cli.canonical import canonical_check as canonical_check_cmd
from graflo.cli.check import check as check_cmd
from graflo.cli.commit import commit_group
from graflo.cli.inverses import inverses as inverses_cmd
from graflo.cli.lift import lift as lift_cmd
from graflo.cli.merge import merge as merge_cmd


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="graflo")
def graflo() -> None:
    """GraFlo: declare, evolve and version a graph world model."""


# Version control for world models. Registered as a *group* rather than
# flattened, so `graflo commit`, `graflo log`, `graflo merge3` and friends share
# the `--store` option and one help page.
for _name, _command in commit_group().items():
    graflo.add_command(_command, name=_name)

# Conformance profiles over a manifest. One verb rather than a group: the
# profile is an option, so a new profile grows no new command.
graflo.add_command(check_cmd, name="check")

# Binary merge of two manifests. Mounted here rather than in
# `_mount_existing`, whose defensive try/except exists for verbs behind
# optional extras -- this one has no extra to be missing.
graflo.add_command(merge_cmd, name="merge")

# A canonical map against the manifest it maps, without merging. Merge is
# the only thing that checked a map until now, which makes authoring one a loop
# through a refusal that reports a single entry.
graflo.add_command(canonical_check_cmd, name="canonical-check")

# Lifting a manifest into a twin-ready schema. A planner over the same op
# vocabulary `graflo evolve` applies, so the conversion is reviewable before it
# runs and invertible after.
graflo.add_command(lift_cmd, name="lift")

# Declared edge inverses: audit how each is realized, then realize, repair or
# switch. Planners over the same op vocabulary, like `lift`; a group because the
# verbs share one subject and one vocabulary of findings.
graflo.add_command(inverses_cmd, name="inverses")


def _mount_existing() -> None:
    """Mount the pre-existing console scripts as subcommands.

    Imported lazily and defensively: several of these pull optional extras
    (plotting needs pygraphviz, the TigerGraph verbs need a driver), and a
    missing extra must not take the whole CLI down with it. A verb that cannot
    import is simply absent, which is what the user can act on.
    """
    mounts = {
        "ingest": ("graflo.cli.ingest", "ingest"),
        "migrate-schema": ("graflo.cli.migrate_schema", "migrate_schema"),
        "plot-manifest": ("graflo.cli.plot_manifest", "plot_manifest"),
        "xml2json": ("graflo.cli.xml2json", "xml2json"),
        "manage-dbs": ("graflo.cli.manage_dbs", "manage_dbs"),
        "manifest-to-rdf": ("graflo.rdf.cli", "manifest_to_rdf"),
        "rdf-to-manifest": ("graflo.rdf.cli", "rdf_to_manifest"),
    }
    for name, (module_path, attribute) in mounts.items():
        try:
            module = __import__(module_path, fromlist=[attribute])
            graflo.add_command(getattr(module, attribute), name=name)
        except Exception:
            continue


_mount_existing()


if __name__ == "__main__":
    graflo()
