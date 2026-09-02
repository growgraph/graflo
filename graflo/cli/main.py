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

from graflo.cli.commit import commit_group


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="graflo")
def graflo() -> None:
    """GraFlo: declare, evolve and version a graph world model."""


# Version control for world models. Registered as a *group* rather than
# flattened, so `graflo commit`, `graflo log`, `graflo merge` and friends share
# the `--store` option and one help page.
for _name, _command in commit_group().items():
    graflo.add_command(_command, name=_name)


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
        "plot-schema": ("graflo.cli.plot_schema", "xml2json"),
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
