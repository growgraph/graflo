"""
Build the union of two manifests in a canonical vocabulary, with a single
n-ary boundary cluster — composed entirely from fundamental evolution ops.

Two declarations, one recipe:

1. the **canonical map** says what things are called — ``Firm`` is
   ``Company``, ``firm_id`` is ``company_id``;
2. the **equivalence cluster** says which classes are one — ``{Firm, Shop}``
   on A and ``{Org, Branch}`` on B — and leaves the composed name to the map.

``compose_manifests`` resolves both into one composite map per side and
applies it in one step before the union, so the equivalence is written in
A's own vocabulary and nothing has to be renamed by hand first. It refuses
when the two declarations disagree (``--disagreeing-map-demo``), and when
clusters contradict each other (``--conflicting-cluster-demo``).

Two declarations can also be consistent but *incomplete* — they leave a class
unaccounted for. Compose refuses those too, and hands back the declaration
that would settle it: ``--forgotten-member-demo`` leaves a member out of the
cluster, ``--shared-name-demo`` leaves a name both sides arrive at
undeclared. Both print their completion.

A primary identity is a property of the class: the funnel references only
canonical attributes (``match_key``, ``local_key``). How each source populates
them — gating, normalization, namespacing — is resource knowledge, appended to
the resource pipelines as ops. The source manifests stay pure.

Each refusal is one problem: compose stops at the first. ``--plot-dir`` draws
every mode instead — the declaration graph, with *all* the conflicts in it —
through ``preview_compose``, which walks the same checks without refusing. The
conflicting-cluster mode is the one to look at: compose reports the overlap,
the picture also shows the two identity disagreements behind it.

    cd examples/19-union-canonical-equivalence
    uv run python build_union.py                      # → artifacts/manifest_union.yaml
    uv run python build_union.py --disagreeing-map-demo
    uv run python build_union.py --conflicting-cluster-demo
    uv run python build_union.py --forgotten-member-demo
    uv run python build_union.py --shared-name-demo [--union-right]
    uv run python build_union.py --plot-dir figs      # → figs/union-<mode>.svg
"""

from __future__ import annotations

from pathlib import Path

import click
import yaml
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.evolution import (
    AlignmentAttribute,
    CanonicalMap,
    ComposeIncompleteError,
    ComposeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    VertexEquivalence,
    compose_manifests,
)

EXAMPLE_DIR = Path(__file__).resolve().parent

# The identity alignment: attributes are canonical attributes in priority order;
# derivation inputs are RAW source-doc field names (renamed documents still
# carry their original keys). The local_key fallback keeps non-gated records
# ingested as their own entities, namespaced per resource so cross-side
# collisions are impossible.
ALIGNMENT = IdentityAlignment(
    vertex="Company",
    attributes=[
        AlignmentAttribute(
            name="match_key",
            sources={
                "r_a": DerivationSpec(
                    input=["secondary_key", "shared_raw"],
                    params={"prefix": "abc_", "strip_prefix": "ABC-"},
                ),
                "r_shop": DerivationSpec(
                    input=["secondary_key", "shared_raw"],
                    params={"prefix": "abc_", "strip_prefix": "ABC-"},
                ),
                "r_b": DerivationSpec(
                    # Empty prefix = always-true gate: same normalization,
                    # one code path, no drift between the two sides.
                    input=["org_id", "shared_raw"],
                    params={"prefix": "", "strip_prefix": "ABC-"},
                ),
                "r_branch": DerivationSpec(
                    input=["branch_id", "shared_raw"],
                    params={"prefix": "", "strip_prefix": "ABC-"},
                ),
            },
        )
    ],
    local_key=LocalKeySpec(
        sources={
            "r_a": LocalKeySource(field="firm_id", tag="a"),
            "r_shop": LocalKeySource(field="shop_id", tag="shop"),
            "r_b": LocalKeySource(field="org_id", tag="b"),
            "r_branch": LocalKeySource(field="branch_id", tag="br"),
        }
    ),
    secondary_identities={
        "by_company_id": ["company_id"],
        "by_shop_id": ["shop_id"],
        "by_org_id": ["org_id"],
        "by_branch_id": ["branch_id"],
    },
)


def load_manifest(path: Path) -> GraphManifest:
    manifest = GraphManifest.from_config(FileHandle.load(path))
    manifest.finish_init()
    return manifest


def _forgotten_member_op(canonical_map: CanonicalMap) -> ComposeManifestsOp:
    """The cluster names ``Firm`` only, while the map also sends ``Shop`` to ``Company``.

    Consistent, but incomplete: ``Shop`` arrives at a composed class without
    being declared a member of it, so the cluster's identity and property maps
    would never govern it. Compose refuses with a ``Completion`` carrying the
    same cluster with ``Shop`` added — the commonest real mistake, answered
    with the exact fix.
    """
    extended = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Company"},
            "allow_merges": True,
        }
    )
    return ComposeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right=["Org", "Branch"])],
        allow_merges=True,
        canonical_maps={"left": extended},
    )


def _shared_name_op(
    canonical_map: CanonicalMap, *, union_right: bool
) -> ComposeManifestsOp:
    """Both sides canonicalize a class to ``Outlet``, and no cluster composes it.

    The maps alone make the two sides meet at a name — ``Shop`` on A and
    ``Branch`` on B — which is not a disjoint union and is never silently
    treated as one. Under ``error`` compose refuses, and the completion is the
    ``{Outlet} ~ {Outlet}`` declaration to add; under ``union_right`` it
    declares that equivalence itself (a *synthesized* cluster) and composes.

    The maps align the keys too (``shop_id`` / ``branch_id`` -> ``outlet_id``,
    ``org_id`` -> ``company_id``). Drop either alignment and the union raises
    ``ComposeIdentityError``: a synthesized cluster is a real cluster, so its
    members must agree on an identity exactly as a declared one's must.
    """
    left = canonical_map.model_copy(
        update={
            "vertices": {**canonical_map.vertices, "Shop": "Outlet"},
            "properties": {
                **canonical_map.properties,
                "Shop": {"shop_id": "outlet_id"},
            },
        }
    )
    right = CanonicalMap(
        vertices={"Branch": "Outlet"},
        properties={
            "Branch": {"branch_id": "outlet_id"},
            "Org": {"org_id": "company_id"},
        },
    )
    return ComposeManifestsOp(
        vertex_equivalences=[VertexEquivalence(left="Firm", right="Org")],
        canonical_maps={"left": left, "right": right},
        name_conflict="union_right" if union_right else "error",
    )


def _boundary_op(
    canonical_map: CanonicalMap, *, disagreeing_map: bool, conflicting_cluster: bool
) -> ComposeManifestsOp:
    """One n-ary cluster: {Firm, Shop} ~ {Org, Branch}, named Company by the map.

    ``left`` / ``right`` name every member collapsing together in a single
    declaration, in each manifest's own vocabulary — the equivalence layer
    refuses two *separate* declarations that overlap or disagree rather than
    silently picking one. The conflicting-cluster demo authors exactly that
    mistake; the disagreeing-map demo names the composed class ``Party`` while
    the map says ``Firm`` is ``Company``.
    """
    if conflicting_cluster:
        return ComposeManifestsOp(
            vertex_equivalences=[
                VertexEquivalence(left="Firm", right=["Org", "Branch"]),
                # Shares right:Org with the declaration above but targets a
                # different `into` — an overlap, not a second independent
                # cluster.
                VertexEquivalence(left="Shop", right="Org", into="Party"),
            ],
            allow_merges=True,
            canonical_maps={"left": canonical_map},
        )

    return ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"],
                right=["Org", "Branch"],
                into="Party" if disagreeing_map else None,
            )
        ],
        allow_merges=True,
        canonical_maps={"left": canonical_map},
        identity_alignments=[] if disagreeing_map else [ALIGNMENT],
    )


def build_union(
    *,
    disagreeing_map: bool = False,
    conflicting_cluster: bool = False,
    forgotten_member: bool = False,
    shared_name: bool = False,
    union_right: bool = False,
) -> GraphManifest:
    canonical_map = CanonicalMap.model_validate(
        FileHandle.load(EXAMPLE_DIR / "canonical_map.yaml")
    )
    manifest_a = load_manifest(EXAMPLE_DIR / "manifest_a.yaml")
    manifest_b = load_manifest(EXAMPLE_DIR / "manifest_b.yaml")

    # The two incompleteness demos: consistent declarations that leave a class
    # unaccounted for. Each refusal carries the declaration that settles it.
    if forgotten_member:
        return compose_manifests(
            manifest_a, manifest_b, _forgotten_member_op(canonical_map)
        )
    if shared_name:
        return compose_manifests(
            manifest_a,
            manifest_b,
            _shared_name_op(canonical_map, union_right=union_right),
        )

    # The op carries the cluster, the canonical map and the identity
    # alignment: one recipe. Compose resolves the cluster's composed name
    # (Company, from the map), checks the two declarations agree, applies one
    # composite map per side in a single step, unions by name, then aligns
    # identity. --disagreeing-map-demo raises ComposeCanonicalConflictError
    # and --conflicting-cluster-demo raises ClusterConflictError.
    op = _boundary_op(
        canonical_map,
        disagreeing_map=disagreeing_map,
        conflicting_cluster=conflicting_cluster,
    )
    return compose_manifests(manifest_a, manifest_b, op)


#: Every declaration the demos author, by the flag that selects it. Kept as
#: one table so the figures and the single-mode runs cannot drift apart.
def ops_by_mode(canonical_map: CanonicalMap) -> dict[str, ComposeManifestsOp]:
    """Each demo's compose op, keyed by the mode that selects it."""
    return {
        "default": _boundary_op(
            canonical_map, disagreeing_map=False, conflicting_cluster=False
        ),
        "disagreeing-map": _boundary_op(
            canonical_map, disagreeing_map=True, conflicting_cluster=False
        ),
        "conflicting-cluster": _boundary_op(
            canonical_map, disagreeing_map=False, conflicting_cluster=True
        ),
        "forgotten-member": _forgotten_member_op(canonical_map),
        "shared-name": _shared_name_op(canonical_map, union_right=False),
        "shared-name-union-right": _shared_name_op(canonical_map, union_right=True),
    }


def plot_modes(plot_dir: Path) -> list[Path]:
    """Draw every mode's declaration graph and its conflicts.

    One figure per mode. The preview never refuses, so a mode compose rejects
    still produces a picture — which is the case the picture is for.
    """
    from graflo.architecture.evolution.preview import preview_compose
    from graflo.plot.compose import plot_compose_preview

    canonical_map = CanonicalMap.model_validate(
        FileHandle.load(EXAMPLE_DIR / "canonical_map.yaml")
    )
    manifest_a = load_manifest(EXAMPLE_DIR / "manifest_a.yaml")
    manifest_b = load_manifest(EXAMPLE_DIR / "manifest_b.yaml")

    written: list[Path] = []
    for mode, op in ops_by_mode(canonical_map).items():
        preview = preview_compose(manifest_a, manifest_b, op)
        written.append(plot_compose_preview(preview, plot_dir / f"union-{mode}.svg"))
        blocking = len(preview.blocking)
        click.echo(f"{mode:24} {blocking} finding(s) → {written[-1].name}")
    return written


@click.command()
@click.option(
    "--plot-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Draw every mode's declaration graph and conflicts into this directory.",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=EXAMPLE_DIR / "artifacts" / "manifest_union.yaml",
    show_default=True,
    help="Where to write the composed manifest.",
)
@click.option(
    "--disagreeing-map-demo",
    is_flag=True,
    help="Name the composed class differently from the canonical map to see "
    "compose refuse the contradiction.",
)
@click.option(
    "--conflicting-cluster-demo",
    is_flag=True,
    help="Declare overlapping equivalences with disagreeing `into` labels to "
    "see the cluster-conflict detector fail loudly.",
)
@click.option(
    "--forgotten-member-demo",
    is_flag=True,
    help="Leave `Shop` out of the cluster while the map still sends it to "
    "`Company`, to see the completion that adds it back.",
)
@click.option(
    "--shared-name-demo",
    is_flag=True,
    help="Let both sides canonicalize a class to `Outlet` with no cluster "
    "composing it; add --union-right to union it by name instead of refusing.",
)
@click.option(
    "--union-right",
    is_flag=True,
    help="With --shared-name-demo: union the shared name through a "
    "synthesized equivalence rather than refusing.",
)
def main(
    plot_dir: Path | None,
    output: Path,
    disagreeing_map_demo: bool,
    conflicting_cluster_demo: bool,
    forgotten_member_demo: bool,
    shared_name_demo: bool,
    union_right: bool,
) -> None:
    if plot_dir is not None:
        plot_modes(plot_dir)
        return

    demos = {
        "--disagreeing-map-demo": disagreeing_map_demo,
        "--conflicting-cluster-demo": conflicting_cluster_demo,
        "--forgotten-member-demo": forgotten_member_demo,
        "--shared-name-demo": shared_name_demo,
    }
    chosen = [name for name, on in demos.items() if on]
    if len(chosen) > 1:
        raise click.UsageError(f"pass at most one of {' / '.join(demos)}")
    if union_right and not shared_name_demo:
        raise click.UsageError("--union-right only applies to --shared-name-demo")

    try:
        union = build_union(
            disagreeing_map=disagreeing_map_demo,
            conflicting_cluster=conflicting_cluster_demo,
            forgotten_member=forgotten_member_demo,
            shared_name=shared_name_demo,
            union_right=union_right,
        )
    except ComposeIncompleteError as exc:
        # The declarations are consistent, just not covering. The completion is
        # the declaration to paste into the op -- `graflo compose` prints it
        # the same way.
        click.echo(f"compose refused: {exc}", err=True)
        click.echo("completion:", err=True)
        click.echo(
            yaml.safe_dump(exc.completion.to_dict(), sort_keys=False).rstrip(), err=True
        )
        raise SystemExit(1)

    if chosen:
        # A demo that composed rather than refusing: report it, write nothing,
        # so the committed artifact stays the one the default path produces.
        click.echo(f"composed with {chosen[0]}; no artifact written")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(union.to_dict(), output)
    click.echo(f"Union manifest → {output}")


if __name__ == "__main__":
    main()
