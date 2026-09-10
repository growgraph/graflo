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

A primary identity is a property of the class: the funnel references only
canonical attributes (``match_key``, ``local_key``). How each source populates
them — gating, normalization, namespacing — is resource knowledge, appended to
the resource pipelines as ops. The source manifests stay pure.

    cd examples/19-union-canonical-equivalence
    uv run python build_union.py                      # → artifacts/manifest_union.yaml
    uv run python build_union.py --disagreeing-map-demo
    uv run python build_union.py --conflicting-cluster-demo
"""

from __future__ import annotations

from pathlib import Path

import click
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.evolution import (
    AlignmentAttribute,
    CanonicalMap,
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
            into="match_key",
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
    *, disagreeing_map: bool = False, conflicting_cluster: bool = False
) -> GraphManifest:
    canonical_map = CanonicalMap.model_validate(
        FileHandle.load(EXAMPLE_DIR / "canonical_map.yaml")
    )
    manifest_a = load_manifest(EXAMPLE_DIR / "manifest_a.yaml")
    manifest_b = load_manifest(EXAMPLE_DIR / "manifest_b.yaml")

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


@click.command()
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
def main(
    output: Path, disagreeing_map_demo: bool, conflicting_cluster_demo: bool
) -> None:
    if disagreeing_map_demo and conflicting_cluster_demo:
        raise click.UsageError(
            "pass at most one of --disagreeing-map-demo / --conflicting-cluster-demo"
        )
    union = build_union(
        disagreeing_map=disagreeing_map_demo,
        conflicting_cluster=conflicting_cluster_demo,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(union.to_dict(), output)
    click.echo(f"Union manifest → {output}")


if __name__ == "__main__":
    main()
