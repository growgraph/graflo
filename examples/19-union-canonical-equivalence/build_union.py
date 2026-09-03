"""
Build the union of two manifests in a canonical vocabulary, with a single
n-ary boundary cluster — composed entirely from fundamental evolution ops.

The recipe, in order:

1. canonicalize A standalone (``canonical_map_to_ops`` + ``apply_evolution``)
2. validate + complete the canonical map against the compose op — fails loudly
   on stale names, retargeted attributes, cluster conflicts (an equivalence
   overlap, two declarations sharing one ``into``, or an ``into`` that would
   silently occupy an existing non-member class), or an unacknowledged merge;
   completes unmapped peers (e.g. ``Org → Company``) along the cluster
3. ``compose_manifests`` with one n-ary ``VertexEquivalence`` naming every
   member on each side *and* ``identity_alignments`` on the same op (schema
   union + identity alignment)

A primary identity is a property of the class: the funnel references only
canonical attributes (``match_key``, ``local_key``). How each source populates
them — gating, normalization, namespacing — is resource knowledge, appended to
the resource pipelines as ops. The source manifests stay pure.

    cd examples/19-union-canonical-equivalence
    uv run python build_union.py                      # → artifacts/manifest_union.yaml
    uv run python build_union.py --stale-demo         # stale pre-canonical name
    uv run python build_union.py --conflicting-cluster-demo
"""

from __future__ import annotations

from pathlib import Path

import click
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.evolution import (
    AlignmentRow,
    CanonicalMap,
    ComposeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_and_complete_canonical_map,
)

EXAMPLE_DIR = Path(__file__).resolve().parent

# The identity alignment: rows are canonical attributes in priority order;
# derivation inputs are RAW source-doc field names (renamed documents still
# carry their original keys). The local_key fallback keeps non-gated records
# ingested as their own entities, namespaced per resource so cross-side
# collisions are impossible.
ALIGNMENT = IdentityAlignment(
    vertex="Company",
    rows=[
        AlignmentRow(
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


def _boundary_op(*, stale_demo: bool, conflicting_cluster: bool) -> ComposeManifestsOp:
    """One n-ary cluster: {Company, Shop} ~ {Org, Branch} → Company.

    ``left`` / ``right`` name every member collapsing onto ``into`` in a
    single declaration — the equivalence layer refuses two *separate*
    declarations that overlap or disagree rather than silently picking one.
    The conflicting-cluster demo authors exactly that mistake.
    """
    if conflicting_cluster:
        return ComposeManifestsOp(
            vertices=[
                VertexEquivalence(
                    left="Company", right=["Org", "Branch"], into="Company"
                ),
                # Shares right:Org with the declaration above but targets a
                # different `into` — an overlap, not a second independent
                # cluster.
                VertexEquivalence(left="Shop", right="Org", into="Party"),
            ],
            allow_merges=True,
        )

    boundary = "Firm" if stale_demo else "Company"
    return ComposeManifestsOp(
        vertices=[
            VertexEquivalence(
                left=[boundary, "Shop"], right=["Org", "Branch"], into=boundary
            )
        ],
        allow_merges=True,
        identity_alignments=[] if stale_demo else [ALIGNMENT],
    )


def build_union(
    *, stale_demo: bool = False, conflicting_cluster: bool = False
) -> GraphManifest:
    canonical_map = CanonicalMap.model_validate(
        FileHandle.load(EXAMPLE_DIR / "canonical_map.yaml")
    )

    # Step 1 — canonicalize A standalone: Firm → Company; Shop stays Shop.
    canonical_a = apply_evolution(
        load_manifest(EXAMPLE_DIR / "manifest_a.yaml"),
        canonical_map_to_ops(canonical_map),
    )
    manifest_b = load_manifest(EXAMPLE_DIR / "manifest_b.yaml")

    # Step 2 — declare the n-ary boundary equivalence in canonical names.
    op = _boundary_op(stale_demo=stale_demo, conflicting_cluster=conflicting_cluster)

    # Step 3 — validate + complete against the canonical map BEFORE composing.
    # Completes Org/Branch/Shop → Company along the cluster; --stale-demo
    # raises ComposeCanonicalConflictError and --conflicting-cluster-demo
    # raises ClusterConflictError (wrapped by the validator).
    validate_and_complete_canonical_map(
        op,
        left=canonical_a,
        right=manifest_b,
        canonical_maps=[("left", canonical_map)],
    )

    # Step 4 — compose (loud on collisions) and apply identity_alignments
    # declared on the op.
    return compose_manifests(
        canonical_a,
        manifest_b,
        op,
        canonical_maps=[("left", canonical_map)],
    )


@click.command()
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=EXAMPLE_DIR / "artifacts" / "manifest_union.yaml",
    show_default=True,
    help="Where to write the composed manifest.",
)
@click.option(
    "--stale-demo",
    is_flag=True,
    help="Author the equivalence with a pre-canonical class name to see the "
    "validator fail loudly.",
)
@click.option(
    "--conflicting-cluster-demo",
    is_flag=True,
    help="Declare overlapping equivalences with disagreeing `into` labels to "
    "see the cluster-conflict detector fail loudly.",
)
def main(output: Path, stale_demo: bool, conflicting_cluster_demo: bool) -> None:
    if stale_demo and conflicting_cluster_demo:
        raise click.UsageError(
            "pass at most one of --stale-demo / --conflicting-cluster-demo"
        )
    union = build_union(
        stale_demo=stale_demo, conflicting_cluster=conflicting_cluster_demo
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(union.to_dict(), output)
    click.echo(f"Union manifest → {output}")


if __name__ == "__main__":
    main()
