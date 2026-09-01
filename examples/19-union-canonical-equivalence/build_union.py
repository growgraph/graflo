"""
Build the union of two manifests in a canonical vocabulary, with a
conditionally-equivalent boundary class — composed entirely from fundamental
evolution ops.

The recipe, in order:

1. canonicalize A standalone (``canonical_map_to_ops`` + ``apply_evolution``)
2. validate the compose op against the canonical map — fails loudly on stale
   names, retargeted attributes, or unacknowledged merges
3. ``compose_manifests`` with an explicit ``VertexEquivalence``
4. apply the identity alignment: ``alignment_to_ops`` emits only fundamentals
   (declare canonical attrs → per-resource derivation transforms → priority
   funnel over canonical attrs → per-side secondary identities)

A primary identity is a property of the class: the funnel references only
canonical attributes (``match_key``, ``local_key``). How each source populates
them — gating, normalization, namespacing — is resource knowledge, appended to
the resource pipelines as ops. The source manifests stay pure.

    cd examples/19-union-canonical-equivalence
    uv run python build_union.py                # → artifacts/manifest_union.yaml
    uv run python build_union.py --stale-demo   # the validator failing loudly
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
    alignment_to_ops,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_compose_against_canonical_map,
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
                "r_b": DerivationSpec(
                    # Empty prefix = always-true gate: same normalization,
                    # one code path, no drift between the two sides.
                    input=["org_id", "shared_raw"],
                    params={"prefix": "", "strip_prefix": "ABC-"},
                ),
            },
        )
    ],
    local_key=LocalKeySpec(
        sources={
            "r_a": LocalKeySource(field="firm_id", tag="a"),
            "r_b": LocalKeySource(field="org_id", tag="b"),
        }
    ),
    secondary_identities={
        "by_company_id": ["company_id"],
        "by_org_id": ["org_id"],
    },
)


def load_manifest(path: Path) -> GraphManifest:
    manifest = GraphManifest.from_config(FileHandle.load(path))
    manifest.finish_init()
    return manifest


def build_union(*, stale_demo: bool = False) -> GraphManifest:
    canonical_map = CanonicalMap.model_validate(
        FileHandle.load(EXAMPLE_DIR / "canonical_map.yaml")
    )

    # Step 1 — canonicalize A standalone: A now speaks the canonical vocabulary.
    canonical_a = apply_evolution(
        load_manifest(EXAMPLE_DIR / "manifest_a.yaml"),
        canonical_map_to_ops(canonical_map),
    )
    manifest_b = load_manifest(EXAMPLE_DIR / "manifest_b.yaml")

    # Step 2 — declare the boundary equivalence in canonical names.
    boundary_class = "Firm" if stale_demo else "Company"
    op = ComposeManifestsOp(
        vertices=[
            VertexEquivalence(left=boundary_class, right="Org", into=boundary_class)
        ]
    )

    # Step 3 — cross-validate against the canonical map BEFORE composing.
    # With --stale-demo the equivalence uses the retired name "Firm" and this
    # raises ComposeCanonicalConflictError instead of composing a wrong union.
    validate_compose_against_canonical_map(
        canonical_map, op, left=canonical_a, right=manifest_b
    )

    # Step 4 — compose (loud on collisions: name_conflict defaults to "error").
    union = compose_manifests(canonical_a, manifest_b, op)

    # Step 5 — apply the identity alignment as fundamental ops.
    return apply_evolution(
        union,
        alignment_to_ops(ALIGNMENT, manifest=union, canonical_maps=[canonical_map]),
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
def main(output: Path, stale_demo: bool) -> None:
    union = build_union(stale_demo=stale_demo)
    output.parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(union.to_dict(), output)
    click.echo(f"Union manifest → {output}")


if __name__ == "__main__":
    main()
