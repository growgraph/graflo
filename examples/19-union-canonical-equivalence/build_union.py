"""
Build the union of two manifests in a canonical vocabulary, with a
conditionally-equivalent boundary class.

The recipe, in order:

1. canonicalize A standalone (``canonical_map_to_ops`` + ``apply_evolution``)
2. validate the compose op against the canonical map — fails loudly on stale
   names, retargeted attributes, or unacknowledged merges
3. ``compose_manifests`` with an explicit ``VertexEquivalence``
4. install the conditional-equivalence identity funnel post-compose

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
    CanonicalMap,
    ComposeManifestsOp,
    FunnelIdentityTarget,
    IdentityReplacement,
    ReplaceIdentityOp,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_compose_against_canonical_map,
)
from graflo.architecture.schema.identity_funnel import IdentityBranch, IdentityFunnel

EXAMPLE_DIR = Path(__file__).resolve().parent

# The equivalence funnel: shared evidence first, side-local keys as fallback.
# Only records the gate transform gave a match_key can take the shared branch.
FUNNEL = IdentityFunnel(
    branches=[
        IdentityBranch(id="shared", fields=["match_key"]),
        IdentityBranch(id="a_local", fields=["company_id"]),
        IdentityBranch(id="b_local", fields=["org_id"]),
    ]
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

    # Step 5 — install the conditional-equivalence identity, post-compose only:
    # the funnel references both sides' local keys, which coexist on the merged
    # class and nowhere else.
    return apply_evolution(
        union,
        [
            ReplaceIdentityOp(
                vertices={
                    "Company": IdentityReplacement(
                        to=FunnelIdentityTarget(funnel=FUNNEL),
                        retire="keep",
                    )
                }
            )
        ],
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
