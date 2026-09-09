"""
Align identity across a union where one side is a routed source, without
splitting the router.

Source A is a single view: one heterogeneous stream discriminated by ``kind``,
routed by ONE ``vertex_router`` nested under a ``descend``. The equivalence
collapses two of its members (``Company`` — ``Firm`` before the canonical map —
and ``Shop``) onto ``Company`` while ``Person`` keeps flowing through the same
router — splitting the resource would mean scanning the view twice and
duplicating a discriminator it already carries.

**The member is the unit of derivation.** Every record that becomes ``Company``
was produced *as one member* of the cluster by *one resource*: here the view
produces ``Company`` for ``kind: firm`` rows and ``Shop`` for ``kind: shop``
rows. All kinds carry the shared business key in ONE column, ``secondary_key``,
each member under its own marker (``abc_`` for firms, ``def_`` for shops). So
which member a document *is* must decide the derivation — which is what keying
``sources["r_view"]`` by member says. The lowering asks the pre-merge side how
the resource produces each member and guards the step with ``when`` on the
router's discriminator; nothing here names ``kind`` or ``firm``.

``match_key`` is derived by ``affix_gated_key``: the marker on the value is the
admission test, so a value carrying it is stripped and accepted as canonical
key material and an unmarked one yields ``None``. That ``None`` is a
fall-through, not a drop — the record still ingests, on its side-local key,
outside the cross-source cluster. A shop row carrying the *firm* marker falls
through the same way: its member's derivation requires ``def_``.

    cd examples/21-router-union-alignment
    uv run python build_union.py            # → artifacts/manifest_union.yaml
    uv run python inspect_fusion.py         # which records fuse, and to what
    uv run python build_union.py --root-demo    # derive at the root → conflict
"""

from __future__ import annotations

from pathlib import Path

import click
from suthing import FileHandle

from graflo import GraphManifest
from graflo.architecture.evolution import (
    AlignmentAttribute,
    AlignmentConflictError,
    CanonicalMap,
    ComposeManifestsOp,
    DerivationSpec,
    IdentityAlignment,
    LocalKeySource,
    LocalKeySpec,
    SharedDerivation,
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_and_complete_canonical_map,
)

EXAMPLE_DIR = Path(__file__).resolve().parent

# Derivation inputs are RAW view columns. Every kind carries `secondary_key`,
# so column presence cannot select a member; the member key does.
ALIGNMENT = IdentityAlignment(
    vertex="Company",
    attributes=[
        AlignmentAttribute(
            into="match_key",
            sources={
                # Keyed by member: the classes the equivalence names on this
                # side (`Company` is the left member because the canonical map
                # renamed `Firm` before the compose). One call shared by both,
                # only the marker differs — the same one-field call runs on
                # every side, so no normal form can drift.
                "r_view": SharedDerivation(
                    spec=DerivationSpec(input=["secondary_key"], foo="affix_gated_key"),
                    members={"Company": {"prefix": "abc_"}, "Shop": {"prefix": "def_"}},
                ),
                # B's resources each produce one member — no key needed.
                "r_b": DerivationSpec(
                    input=["shared_raw"],
                    foo="affix_gated_key",
                    params={"prefix": "abc_"},
                ),
                "r_branch": DerivationSpec(
                    input=["shared_raw"],
                    foo="affix_gated_key",
                    params={"prefix": "def_"},
                ),
            },
        )
    ],
    # One resource, two side-local namespaces: the member a document is
    # decides its tag, and the guard comes from the router — not from here.
    local_key=LocalKeySpec(
        sources={
            "r_view": {
                "Company": LocalKeySource(field="firm_id", tag="firm"),
                "Shop": LocalKeySource(field="shop_id", tag="shop"),
            },
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


def build_union(*, root_demo: bool = False) -> GraphManifest:
    canonical_map = CanonicalMap.model_validate(
        FileHandle.load(EXAMPLE_DIR / "canonical_map.yaml")
    )
    left = apply_evolution(
        load_manifest(EXAMPLE_DIR / "manifest_a.yaml"),
        canonical_map_to_ops(canonical_map),
        bump_version=False,
    )
    right = load_manifest(EXAMPLE_DIR / "manifest_b.yaml")

    alignment = ALIGNMENT
    if root_demo:
        # The router is nested under `descend`; the root level produces nothing.
        alignment = ALIGNMENT.model_copy(update={"at": {"r_view": []}})

    op = ComposeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Company", "Shop"], right=["Org", "Branch"], into="Company"
            ),
        ],
        allow_merges=True,
        identity_alignments=[alignment],
    )
    validate_and_complete_canonical_map(
        op, left=left, right=right, canonical_maps=[("left", canonical_map)]
    )
    return compose_manifests(left, right, op, canonical_maps=[("left", canonical_map)])


@click.command()
@click.option("--root-demo", is_flag=True, help="Derive at the root level → conflict.")
def main(root_demo: bool) -> None:
    try:
        union = build_union(root_demo=root_demo)
    except AlignmentConflictError as exc:
        click.echo(f"AlignmentConflictError: {exc}")
        return
    out = EXAMPLE_DIR / "artifacts" / "manifest_union.yaml"
    out.parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(union.to_dict(), out)
    click.echo(f"Union manifest → {out}")


if __name__ == "__main__":
    main()
