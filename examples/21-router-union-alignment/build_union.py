"""
Align identity across a union where one side is a routed source, without
splitting the router.

Source A is a single view: one heterogeneous stream discriminated by ``kind``,
routed by ONE ``vertex_router`` nested under a ``descend``. The equivalence
collapses two of its branches (``Firm``, ``Shop``) onto ``Company`` while
``Person`` keeps flowing through the same router — splitting the resource would
mean scanning the view twice and duplicating a discriminator it already carries.

Two things make that work, and neither is optional here:

1. **The derivations land at the router's level.** ``alignment_to_ops`` resolves
   the level that produces ``Company`` and targets it. Appended at the root they
   would be invisible: an actor reads its transform buffer at its own
   ``LocationIndex`` with no ancestor fallback, and a ``descend`` subtree runs
   before its own level's transforms.
2. **Each collapsing branch derives its own way.** ``firm`` rows carry
   ``firm_ref``, ``shop`` rows carry ``shop_ref``. One ``DerivationSpec`` per
   resource cannot say that, so the resource supplies a list; the lowering gives
   each branch a scratch field and coalesces them into one writer.

``match_key`` is derived by ``affix_gated_key``: the ``ABC-`` marker on the
value is the admission test, so a value carrying it is stripped and accepted as
canonical key material and an unmarked one yields ``None``. That ``None`` is a
fall-through, not a drop — the record still ingests, on its side-local key,
outside the cross-source cluster. Every side uses the same one-field call, so
there is no normalization to drift.

Branch selection needs no extra gate: a union view leaves the other branch's
column empty, and an empty value carries no marker. ``local_key`` does gate —
both branches would otherwise be namespaced the same — so its sources read the
router's own discriminator.

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
    VertexEquivalence,
    apply_evolution,
    canonical_map_to_ops,
    compose_manifests,
    validate_and_complete_canonical_map,
)

EXAMPLE_DIR = Path(__file__).resolve().parent

# Derivation inputs are RAW view columns. `firm_ref` and `shop_ref` are never
# both populated, so at most one spec yields a value per document and the
# lowering's coalesce picks it — no discriminator gate needed.
ALIGNMENT = IdentityAlignment(
    vertex="Company",
    attributes=[
        AlignmentAttribute(
            into="match_key",
            sources={
                # Each branch of the view carries the shared business key in
                # its own column; the other is empty, which is what selects.
                # The `ABC-` marker decides participation: carry it and you are
                # stripped into the cluster, omit it and you fall through.
                "r_view": [
                    DerivationSpec(
                        input=["firm_ref"],
                        foo="affix_gated_key",
                        params={"prefix": "ABC-"},
                    ),
                    DerivationSpec(
                        input=["shop_ref"],
                        foo="affix_gated_key",
                        params={"prefix": "ABC-"},
                    ),
                ],
                # Literally the same call on the other side: one code path, so
                # the two normal forms cannot drift apart.
                "r_b": DerivationSpec(
                    input=["shared_raw"],
                    foo="affix_gated_key",
                    params={"prefix": "ABC-"},
                ),
                "r_branch": DerivationSpec(
                    input=["shared_raw"],
                    foo="affix_gated_key",
                    params={"prefix": "ABC-"},
                ),
            },
        )
    ],
    # One resource, two side-local namespaces: the branch a document came from
    # decides its tag, so the gate is the router's own discriminator.
    local_key=LocalKeySpec(
        sources={
            "r_view": [
                LocalKeySource(
                    field="firm_id", tag="firm", gate="kind", gate_prefix="firm"
                ),
                LocalKeySource(
                    field="shop_id", tag="shop", gate="kind", gate_prefix="shop"
                ),
            ],
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
        vertices=[
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
