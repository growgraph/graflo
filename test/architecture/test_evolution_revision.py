"""Forward-only revision chains.

The guarantees under test: a chain replays onto its base reproducing every
recorded hash; a chain whose links or hashes disagree is rejected rather than
half-applied; and going back prefers replay from the base, falling back to
inverses only when it can do so exactly.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution.autogenerate import diff_manifests
from graflo.architecture.evolution.codec import op_from_dict
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.revision import (
    FileRevisionStore,
    Revision,
    RevisionChain,
    RevisionError,
    apply_revisions,
    build_revision,
    compute_revision_id,
    downgrade_to,
)

PARTY = {"name": "party", "properties": ["id", "name"], "identity": ["id"]}
WIDER = {"name": "party", "properties": ["id", "name", "email"], "identity": ["id"]}
ORDER = {"name": "order", "properties": ["oid"], "identity": ["oid"]}


def _manifest(vertices: list[dict]) -> GraphManifest:
    return GraphManifest.model_validate(
        {
            "schema": {
                "metadata": {"name": "revision-demo", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": []},
                },
            }
        }
    )


@pytest.fixture
def states() -> tuple[GraphManifest, GraphManifest, GraphManifest]:
    return _manifest([PARTY]), _manifest([WIDER]), _manifest([WIDER, ORDER])


@pytest.fixture
def chain(states) -> RevisionChain:
    base, first, second = states
    r1 = build_revision(base, diff_manifests(base, first)[0], label="add email")
    r2 = build_revision(
        first,
        diff_manifests(first, second)[0],
        down_revision=r1.revision,
        label="add order",
    )
    return RevisionChain(revisions=[r1, r2])


class TestBuild:
    def test_a_revision_records_the_transition_it_performed(self, states) -> None:
        base, first, _ = states

        revision = build_revision(base, diff_manifests(base, first)[0])

        assert revision.manifest_hash_before == manifest_hash(base)
        assert revision.manifest_hash_after == manifest_hash(first)
        assert revision.down_revision is None
        assert revision.reversible is True

    def test_the_id_is_content_derived_and_stable(self, states) -> None:
        """Regenerating the same change set must not mint a second identity."""
        base, first, _ = states
        ops = diff_manifests(base, first)[0]

        assert build_revision(base, ops).revision == build_revision(base, ops).revision
        assert compute_revision_id(ops, None) != compute_revision_id(ops, "parent")

    def test_an_empty_change_set_is_refused(self, states) -> None:
        base, _, _ = states
        assert diff_manifests(base, base)[0] == []

        with pytest.raises(RevisionError, match="at least one operation"):
            build_revision(base, [])

    def test_a_change_set_that_changes_nothing_is_refused(self, states) -> None:
        """A no-op revision would sit in the chain claiming a transition."""
        base, _, _ = states
        no_op = op_from_dict(
            {
                "op": "replace_identity",
                "vertices": {"party": {"to": {"mode": "natural", "identity": ["id"]}}},
            }
        )

        with pytest.raises(RevisionError, match="nothing to record"):
            build_revision(base, [no_op])

    def test_an_irreversible_op_is_flagged_on_the_revision(self, states) -> None:
        _base, _, _ = states
        merge = op_from_dict(
            {"op": "merge_vertices", "sources": ["party"], "into": "party2"}
        )
        manifest = _manifest(
            [PARTY, {"name": "party2", "properties": ["id"], "identity": ["id"]}]
        )

        revision = build_revision(manifest, [merge])

        assert revision.reversible is False

    def test_the_binary_compose_op_cannot_enter_a_revision(self, states) -> None:
        """A revision describes one manifest's transition; compose takes two."""
        from graflo.architecture.evolution.ops import ComposeManifestsOp

        base, _, _ = states

        with pytest.raises(ValueError):
            build_revision(base, [ComposeManifestsOp()])


class TestChainValidation:
    def test_a_broken_parent_link_is_rejected(self, chain) -> None:
        first, second = chain.revisions
        detached = second.model_copy(update={"down_revision": "somethingelse"})

        with pytest.raises(ValueError, match="must be linear"):
            RevisionChain(revisions=[first, detached])

    def test_a_hash_gap_between_revisions_is_rejected(self, chain) -> None:
        """A chain whose steps do not meet cannot describe one history."""
        first, second = chain.revisions
        shifted = second.model_copy(update={"manifest_hash_before": "00" * 32})

        with pytest.raises(ValueError, match="expects to start from hash"):
            RevisionChain(revisions=[first, shifted])

    def test_duplicate_ids_are_rejected(self, chain) -> None:
        first, _ = chain.revisions

        with pytest.raises(ValueError, match="duplicate revision ids"):
            RevisionChain(revisions=[first, first])

    def test_a_chain_must_start_at_a_root(self, chain) -> None:
        _, second = chain.revisions

        with pytest.raises(ValueError, match="not in this chain"):
            RevisionChain(revisions=[second])

    def test_head_and_path_to(self, chain) -> None:
        first, second = chain.revisions

        assert chain.head() is second
        assert chain.path_to(first.revision) == [first]
        assert chain.path_to(None) == [first, second]
        with pytest.raises(RevisionError, match="unknown revision"):
            chain.path_to("nope")


class TestReplay:
    def test_replaying_the_chain_reproduces_the_target(self, states, chain) -> None:
        base, _, second = states

        assert manifest_hash(apply_revisions(base, chain)) == manifest_hash(second)

    def test_replaying_part_way_reproduces_that_state(self, states, chain) -> None:
        base, first, _ = states

        replayed = apply_revisions(base, chain, upto=chain.revisions[0].revision)

        assert manifest_hash(replayed) == manifest_hash(first)

    def test_replaying_onto_the_wrong_base_fails_loudly(self, chain) -> None:
        """The point of recording hashes: drift is caught, not absorbed."""
        wrong_base = _manifest([ORDER])

        with pytest.raises(RevisionError, match="expects to start from hash"):
            apply_revisions(wrong_base, chain)

    def test_a_tampered_after_hash_is_caught(self, states, chain) -> None:
        base, _, _ = states
        first, second = chain.revisions
        tampered = RevisionChain(
            revisions=[
                first.model_copy(update={"manifest_hash_after": "11" * 32}),
                second.model_copy(update={"manifest_hash_before": "11" * 32}),
            ]
        )

        with pytest.raises(RevisionError, match="not the recorded"):
            apply_revisions(base, tampered)

    def test_verification_can_be_disabled_for_a_known_good_chain(
        self, states, chain
    ) -> None:
        base, _, second = states

        replayed = apply_revisions(base, chain, verify=False)

        assert manifest_hash(replayed) == manifest_hash(second)


class TestDowngrade:
    def test_replay_from_base_is_exact(self, states, chain) -> None:
        base, first, _ = states

        restored = downgrade_to(chain, chain.revisions[0].revision, base=base)

        assert manifest_hash(restored) == manifest_hash(first)

    def test_downgrading_to_the_base_state(self, states, chain) -> None:
        base, _, _ = states

        assert manifest_hash(downgrade_to(chain, None, base=base)) == manifest_hash(
            base
        )

    def test_inversion_works_when_nothing_was_lost(self, states, chain) -> None:
        base, _, second = states

        restored = downgrade_to(chain, None, current=second)

        assert manifest_hash(restored) == manifest_hash(base)

    def test_inversion_refuses_when_the_data_is_gone(self, states) -> None:
        """A removal cannot be undone from the post-state; say so, do not guess."""
        base, first, second = states
        r1 = build_revision(base, diff_manifests(base, second)[0])
        r2 = build_revision(
            second,
            diff_manifests(second, first)[0],
            down_revision=r1.revision,
            label="drop order",
        )
        chain = RevisionChain(revisions=[r1, r2])

        with pytest.raises(RevisionError, match="cannot be inverted"):
            downgrade_to(chain, r1.revision, current=first)

        # …but replaying from the base still works.
        assert manifest_hash(
            downgrade_to(chain, r1.revision, base=base)
        ) == manifest_hash(second)

    def test_downgrade_needs_a_base_or_a_current_manifest(self, chain) -> None:
        with pytest.raises(RevisionError, match="needs either the base"):
            downgrade_to(chain, None)


class TestFileStore:
    def test_a_chain_round_trips_through_disk(self, tmp_path, chain) -> None:
        store = FileRevisionStore(tmp_path / "revisions")

        store.save(chain)
        loaded = store.load()

        assert [r.revision for r in loaded.revisions] == [
            r.revision for r in chain.revisions
        ]
        assert loaded.revisions[0].ops[0] == chain.revisions[0].ops[0]

    def test_a_replayed_stored_chain_still_reproduces_the_target(
        self, tmp_path, states, chain
    ) -> None:
        base, _, second = states
        store = FileRevisionStore(tmp_path / "revisions")
        store.save(chain)

        replayed = apply_revisions(base, store.load())

        assert manifest_hash(replayed) == manifest_hash(second)

    def test_append_validates_the_link(self, tmp_path, states, chain) -> None:
        _base, _first, _second = states
        store = FileRevisionStore(tmp_path / "revisions")
        store.save(RevisionChain(revisions=[chain.revisions[0]]))

        store.append(chain.revisions[1])

        assert len(store.load().revisions) == 2

    def test_appending_a_detached_revision_is_rejected(
        self, tmp_path, states, chain
    ) -> None:
        _base, first, second = states
        store = FileRevisionStore(tmp_path / "revisions")
        store.save(RevisionChain(revisions=[chain.revisions[0]]))
        orphan = build_revision(first, diff_manifests(first, second)[0])  # no parent

        with pytest.raises(ValueError):
            store.append(orphan)

    def test_an_empty_store_loads_an_empty_chain(self, tmp_path) -> None:
        assert FileRevisionStore(tmp_path / "nothing").load().revisions == []

    def test_stored_files_are_named_in_order(self, tmp_path, chain) -> None:
        store = FileRevisionStore(tmp_path / "revisions")

        paths = store.save(chain)

        assert [p.name.split("_")[0] for p in paths] == ["0000", "0001"]
        assert "add_email" in paths[0].name

    def test_load_orders_by_parent_links_not_filenames(self, tmp_path, chain) -> None:
        """Filenames are a convenience; the links are the truth."""
        store = FileRevisionStore(tmp_path / "revisions")
        store.save(chain)
        root = tmp_path / "revisions"
        files = sorted(root.glob("*.yaml"))
        files[0].rename(root / "zzzz_first.yaml")
        files[1].rename(root / "aaaa_second.yaml")

        loaded = store.load()

        assert [r.revision for r in loaded.revisions] == [
            r.revision for r in chain.revisions
        ]


class TestSerialization:
    def test_a_revision_round_trips_through_dict(self, chain) -> None:
        from graflo.architecture.evolution.revision import (
            _revision_from_dict,
            _revision_to_dict,
        )

        original = chain.revisions[0]

        restored = _revision_from_dict(_revision_to_dict(original))

        assert restored == original

    def test_a_revision_holding_a_nested_discriminator_round_trips(
        self, tmp_path, states
    ) -> None:
        """Guards the `IdentityTarget.mode` default-drop the codec compensates for."""
        base, _, _ = states
        target = _manifest(
            [{"name": "party", "properties": ["id", "name"], "identity": ["name"]}]
        )
        revision = build_revision(base, diff_manifests(base, target)[0])
        store = FileRevisionStore(tmp_path / "revisions")
        store.save(RevisionChain(revisions=[revision]))

        loaded = store.load().revisions[0]

        assert loaded.ops[0].op == "replace_identity"
        assert manifest_hash(
            apply_revisions(base, RevisionChain(revisions=[loaded]))
        ) == manifest_hash(target)


def test_revision_dataclass_defaults_are_explicit() -> None:
    """A revision must not silently claim reversibility it has not checked."""
    fields = Revision.model_fields
    assert fields["reversible"].default is True
    assert fields["down_revision"].default is None
