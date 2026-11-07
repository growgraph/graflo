"""Where every merged attribute comes from, and how each merged class is keyed.

The naming pass records which declaration wrote each attribute rename; the
preview turns that, the composite relabel and each group's effective
identity into edges and an identity block. The cases run on the derived
identity of ``examples/20-manifest-union`` (``Asset`` and ``Device`` into
``Machine``, keyed by a derived ``match_key`` then a tagged ``local_key``)
and on the ``merge_union`` fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.canonical import CanonicalMap, resolve_clusters
from graflo.architecture.evolution.ops import (
    DerivationSpec,
    DerivedBranch,
    MergeManifestsOp,
    VertexEquivalence,
)
from graflo.architecture.evolution.preview import (
    MergePreview,
    PreviewIdentity,
    preview_merge,
)
from test.architecture.test_naming_graph import (
    _devices_manifest,
    _engineer_op,
    _register_manifest,
)

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "20-manifest-union"
FIXTURES = Path(__file__).resolve().parents[1] / "data" / "merge_union"


def _manifest(path: Path) -> GraphManifest:
    return GraphManifest.from_config(FileHandle.load(path))


@pytest.fixture(scope="module")
def maintenance() -> GraphManifest:
    return _manifest(EXAMPLE / "manifest_maintenance.yaml")


@pytest.fixture(scope="module")
def sensors() -> GraphManifest:
    return _manifest(EXAMPLE / "manifest_sensors.yaml")


@pytest.fixture(scope="module")
def triage_op() -> MergeManifestsOp:
    return MergeManifestsOp.model_validate(FileHandle.load(EXAMPLE / "merge.yaml"))


@pytest.fixture(scope="module")
def preview(maintenance, sensors, triage_op) -> MergePreview:
    return preview_merge(maintenance, sensors, triage_op)


def _identity(preview: MergePreview, into: str) -> PreviewIdentity:
    cluster = next(c for c in preview.clusters if c.into == into)
    assert cluster.identity is not None
    return cluster.identity


def _edge(preview: MergePreview, edge_id: str):
    return next((e for e in preview.edges if e.id == edge_id), None)


# ── the naming pass ─────────────────────────────────────────────────────────


class TestPropertyLayers:
    def test_each_rename_records_the_declaration_that_wrote_it(
        self, maintenance, sensors, triage_op
    ) -> None:
        resolution = resolve_clusters(triage_op, left=maintenance, right=sensors)

        assert resolution.property_layers == {
            ("right", "Device", "serial"): (("vocabulary", "serial_number"),),
            ("left", "Asset", "asset_id"): (("demotion", "maintenance__asset_id"),),
            ("right", "Device", "device_id"): (("demotion", "sensors__device_id"),),
        }

    def test_a_rename_after_the_vocabulary_is_recorded_after_it(self) -> None:
        resolution = resolve_clusters(
            _override_op(),
            left=_manifest(FIXTURES / "manifest_a.yaml"),
            right=_manifest(FIXTURES / "manifest_b.yaml"),
        )

        assert resolution.property_layers[("left", "Agreement", "agreement_id")] == (
            ("vocabulary", "contract_id"),
            ("rename", "deal_id"),
        )


def _override_op() -> MergeManifestsOp:
    """``Agreement`` belongs to no group: the vocabulary renames one of its attributes, then ``renames`` overrides it."""
    cmap = CanonicalMap.model_validate(FileHandle.load(FIXTURES / "canonical_map.yaml"))
    cmap = cmap.model_copy(
        update={
            "properties": {
                **cmap.properties,
                "Agreement": {"agreement_id": "contract_id"},
            }
        }
    )
    return MergeManifestsOp.model_validate(
        {
            "vertex_equivalences": [
                {
                    "left": ["Firm", "Shop"],
                    "right": ["Org", "Branch"],
                    "identity": ["company_id", "shop_id", "org_id", "branch_id"],
                }
            ],
            "canonical_maps": {"left": cmap.to_dict()},
            "renames": {
                "left": {"properties": {"Agreement": {"agreement_id": "deal_id"}}}
            },
            "origins": {"left": "source_a", "right": "source_b"},
        }
    )


# ── attribute lineage ───────────────────────────────────────────────────────


class TestAttributeFlow:
    @pytest.mark.parametrize(
        ("edge_id", "layer"),
        [
            (
                "fusion:left:Asset.serial_number->merged:Machine.serial_number",
                "fusion",
            ),
            (
                "property_map:right:Device.serial->merged:Machine.serial_number",
                "vocabulary",
            ),
            (
                "demotion:left:Asset.asset_id->merged:Machine.maintenance__asset_id",
                "demotion",
            ),
            (
                "demotion:right:Device.device_id->merged:Machine.sensors__device_id",
                "demotion",
            ),
        ],
    )
    def test_every_member_attribute_reaches_its_merged_row(
        self, preview, edge_id, layer
    ) -> None:
        edge = _edge(preview, edge_id)
        assert edge is not None
        assert edge.layer == layer
        assert edge.effective

    def test_a_demotion_is_labelled_by_its_key_space(self, preview) -> None:
        edge = _edge(
            preview,
            "demotion:left:Asset.asset_id->merged:Machine.maintenance__asset_id",
        )
        assert edge is not None
        assert edge.label == "lookup · maintenance"

    @pytest.mark.parametrize("which", ["triage", "engineer"])
    def test_every_member_attribute_has_exactly_one_effective_edge(
        self, preview, which
    ) -> None:
        if which == "engineer":
            preview = preview_merge(
                _register_manifest(), _devices_manifest(), _engineer_op()
            )
        members = {
            f"{side}:{m}"
            for c in preview.clusters
            if c.kind == "vertex"
            for side, names in (("left", c.left), ("right", c.right))
            for m in names
        }
        member_rows = [
            n.id for n in preview.nodes if n.kind == "attribute" and n.owner in members
        ]
        assert member_rows
        lineage = {"fusion", "demotion", "property_map", "property_equivalence"}
        for row in member_rows:
            effective = [
                e
                for e in preview.edges
                if e.source == row and e.kind in lineage and e.effective
            ]
            assert len(effective) == 1, (row, effective)

    def test_an_overridden_vocabulary_entry_is_kept_but_not_effective(self) -> None:
        preview = preview_merge(
            _manifest(FIXTURES / "manifest_a.yaml"),
            _manifest(FIXTURES / "manifest_b.yaml"),
            _override_op(),
            attempt=False,
        )

        source = "left:Agreement.agreement_id"
        vocabulary = _edge(
            preview, f"property_map:{source}->canonical:Contract.contract_id"
        )
        rename = _edge(preview, f"property_rename:{source}->canonical:Contract.deal_id")
        assert vocabulary is not None and not vocabulary.effective
        assert rename is not None and rename.effective and rename.layer == "rename"


# ── identity blocks ─────────────────────────────────────────────────────────


class TestIdentityBlock:
    def test_branches_are_numbered_in_the_order_a_record_tries_them(
        self, preview
    ) -> None:
        identity = _identity(preview, "Machine")

        assert identity.mode == "funnel"
        assert identity.digest_field == "id"
        assert [(b.position, b.id, b.kind) for b in identity.branches] == [
            (1, "match_key", "derived"),
            (2, "local_key", "local_key"),
        ]

    def test_a_derived_branch_names_each_resource_and_what_it_reads(
        self, preview
    ) -> None:
        match_key = _identity(preview, "Machine").branches[0]

        assert [
            (s.resource, s.side, s.input, s.foo, s.label) for s in match_key.sources
        ] == [
            (
                "assets",
                "left",
                ["serial_number"],
                "normalized_key",
                "normalized_key(serial_number)",
            ),
            (
                "devices",
                "right",
                ["serial"],
                "normalized_key",
                "normalized_key(serial)",
            ),
        ]

    def test_a_local_key_names_each_field_and_tag(self, preview) -> None:
        local_key = _identity(preview, "Machine").branches[1]

        assert [(s.resource, s.field, s.tag, s.auto) for s in local_key.sources] == [
            ("assets", "asset_id", "maintenance", False),
            ("devices", "device_id", "sensors", False),
        ]

    def test_demoted_member_keys_are_listed_with_their_merged_names(
        self, preview
    ) -> None:
        demoted = _identity(preview, "Machine").demoted

        assert [(d.side, d.member, d.merged_fields, d.key_space) for d in demoted] == [
            ("left", "Asset", ["maintenance__asset_id"], "maintenance"),
            ("right", "Device", ["sensors__device_id"], "sensors"),
        ]

    def test_raw_fields_point_at_the_branch_they_feed(self, preview) -> None:
        assert _edge(
            preview,
            "derivation:resource:assets.serial_number->merged:Machine.match_key",
        )
        assert _edge(
            preview, "local_key:resource:devices.device_id->merged:Machine.local_key"
        )
        resource = preview.node("resource:assets")
        assert resource is not None and resource.kind == "resource"
        assert resource.side == "left"
        field = preview.node("resource:assets.serial_number")
        assert field is not None and field.owner == "resource:assets"

    def test_the_digest_keys_the_merged_class(self, preview) -> None:
        digest = preview.node("merged:Machine.id")
        assert digest is not None and digest.identity

    def test_a_resource_no_side_has_is_drawn_absent(self) -> None:
        preview = _derived_preview({"nope": DerivationSpec(input=["shared_raw"])})

        resource = preview.node("resource:nope")
        assert resource is not None and not resource.exists
        (source,) = _identity(preview, "Company").branches[0].sources
        assert source.side is None and not source.exists

    def test_member_keyed_entries_reading_one_field_stay_apart(self) -> None:
        preview = _derived_preview(
            {
                "r_a": {
                    "Firm": DerivationSpec(input=["shared_raw"], foo="lower"),
                    "Shop": DerivationSpec(input=["shared_raw"]),
                }
            }
        )

        edges = [
            e
            for e in preview.edges
            if e.kind == "derivation" and e.source == "resource:r_a.shared_raw"
        ]
        assert sorted(e.id.rpartition("#")[2] for e in edges) == ["Firm", "Shop"]
        assert {e.label for e in edges} == {
            "lower(shared_raw)",
            "normalized_key(shared_raw)",
        }

    def test_an_automatic_local_key_is_marked(self) -> None:
        """``Bench`` joins only through the vocabulary, so merge keys it on its own key."""
        resolution = resolve_clusters(
            _engineer_op(), left=_register_manifest(), right=_devices_manifest()
        )
        assert resolution.auto_key_sources == {"Machine": {("register", "Bench")}}

        preview = preview_merge(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )
        local_key = next(
            b for b in _identity(preview, "Machine").branches if b.kind == "local_key"
        )
        (auto,) = [s for s in local_key.sources if s.auto]
        assert (auto.resource, auto.member, auto.tag) == (
            "register",
            "Bench",
            "maintenance:Bench",
        )
        assert auto.label == "tag maintenance:Bench · auto"
        assert _edge(
            preview,
            "local_key:resource:register.asset_id->merged:Machine.local_key#Bench",
        )

    def test_a_property_equivalence_is_tagged_with_its_layer(self) -> None:
        preview = preview_merge(
            _register_manifest(), _devices_manifest(), _engineer_op()
        )

        edge = _edge(
            preview,
            "property_equivalence:left:Press.press_serial->merged:Machine.serial",
        )
        assert edge is not None
        assert edge.layer == "equivalence" and edge.effective


def _derived_preview(sources: dict) -> MergePreview:
    op = MergeManifestsOp(
        vertex_equivalences=[
            VertexEquivalence(
                left=["Firm", "Shop"],
                right=["Org", "Branch"],
                identity=[DerivedBranch(name="match_key", sources=sources)],
            )
        ],
        canonical_maps={
            "left": CanonicalMap.model_validate(
                FileHandle.load(FIXTURES / "canonical_map.yaml")
            )
        },
    )
    return preview_merge(
        _manifest(FIXTURES / "manifest_a.yaml"),
        _manifest(FIXTURES / "manifest_b.yaml"),
        op,
        attempt=False,
    )


# ── the preview as a whole ──────────────────────────────────────────────────


def test_every_edge_endpoint_is_a_node(preview) -> None:
    ids = {n.id for n in preview.nodes}
    for edge in preview.edges:
        assert edge.source in ids and edge.target in ids, edge.id


def test_every_row_owner_is_a_node(preview) -> None:
    ids = {n.id for n in preview.nodes}
    assert all(n.owner in ids for n in preview.nodes if n.owner is not None)


def test_the_preview_round_trips_through_json(preview) -> None:
    payload = json.loads(json.dumps(preview.to_dict()))
    assert MergePreview.model_validate(payload) == preview


def test_a_merge_that_goes_through_reports_its_demotions(preview) -> None:
    assert [f.kind for f in preview.findings if f.severity == "note"].count(
        "lookup_demotion"
    ) == 2
