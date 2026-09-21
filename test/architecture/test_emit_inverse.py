"""``emit_inverse``: an edge step that also writes its declared inverse.

The inverse is mirrored at assembly, after the relation is resolved, so it does
not matter how a step names its relation -- fixed, read from a field, mapped, or
taken from a document key. Only a *materialized* inverse is written: the pair
must be declared and the inverse edge must be declared too.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from graflo.architecture.contract import GraphManifest
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.pipeline.runtime.resource import ResourceRuntime

FORWARD = {"source": "person", "target": "institution", "relation": "employed_by"}
INVERSE = {"source": "institution", "target": "person", "relation": "employs"}
PAIR = {"relation": "employed_by", "inverse": "employs"}
ROW = {"pid": "p1", "iid": "i1"}

VERTICES = [{"vertex": "person"}, {"vertex": "institution"}]


def _manifest(
    pipeline: list[dict[str, Any]],
    *,
    edges: list[dict[str, Any]] | None = None,
    inverses: list[dict[str, str]] | None = None,
    symmetric: list[str] | None = None,
    resource: dict[str, Any] | None = None,
) -> GraphManifest:
    manifest = GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "mirror", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "identity": ["pid"],
                                "properties": ["pid"],
                            },
                            {
                                "name": "institution",
                                "identity": ["iid"],
                                "properties": ["iid"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": copy.deepcopy(
                            edges if edges is not None else [FORWARD, INVERSE]
                        ),
                        "inverses": inverses if inverses is not None else [PAIR],
                        "symmetric": symmetric or [],
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {"name": "rows", "pipeline": pipeline, **(resource or {})}
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _cast(manifest: GraphManifest, row: dict[str, Any]) -> dict[Any, list]:
    entities = manifest.require_ingestion_model().fetch_resource("rows")(row)
    return {key: docs for key, docs in entities.items() if isinstance(key, tuple)}


def _endpoints(edocs: list) -> list[tuple[str, str]]:
    return [
        (next(iter(source.values())), next(iter(target.values())))
        for source, target, _weight in edocs
    ]


class TestEveryStepShapeIsMirrored:
    def test_a_fixed_relation(self) -> None:
        step = {"edge": {**_flat(FORWARD), "emit_inverse": True}}
        edges = _cast(_manifest([*VERTICES, step]), ROW)

        assert _endpoints(edges[("person", "institution", "employed_by")]) == [
            ("p1", "i1")
        ]
        assert _endpoints(edges[("institution", "person", "employs")]) == [("i1", "p1")]

    def test_a_relation_read_from_a_field_with_fixed_endpoints(self) -> None:
        """Assembly reads the relation here, where no ``relation_map`` applies."""
        step = {
            "edge": {
                "from": "person",
                "to": "institution",
                "relation_field": "rel",
                "emit_inverse": True,
            }
        }
        edges = _cast(_manifest([*VERTICES, step]), {**ROW, "rel": "employed_by"})

        assert _endpoints(edges[("institution", "person", "employs")]) == [("i1", "p1")]

    def test_a_mapped_relation_with_routed_endpoints(self) -> None:
        pipeline = [
            {"vertex_router": {"type_field": "source_type", "role": "source"}},
            {"vertex_router": {"type_field": "target_type", "role": "target"}},
            {
                "edge": {
                    "source_role": "source",
                    "target_role": "target",
                    "relation_field": "rel",
                    "relation_map": {"EMPLOYED_BY": "employed_by"},
                    "emit_inverse": True,
                }
            },
        ]
        row = {
            "source_type": "person",
            "target_type": "institution",
            "rel": "EMPLOYED_BY",
            **ROW,
        }
        edges = _cast(_manifest(pipeline), row)

        assert ("person", "institution", "employed_by") in edges
        assert _endpoints(edges[("institution", "person", "employs")]) == [("i1", "p1")]

    def test_a_link(self) -> None:
        step = {"edge": {"links": [{**_flat(FORWARD), "emit_inverse": True}]}}
        edges = _cast(_manifest([*VERTICES, step]), ROW)

        assert _endpoints(edges[("institution", "person", "employs")]) == [("i1", "p1")]

    def test_a_relation_taken_from_the_document_key(self) -> None:
        manifest = GraphManifest.from_dict(
            {
                "schema": {
                    "metadata": {"name": "keys", "version": "1.0.0"},
                    "graph": {
                        "vertex_config": {
                            "vertices": [
                                {
                                    "name": "package",
                                    "identity": ["name"],
                                    "properties": ["name"],
                                }
                            ]
                        },
                        "edge_config": {
                            "edges": [
                                {"source": "package", "target": "package"},
                            ],
                            "inverses": [
                                {"relation": "depends", "inverse": "required_by"}
                            ],
                        },
                    },
                },
                "ingestion_model": {
                    "resources": [
                        {
                            "name": "rows",
                            "pipeline": [
                                {"vertex": "package"},
                                {
                                    "key": "dependencies",
                                    "pipeline": [
                                        {
                                            "any_key": True,
                                            "pipeline": [{"vertex": "package"}],
                                        }
                                    ],
                                },
                                {
                                    "source": "package",
                                    "target": "package",
                                    "relation_from_key": True,
                                    "emit_inverse": True,
                                },
                            ],
                        }
                    ]
                },
            }
        )
        manifest.finish_init()
        edges = _cast(
            manifest, {"name": "a", "dependencies": {"depends": [{"name": "b"}]}}
        )

        assert _endpoints(edges[("package", "package", "depends")]) == [("a", "b")]
        assert _endpoints(edges[("package", "package", "required_by")]) == [("b", "a")]


def _flat(edge: dict[str, str]) -> dict[str, str]:
    return {"from": edge["source"], "to": edge["target"], "relation": edge["relation"]}


class TestWhatIsMirrored:
    def test_the_mirror_carries_what_the_forward_edge_carries(self) -> None:
        forward = {**FORWARD, "properties": ["since"]}
        inverse = {**INVERSE, "properties": ["since"]}
        step = {
            "edge": {**_flat(FORWARD), "properties": ["since"], "emit_inverse": True}
        }
        edges = _cast(
            _manifest([*VERTICES, step], edges=[forward, inverse]),
            {**ROW, "since": 2020},
        )

        (_s, _t, forward_weight) = edges[("person", "institution", "employed_by")][0]
        (_s, _t, mirror_weight) = edges[("institution", "person", "employs")][0]
        assert forward_weight == mirror_weight == {"since": 2020}
        assert forward_weight is not mirror_weight

    def test_a_relation_with_no_materialized_inverse_is_written_once(self) -> None:
        """Data-driven steps are checked per document: an unpaired relation is not mirrored."""
        advises = {"source": "person", "target": "institution", "relation": "advises"}
        step = {
            "edge": {
                "from": "person",
                "to": "institution",
                "relation_field": "rel",
                "emit_inverse": True,
            }
        }
        edges = _cast(
            _manifest([*VERTICES, step], edges=[FORWARD, INVERSE, advises]),
            {**ROW, "rel": "advises"},
        )

        assert ("person", "institution", "advises") in edges
        assert [key for key in edges if key[0] == "institution"] == []

    def test_without_the_flag_nothing_is_mirrored(self) -> None:
        manifest = _manifest(
            [*VERTICES, {"edge": _flat(FORWARD)}], resource={"infer_edges": False}
        )
        assert ("institution", "person", "employs") not in _cast(manifest, ROW)

    def test_inference_does_not_write_the_mirror_a_second_time(self) -> None:
        step = {"edge": {**_flat(FORWARD), "emit_inverse": True}}
        manifest = _manifest([*VERTICES, step])
        assert manifest.require_ingestion_model().resources[0].infer_edges is True

        edges = _cast(manifest, ROW)
        assert len(edges[("institution", "person", "employs")]) == 1

    def test_a_flagged_step_keeps_inference_off_the_reversed_pair(self) -> None:
        step = {"edge": {**_flat(FORWARD), "emit_inverse": True}}
        assert ResourceRuntime.edge_ids_from_pipeline([step]) == {
            ("person", "institution", None),
            ("institution", "person", None),
        }


class TestRefusals:
    def test_a_relation_with_no_declared_inverse(self) -> None:
        step = {"edge": {**_flat(FORWARD), "emit_inverse": True}}
        with pytest.raises(ValueError, match="has no declared inverse"):
            _manifest([*VERTICES, step], edges=[FORWARD], inverses=[])

    def test_an_inverse_edge_that_is_not_declared(self) -> None:
        """A pair that is only declared stores nothing, so there is nothing to write into."""
        step = {"edge": {**_flat(FORWARD), "emit_inverse": True}}
        with pytest.raises(ValueError, match="is not declared") as refusal:
            _manifest([*VERTICES, step], edges=[FORWARD])
        assert "add_inverse_edges" in str(refusal.value)
        assert "set_native_inverses" in str(refusal.value)

    def test_a_symmetric_relation(self) -> None:
        knows = {
            "source": "person",
            "target": "person",
            "relation": "knows",
            "directed": False,
        }
        step = {
            "edge": {
                "from": "person",
                "to": "person",
                "relation": "knows",
                "emit_inverse": True,
            }
        }
        with pytest.raises(ValueError, match="is symmetric"):
            _manifest(
                [{"vertex": "person"}, step],
                edges=[knows],
                inverses=[],
                symmetric=["knows"],
            )

    def test_the_flag_on_a_step_with_links(self) -> None:
        step = {"edge": {"links": [_flat(FORWARD)], "emit_inverse": True}}
        with pytest.raises(ValueError, match="set it on each link"):
            _manifest([*VERTICES, step])

    def test_a_relation_less_template_edge_is_enough_to_write_into(self) -> None:
        template = {"source": "institution", "target": "person"}
        step = {"edge": {**_flat(FORWARD), "emit_inverse": True}}
        edges = _cast(_manifest([*VERTICES, step], edges=[FORWARD, template]), ROW)
        assert ("institution", "person", "employs") in edges


def test_a_step_that_does_not_set_the_flag_hashes_as_before() -> None:
    """The flag defaults off, so manifests that never heard of it keep their hash."""
    plain = _manifest([*VERTICES, {"edge": _flat(FORWARD)}])
    explicit = _manifest(
        [*VERTICES, {"edge": {**_flat(FORWARD), "emit_inverse": False}}]
    )
    assert manifest_hash(plain) == manifest_hash(explicit)
