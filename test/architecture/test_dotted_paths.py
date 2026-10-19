"""Dot paths in a vertex step's ``from`` and an edge step's ``relation_field``."""

from __future__ import annotations

from typing import Any

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.util.paths import get_at_path, lookup_field

ISSUE = {
    "name": "Issue",
    "properties": ["key", "status", "summary"],
    "identity": ["key"],
}
DUPLICATES = {"source": "Issue", "target": "Issue", "relation": "duplicates"}
BLOCKS = {"source": "Issue", "target": "Issue", "relation": "blocks"}

#: An issue with one outward link of each kind and one inward link.
DOC: dict[str, Any] = {
    "key": "A",
    "fields": {"summary": "Pump stops", "status": {"name": "Open"}},
    "links": [
        {"type": {"outward": "duplicates"}, "outwardIssue": {"key": "B"}},
        {"type": {"outward": "relates to"}, "outwardIssue": {"key": "C"}},
        {"type": {"outward": "blocks"}, "inwardIssue": {"key": "D"}},
    ],
}

#: The link's target, read through the link entry so the entry is observed there.
LINK_TARGET = {
    "descend": {
        "key": "links",
        "apply": [
            {
                "vertex": "Issue",
                "from": {"key": "outwardIssue.key"},
                "lookup_only": True,
                "extraction_scope": "mapped_only",
            }
        ],
    }
}


def _cast(
    pipeline: list[dict],
    doc: dict[str, Any],
    *,
    edges: list[dict] | None = None,
    vertices: list[dict] | None = None,
    strict: bool = False,
) -> dict:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "issues", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {"vertices": vertices or [ISSUE]},
                    "edge_config": {"edges": edges or []},
                },
            },
            "ingestion_model": {"resources": [{"name": "r", "pipeline": pipeline}]},
        }
    )
    manifest.finish_init(strict_references=strict)
    resource = manifest.require_ingestion_model().fetch_resource("r")
    return dict(resource.cast_document(doc).entities)


def _link_step(**extra: Any) -> dict:
    return {
        "edge": {
            "from": "Issue",
            "to": "Issue",
            "relation_field": "type.outward",
            "exclude_source": "links",
            "match_target": "links",
            **extra,
        }
    }


def _links(entities: dict) -> dict[str | None, list[tuple[str, str]]]:
    return {
        key[2]: [(s["key"], t["key"]) for s, t, _ in value]
        for key, value in entities.items()
        if isinstance(key, tuple)
    }


class TestPathReader:
    def test_a_path_reads_nested_objects_and_list_items(self) -> None:
        assert get_at_path(DOC, "fields.status.name") == "Open"
        assert get_at_path(DOC, "links.0.outwardIssue.key") == "B"

    @pytest.mark.parametrize(
        "path", ["fields.priority.name", "links.9.type", "key.more", "links.x"]
    )
    def test_a_missing_segment_is_none(self, path: str) -> None:
        assert get_at_path(DOC, path) is None
        assert lookup_field(DOC, path) == (False, None)

    def test_an_exact_key_wins_over_the_path_it_spells(self) -> None:
        doc = {"a.b": "flat", "a": {"b": "nested"}}
        assert lookup_field(doc, "a.b") == (True, "flat")
        assert lookup_field({"a": {"b": "nested"}}, "a.b") == (True, "nested")

    def test_a_present_key_holding_none_is_found(self) -> None:
        assert lookup_field({"a": None}, "a") == (True, None)


class TestVertexFrom:
    def test_a_dotted_source_reads_a_nested_value(self) -> None:
        entities = _cast(
            [
                {
                    "vertex": "Issue",
                    "from": {
                        "key": "key",
                        "status": "fields.status.name",
                        "summary": "fields.summary",
                    },
                }
            ],
            DOC,
        )
        assert entities["Issue"] == [
            {"key": "A", "status": "Open", "summary": "Pump stops"}
        ]

    def test_a_missing_segment_yields_none(self) -> None:
        entities = _cast(
            [
                {
                    "vertex": "Issue",
                    "from": {"key": "key", "status": "fields.state.name"},
                }
            ],
            DOC,
        )
        assert entities["Issue"] == [{"key": "A", "status": None}]

    def test_a_top_level_key_with_a_dot_is_read_as_that_key(self) -> None:
        doc = {"key": "A", "fields.summary": "flat", "fields": {"summary": "nested"}}
        entities = _cast(
            [{"vertex": "Issue", "from": {"key": "key", "summary": "fields.summary"}}],
            doc,
        )
        assert entities["Issue"] == [{"key": "A", "summary": "flat"}]


class TestRelationField:
    def test_a_dotted_relation_field_names_each_edge(self) -> None:
        entities = _cast(
            [{"vertex": "Issue", "from": {"key": "key"}}, LINK_TARGET, _link_step()],
            DOC,
        )
        assert _links(entities) == {
            "duplicates": [("A", "B")],
            "relates to": [("A", "C")],
        }

    def test_a_step_with_fixed_endpoints_applies_its_relation_map(self) -> None:
        entities = _cast(
            [
                {"vertex": "Issue", "from": {"key": "key"}},
                LINK_TARGET,
                _link_step(relation_map={"relates to": "relatesTo"}),
            ],
            DOC,
        )
        assert _links(entities) == {
            "duplicates": [("A", "B")],
            "relatesTo": [("A", "C")],
        }

    def test_relation_map_only_writes_only_the_mapped_relations(self) -> None:
        entities = _cast(
            [
                {"vertex": "Issue", "from": {"key": "key"}},
                LINK_TARGET,
                _link_step(
                    relation_map={"duplicates": "duplicates", "blocks": "blocks"},
                    relation_map_only=True,
                ),
            ],
            DOC,
            edges=[DUPLICATES, BLOCKS],
            strict=True,
        )
        assert _links(entities) == {"duplicates": [("A", "B")]}

    def test_strict_references_refuse_a_mapped_relation_left_undeclared(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="relation 'blocks'"):
            _cast(
                [
                    {"vertex": "Issue", "from": {"key": "key"}},
                    LINK_TARGET,
                    _link_step(
                        relation_map={"duplicates": "duplicates", "blocks": "blocks"},
                        relation_map_only=True,
                    ),
                ],
                DOC,
                edges=[DUPLICATES],
                strict=True,
            )

    def test_strict_references_still_refuse_an_open_relation_field(self) -> None:
        with pytest.raises(ValueError, match="relation None"):
            _cast(
                [
                    {"vertex": "Issue", "from": {"key": "key"}},
                    LINK_TARGET,
                    _link_step(),
                ],
                DOC,
                edges=[DUPLICATES],
                strict=True,
            )

    def test_a_role_step_reads_a_dotted_relation_field(self) -> None:
        pipeline = [
            {"vertex": "Issue", "from": {"key": "from.key"}, "role": "s"},
            {"vertex": "Issue", "from": {"key": "to.key"}, "role": "t"},
            {
                "edge": {
                    "source_role": "s",
                    "target_role": "t",
                    "relation_field": "type.outward",
                    "relation_map": {"duplicates": "duplicates"},
                    "relation_map_only": True,
                }
            },
        ]
        doc = {
            "from": {"key": "A"},
            "to": {"key": "B"},
            "type": {"outward": "duplicates"},
        }
        entities = _cast(pipeline, doc, edges=[DUPLICATES], strict=True)
        assert _links(entities) == {"duplicates": [("A", "B")]}
