"""Per-name membership checks on ``VertexConfig`` agree with its name lists."""

from __future__ import annotations

from graflo.architecture.schema.vertex import VertexConfig


def _config() -> VertexConfig:
    return VertexConfig.from_dict(
        {
            "vertices": [
                {"name": "plain", "properties": ["id"], "identity": ["id"]},
                {"name": "blank", "properties": ["note"], "blank": True},
                {"name": "assigned", "properties": ["id"], "assigned": True},
                {
                    "name": "hashed",
                    "properties": ["a", "b"],
                    "identity": ["id"],
                    "hash_identity_properties": ["a", "b"],
                },
            ]
        }
    )


def test_each_check_agrees_with_its_list() -> None:
    config = _config()
    names = ["plain", "blank", "assigned", "hashed", "unknown"]

    assert [n for n in names if config.has_vertex(n)] == sorted(
        config.vertex_set, key=names.index
    )
    assert [n for n in names if config.is_blank(n)] == config.blank_vertices
    assert [n for n in names if config.is_assigned(n)] == config.assigned_vertices
    assert [
        n for n in names if config.is_hash_identity(n)
    ] == config.hash_identity_vertices


def test_a_check_sees_a_vertex_changed_in_place() -> None:
    config = _config()

    config["plain"].blank = True

    assert config.is_blank("plain")
