"""A strict run refuses a non-conforming manifest before it touches the database.

``clear_data`` and ``define_schema`` act on the target, so the reference check
has to come first: a manifest the run would refuse must not empty or redefine
the graph on its way to being refused.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.connections.onto import ArangoConfig
from graflo.hq.caster import Caster
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams


def _manifest(*, undeclared: bool) -> GraphManifest:
    relation = "pilots" if undeclared else "owns"
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "people"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "properties": ["id"],
                                "identity": ["id"],
                            },
                            {
                                "name": "vehicle",
                                "properties": ["id"],
                                "identity": ["id"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "person",
                                "target": "vehicle",
                                "relation": "owns",
                            }
                        ]
                    },
                },
            },
            "ingestion_model": {
                "resources": [
                    {
                        "name": "rows",
                        "pipeline": [
                            {"vertex": "person", "from": {"id": "person_id"}},
                            {"vertex": "vehicle", "from": {"id": "vehicle_id"}},
                            {
                                "edge": {
                                    "from": "person",
                                    "to": "vehicle",
                                    "relation": relation,
                                }
                            },
                        ],
                    }
                ]
            },
        }
    )
    manifest.finish_init()
    return manifest


def _target() -> ArangoConfig:
    return ArangoConfig(uri="http://127.0.0.1:1", database="never_reached")


@pytest.fixture
def db() -> Iterator[MagicMock]:
    with patch("graflo.hq.graph_engine.ConnectionManager") as connection_manager:
        yield connection_manager


class TestRefusedBeforeTheDatabase:
    def test_ingest_with_clear_data(self, db: MagicMock) -> None:
        engine = GraphEngine(target_db_flavor=_target().connection_type)

        with pytest.raises(ValueError, match="does not declare"):
            engine.ingest(
                manifest=_manifest(undeclared=True),
                target_db_config=_target(),
                ingestion_params=IngestionParams(clear_data=True),
            )

        db.assert_not_called()

    def test_define_and_ingest_with_recreate_schema(self, db: MagicMock) -> None:
        engine = GraphEngine(target_db_flavor=_target().connection_type)

        with pytest.raises(ValueError, match="does not declare"):
            engine.define_and_ingest(
                manifest=_manifest(undeclared=True),
                target_db_config=_target(),
                recreate_schema=True,
                clear_data=True,
            )

        db.assert_not_called()


class TestAConformingOrLooseRunProceeds:
    @pytest.mark.parametrize(("undeclared", "strict"), [(False, True), (True, False)])
    def test_clear_data_reaches_the_database(
        self, db: MagicMock, undeclared: bool, strict: bool
    ) -> None:
        engine = GraphEngine(target_db_flavor=_target().connection_type)

        with patch.object(Caster, "ingest") as cast:
            engine.ingest(
                manifest=_manifest(undeclared=undeclared),
                target_db_config=_target(),
                ingestion_params=IngestionParams(
                    clear_data=True, strict_references=strict
                ),
            )

        db.assert_called_once()
        cast.assert_called_once()
