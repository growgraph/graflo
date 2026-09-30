"""``graflo ingest`` end to end through Click, writing to the file backend."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

import pytest
import yaml
from click.testing import CliRunner, Result

from graflo.architecture.backend import GraFloBackendReader
from graflo.cli.main import graflo


@dataclass(frozen=True)
class Project:
    """The files one ``graflo ingest`` run is pointed at."""

    manifest: pathlib.Path
    db_config: pathlib.Path
    output_dir: pathlib.Path
    people_csv: pathlib.Path

    def run(self, *args: str, db_config: pathlib.Path | None = None) -> Result:
        return CliRunner().invoke(
            graflo,
            [
                "ingest",
                "--db-config-path",
                str(db_config or self.db_config),
                "--schema-path",
                str(self.manifest),
                *args,
            ],
        )

    def person_ids(self) -> set[str]:
        reader = GraFloBackendReader(self.output_dir)
        return {
            str(doc["id"])
            for batch in reader.iter_vertex_batches("person")
            for doc in batch
        }


def _write(path: pathlib.Path, payload: object) -> pathlib.Path:
    path.write_text(yaml.safe_dump(payload))
    return path


@pytest.fixture
def project(tmp_path: pathlib.Path) -> Project:
    data = tmp_path / "data"
    data.mkdir()
    people_csv = data / "people.csv"
    people_csv.write_text("id,name\n1,Ada\n2,Grace\n")
    output_dir = tmp_path / "graph"
    manifest = _write(
        tmp_path / "manifest.yaml",
        {
            "schema": {
                "metadata": {"name": "hr"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "person",
                                "properties": ["id", "name"],
                                "identity": ["id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
                "db_profile": {},
            },
            "ingestion_model": {
                "resources": [{"name": "people", "pipeline": [{"vertex": "person"}]}]
            },
            "bindings": {
                "connectors": [
                    {
                        "regex": "^people.*\\.csv$",
                        "sub_path": str(data),
                        "resource_name": "people",
                    }
                ]
            },
        },
    )
    db_config = _write(
        tmp_path / "db.yaml",
        {"db_type": "graflo_backend", "output_dir": str(output_dir)},
    )
    return Project(
        manifest=manifest,
        db_config=db_config,
        output_dir=output_dir,
        people_csv=people_csv,
    )


def test_ingests_the_sources_the_manifest_binds(project: Project) -> None:
    result = project.run("--fresh-start", "true")

    assert result.exit_code == 0, result.output
    assert project.person_ids() == {"1", "2"}


def test_source_path_is_not_an_option(project: Project) -> None:
    result = project.run("--source-path", str(project.people_csv.parent))

    assert result.exit_code == 2
    assert "No such option" in result.output


def test_a_source_only_database_is_refused_as_target(
    project: Project, tmp_path: pathlib.Path
) -> None:
    sparql = _write(
        tmp_path / "sparql.yaml",
        {"db_type": "sparql", "uri": "http://localhost:3030", "dataset": "ds"},
    )

    result = project.run(db_config=sparql)

    assert result.exit_code == 2
    assert "sparql" in result.output
    assert "target" in result.output


def test_data_source_config_ingests_the_listed_sources(
    project: Project, tmp_path: pathlib.Path
) -> None:
    extra = tmp_path / "more_people.csv"
    extra.write_text("id,name\n7,Edsger\n")
    sources = _write(
        tmp_path / "sources.yaml",
        {"data_sources": [{"resource_name": "people", "path": str(extra)}]},
    )

    result = project.run(
        "--fresh-start", "true", "--data-source-config-path", str(sources)
    )

    assert result.exit_code == 0, result.output
    assert project.person_ids() == {"7"}


def test_init_only_defines_the_schema_and_reads_nothing(project: Project) -> None:
    result = project.run("--init-only")

    assert result.exit_code == 0, result.output
    assert project.person_ids() == set()
