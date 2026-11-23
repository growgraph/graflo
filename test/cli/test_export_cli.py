"""``graflo json-schema``, ``schema-to-owl|shacl|ossie|compact|neo4j-graphrag``, ``schema-from-compact|neo4j-graphrag``."""

from __future__ import annotations

import json
import pathlib

import yaml
from click.testing import CliRunner
from rdflib import Graph

from graflo.architecture.contract.json_schema import GSTL_SCHEMA_ID
from graflo.cli.main import graflo

EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "examples"
SHOP = EXAMPLES / "11-connection-proxy" / "manifest_shop.yaml"


def test_json_schema_to_stdout_and_file(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(graflo, ["json-schema"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["$id"] == GSTL_SCHEMA_ID
    out = tmp_path / "gstl.json"
    result = CliRunner().invoke(graflo, ["json-schema", "-o", str(out)])
    assert result.exit_code == 0, result.output
    assert json.loads(out.read_text())["$id"] == GSTL_SCHEMA_ID


def test_schema_to_owl_writes_turtle() -> None:
    result = CliRunner().invoke(
        graflo, ["schema-to-owl", str(SHOP), "--base-iri", "http://example.org/shop/"]
    )
    assert result.exit_code == 0, result.output
    graph = Graph().parse(data=result.output, format="turtle")
    assert "http://example.org/shop/users" in {str(s) for s in graph.subjects()}


def test_schema_to_shacl_writes_json_ld(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "shapes.jsonld"
    result = CliRunner().invoke(
        graflo, ["schema-to-shacl", str(SHOP), "--format", "json-ld", "-o", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert len(Graph().parse(str(out), format="json-ld")) > 0


def test_schema_to_ossie_writes_a_directory(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(
        graflo, ["schema-to-ossie", str(SHOP), "-o", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "losses.md",
        "shop.mapping.yaml",
        "shop.ontology.yaml",
        "shop.semantic_model.yaml",
    ]


def test_a_missing_manifest_is_a_usage_error(tmp_path: pathlib.Path) -> None:
    result = CliRunner().invoke(graflo, ["schema-to-owl", str(tmp_path / "nope.yaml")])
    assert result.exit_code == 2


SUPPORT = pathlib.Path(__file__).resolve().parents[1] / "interop" / "data"


def _support_manifest(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "manifest.yaml"
    schema = yaml.safe_load((SUPPORT / "support_schema.yaml").read_text())
    path.write_text(yaml.safe_dump({"schema": schema}), encoding="utf-8")
    return path


def test_compact_both_ways_comes_back_to_the_same_text(tmp_path: pathlib.Path) -> None:
    runner = CliRunner()
    compact = tmp_path / "compact.yaml"
    result = runner.invoke(
        graflo,
        ["schema-to-compact", str(_support_manifest(tmp_path)), "-o", str(compact)],
    )
    assert result.exit_code == 0, result.output
    manifest = tmp_path / "back.yaml"
    result = runner.invoke(
        graflo, ["schema-from-compact", str(compact), "-o", str(manifest)]
    )
    assert result.exit_code == 0, result.output
    assert "gstl" in yaml.safe_load(manifest.read_text())["metadata"]
    again = runner.invoke(graflo, ["schema-to-compact", str(manifest)])
    assert again.exit_code == 0, again.output
    assert again.output == compact.read_text()


def test_schema_to_compact_refuses_a_lossy_schema(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "manifest.yaml"
    vertex = {
        "name": "A",
        "properties": [{"name": "k", "type": "UINT"}],
        "identity": ["k"],
    }
    schema = {
        "metadata": {"name": "s"},
        "core_schema": {
            "vertex_config": {"vertices": [vertex]},
            "edge_config": {"edges": []},
        },
    }
    path.write_text(yaml.safe_dump({"schema": schema}), encoding="utf-8")
    result = CliRunner().invoke(graflo, ["schema-to-compact", str(path)])
    assert result.exit_code == 1
    assert "cannot express" in result.output


def test_neo4j_graphrag_both_ways_with_losses(tmp_path: pathlib.Path) -> None:
    runner = CliRunner()
    document = tmp_path / "schema.json"
    losses = tmp_path / "losses.md"
    result = runner.invoke(
        graflo,
        [
            "schema-to-neo4j-graphrag",
            str(_support_manifest(tmp_path)),
            "-o",
            str(document),
            "--losses",
            str(losses),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(document.read_text())["node_types"][0]["label"] == "Customer"
    assert "semantics.unit" in losses.read_text()
    result = runner.invoke(graflo, ["schema-from-neo4j-graphrag", str(document)])
    assert result.exit_code == 0, result.output
    assert "no KEY" in result.output
    manifest = yaml.safe_load(result.stdout)
    assert manifest["schema"]["metadata"]["name"] == "schema"
