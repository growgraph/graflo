"""The six world-model assertions, each with a conformant and a defective case.

Manifests are built as authored documents rather than as models, because two of
the assertions ask what the author *declared* and a model has already filled in
the defaults that hide the answer. Every fixture here is the minimum that makes
one assertion the only variable.
"""

from __future__ import annotations

import copy

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.profile import (
    ProfileWaivers,
    check_manifest,
    check_manifest_config,
)

PROV = "http://www.w3.org/ns/prov#"
SOSA = "http://www.w3.org/ns/sosa/"
QUDT = "http://qudt.org/schema/qudt/"


def _conformant() -> dict:
    """The smallest manifest that satisfies every assertion."""
    return {
        "metadata": {"name": "tiny-model"},
        "schema": {
            "metadata": {"name": "tiny-model", "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Asset",
                            "semantics": {"iri": f"{PROV}Entity"},
                            "properties": [{"name": "asset_id", "type": "STRING"}],
                            "identity": ["asset_id"],
                        },
                        {
                            "name": "Agent",
                            "semantics": {"iri": f"{PROV}Agent"},
                            "properties": [{"name": "agent_id", "type": "STRING"}],
                            "identity": ["agent_id"],
                        },
                        {
                            "name": "Observation",
                            "semantics": {"iri": f"{SOSA}Observation"},
                            "properties": [
                                {"name": "obs_id", "type": "STRING"},
                                {"name": "result_value", "type": "FLOAT"},
                                {
                                    "name": "result_unit",
                                    "type": "STRING",
                                    "semantics": {"iri": f"{QUDT}ucumCode"},
                                },
                                {
                                    "name": "result_time",
                                    "type": "DATETIME",
                                    "semantics": {"iri": f"{SOSA}resultTime"},
                                },
                            ],
                            "identity": ["obs_id"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": "Observation",
                            "target": "Asset",
                            "relation": "hasFeatureOfInterest",
                            "directed": True,
                            "semantics": {"iri": f"{SOSA}hasFeatureOfInterest"},
                        },
                        {
                            "source": "Observation",
                            "target": "Agent",
                            "relation": "wasAttributedTo",
                            "directed": True,
                            "semantics": {"iri": f"{PROV}wasAttributedTo"},
                        },
                    ]
                },
            },
            "db_profile": {},
        },
    }


def _vertices(config: dict) -> list[dict]:
    return config["schema"]["graph"]["vertex_config"]["vertices"]


def _edges(config: dict) -> list[dict]:
    return config["schema"]["graph"]["edge_config"]["edges"]


def _vertex(config: dict, name: str) -> dict:
    return next(v for v in _vertices(config) if v["name"] == name)


def _assertion(report, assertion_id: str):
    return next(a for a in report.assertions if a.id == assertion_id)


# --- the baseline -----------------------------------------------------------


def test_the_conformant_fixture_passes_every_assertion():
    report = check_manifest_config(_conformant())
    assert report.ok, [f.message for f in report.errors()]
    assert report.status == "pass"


# --- 1. grounded types ------------------------------------------------------


def test_ungrounded_vertex_fails():
    config = _conformant()
    del _vertex(config, "Asset")["semantics"]
    result = _assertion(check_manifest_config(config), "grounded-types")
    assert result.status == "fail"
    assert any("vertex:Asset" == f.target for f in result.findings)


def test_ungrounded_edge_fails():
    config = _conformant()
    del _edges(config)[0]["semantics"]
    result = _assertion(check_manifest_config(config), "grounded-types")
    assert result.status == "fail"


def test_malformed_iri_is_an_error_not_a_warning():
    config = _conformant()
    _vertex(config, "Asset")["semantics"] = {"iri": "Entity"}
    result = _assertion(check_manifest_config(config), "grounded-types")
    assert result.status == "fail"
    assert "not an absolute IRI" in result.findings[0].message


def test_unrecognised_namespace_warns_rather_than_failing():
    """We have not heard of it is not the same claim as it is dead."""
    config = _conformant()
    _vertex(config, "Asset")["semantics"] = {"iri": "https://example.test/ns#Thing"}
    report = check_manifest_config(config)
    result = _assertion(report, "grounded-types")
    assert result.status == "warn"
    assert report.ok


# --- 2. declared identity ---------------------------------------------------


def test_blank_identity_fails():
    config = _conformant()
    asset = _vertex(config, "Asset")
    del asset["identity"]
    asset["blank"] = True
    result = _assertion(check_manifest_config(config), "declared-identity")
    assert result.status == "fail"
    assert "blank" in result.findings[0].message


def test_identity_from_all_properties_fallback_fails():
    """The failure the parsed model cannot show: no identity was authored."""
    config = _conformant()
    del _vertex(config, "Asset")["identity"]
    config["schema"]["graph"]["vertex_config"]["identity_from_all_properties"] = True
    result = _assertion(check_manifest_config(config), "declared-identity")
    assert result.status == "fail"
    assert "identity_from_all_properties" in result.findings[0].message


def test_identity_declaration_only_warns_without_the_authored_document():
    config = _conformant()
    del _vertex(config, "Asset")["identity"]
    config["schema"]["graph"]["vertex_config"]["identity_from_all_properties"] = True
    manifest = GraphManifest.from_config(copy.deepcopy(config))
    manifest.finish_init()
    result = _assertion(check_manifest(manifest), "declared-identity")
    assert result.status == "warn"
    assert "not verifiable" in result.findings[0].message


# --- 3. declared directionality ---------------------------------------------


def test_undeclared_directionality_fails():
    config = _conformant()
    del _edges(config)[0]["directed"]
    result = _assertion(check_manifest_config(config), "declared-directionality")
    assert result.status == "fail"
    assert "defaulted to True" in result.findings[0].message


def test_declaring_directed_false_passes():
    config = _conformant()
    _edges(config)[0]["directed"] = False
    assert (
        _assertion(check_manifest_config(config), "declared-directionality").status
        == "pass"
    )


def test_directionality_only_warns_without_the_authored_document():
    manifest = GraphManifest.from_config(_conformant())
    manifest.finish_init()
    result = _assertion(check_manifest(manifest), "declared-directionality")
    assert result.status == "warn"


# --- 4. declared units ------------------------------------------------------


def test_measured_property_with_no_unit_anywhere_fails():
    config = _conformant()
    properties = _vertex(config, "Observation")["properties"]
    _vertex(config, "Observation")["properties"] = [
        p for p in properties if p["name"] != "result_unit"
    ]
    result = _assertion(check_manifest_config(config), "declared-units")
    assert result.status == "fail"
    assert result.findings[0].target == "vertex:Observation.result_value"


def test_schema_level_unit_passes():
    config = _conformant()
    observation = _vertex(config, "Observation")
    observation["properties"] = [
        p for p in observation["properties"] if p["name"] != "result_unit"
    ]
    next(p for p in observation["properties"] if p["name"] == "result_value")[
        "semantics"
    ] = {"unit": "Cel"}
    assert _assertion(check_manifest_config(config), "declared-units").status == "pass"


def test_row_level_unit_passes_and_says_so():
    """An abstract type cannot name one unit honestly; carrying it is the fix."""
    result = _assertion(check_manifest_config(_conformant()), "declared-units")
    assert result.status == "pass"
    assert result.findings[0].detail["unit_source"] == "row"


def test_unit_written_as_an_iri_warns_about_mixed_conventions():
    config = _conformant()
    observation = _vertex(config, "Observation")
    observation["properties"] = [
        p for p in observation["properties"] if p["name"] != "result_unit"
    ]
    next(p for p in observation["properties"] if p["name"] == "result_value")[
        "semantics"
    ] = {"unit": "http://qudt.org/vocab/unit/DEG_C"}
    report = check_manifest_config(config)
    result = _assertion(report, "declared-units")
    assert result.status == "warn"
    assert report.ok


def test_a_float_that_is_part_of_the_key_is_not_measured():
    config = _conformant()
    asset = _vertex(config, "Asset")
    asset["properties"].append({"name": "grid_ref", "type": "FLOAT"})
    asset["identity"] = ["asset_id", "grid_ref"]
    assert _assertion(check_manifest_config(config), "declared-units").status == "pass"


# --- 5. temporal ------------------------------------------------------------


def test_no_temporal_grounding_fails():
    config = _conformant()
    observation = _vertex(config, "Observation")
    next(p for p in observation["properties"] if p["name"] == "result_time")[
        "semantics"
    ] = {"iri": f"{SOSA}somethingElse"}
    result = _assertion(check_manifest_config(config), "temporal")
    assert result.status == "fail"


def test_a_waiver_excuses_temporal_without_reporting_a_pass():
    config = _conformant()
    observation = _vertex(config, "Observation")
    next(p for p in observation["properties"] if p["name"] == "result_time")[
        "semantics"
    ] = {"iri": f"{SOSA}somethingElse"}
    waivers = ProfileWaivers(
        waivers=[{"assertion": "temporal", "reason": "static reference data"}]
    )
    report = check_manifest_config(config, waivers=waivers)
    result = _assertion(report, "temporal")
    assert result.status == "waived"
    assert result.waiver is not None
    assert report.ok
    assert report.status == "waived"


def test_a_waiver_does_not_excuse_a_different_assertion():
    config = _conformant()
    del _vertex(config, "Asset")["semantics"]
    waivers = ProfileWaivers(waivers=[{"assertion": "temporal", "reason": "r"}])
    report = check_manifest_config(config, waivers=waivers)
    assert not report.ok


# --- 6. provenance ----------------------------------------------------------


def test_no_agent_type_fails():
    config = _conformant()
    _vertex(config, "Agent")["semantics"] = {"iri": f"{PROV}Entity"}
    result = _assertion(check_manifest_config(config), "provenance")
    assert result.status == "fail"
    assert any("agent" in f.message for f in result.findings)


def test_no_provenance_edge_fails():
    config = _conformant()
    _edges(config)[1]["semantics"] = {"iri": f"{SOSA}hasFeatureOfInterest"}
    result = _assertion(check_manifest_config(config), "provenance")
    assert result.status == "fail"
    assert any("derivation or attribution" in f.message for f in result.findings)


def test_ingest_half_is_not_applicable_without_an_ingestion_model():
    """Reported, not silently passed: the question was never asked."""
    result = _assertion(check_manifest_config(_conformant()), "provenance")
    assert result.status == "pass"
    assert any(f.status == "not_applicable" for f in result.findings)


# --- schema-less manifests --------------------------------------------------


def test_a_manifest_with_no_schema_reports_not_applicable_not_six_passes():
    config = {
        "metadata": {"name": "bindings-only"},
        "ingestion_model": {"resources": []},
    }
    report = check_manifest_config(config)
    grounded = _assertion(report, "grounded-types")
    assert grounded.status == "not_applicable"
    assert grounded.checked == 0


@pytest.mark.parametrize("profile", ["world-model"])
def test_known_profiles_resolve(profile):
    assert check_manifest_config(_conformant(), profile=profile).profile == profile


def test_unknown_profile_names_the_known_ones():
    with pytest.raises(KeyError, match="world-model"):
        check_manifest_config(_conformant(), profile="does-not-exist")
