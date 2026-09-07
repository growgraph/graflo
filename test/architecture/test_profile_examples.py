"""The profile against the shipped examples.

Two jobs. The checker must never raise on a real manifest -- a conformance
report that crashes is worse than one that reports failures, because the whole
point is running it against models nobody here authored. And the reference manifest must
stay conformant: it is the artifact the profile exists to describe, so a
regression in either one shows up here.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

from graflo.architecture.profile import check_manifest_config

EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[2] / "examples"
REFERENCE = EXAMPLES_DIR / "22-state-core" / "reference.yaml"


def _manifest_paths() -> list[pathlib.Path]:
    """Every ``manifest*.yaml`` under ``examples/``.

    Deliberately excludes ``generated-manifest.yaml``: those are ``Schema``
    documents rather than manifests, and feeding one in is a caller error the
    surfaces report, not something this suite should assert about.
    """
    return sorted(
        path
        for path in EXAMPLES_DIR.rglob("manifest*.yaml")
        if not path.name.startswith("generated-")
    )


@pytest.mark.parametrize("path", _manifest_paths(), ids=lambda p: p.parent.name)
def test_the_checker_never_raises_on_a_shipped_manifest(path: pathlib.Path):
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    report = check_manifest_config(config, subject=str(path))
    assert report.status in ("pass", "fail", "warn", "waived", "not_applicable")
    assert len(report.assertions) == 6


def test_the_reference_is_conformant():
    """The profile's own reference artifact. If this fails, one of them moved."""
    config = yaml.safe_load(REFERENCE.read_text(encoding="utf-8"))
    report = check_manifest_config(config, subject=str(REFERENCE))
    assert report.ok, [f"{f.target}: {f.message}" for f in report.errors()]
    assert report.status == "pass"


def test_the_reference_declares_time_as_modelled_state():
    """Design (b): validity is a property of a State, not a hidden axis."""
    config = yaml.safe_load(REFERENCE.read_text(encoding="utf-8"))
    report = check_manifest_config(config)
    temporal = next(a for a in report.assertions if a.id == "temporal")
    assert temporal.status == "pass"
    carriers = temporal.findings[0].detail["carriers"]
    assert "State.valid_from" in carriers
    assert "State.valid_to" in carriers
    assert "Observation.result_time" in carriers


def test_the_reference_carries_its_unit_per_row():
    """An abstract Observation cannot name one unit in its contract."""
    config = yaml.safe_load(REFERENCE.read_text(encoding="utf-8"))
    units = next(
        a for a in check_manifest_config(config).assertions if a.id == "declared-units"
    )
    assert units.status == "pass"
    assert units.findings[0].detail["unit_source"] == "row"
