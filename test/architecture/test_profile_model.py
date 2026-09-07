"""The report model: roll-up ordering, waivers, and the text renderer.

These pin the contract three surfaces share -- the CLI's output, a pack
pre-flight check's problem list, and an HTTP route's response schema. A change
here is a change to all three at once, which is the reason they are tested
apart from the assertions that produce them.
"""

from __future__ import annotations

import pytest

from graflo.architecture.profile.model import (
    AssertionResult,
    Finding,
    ProfileReport,
    ProfileWaivers,
    Severity,
    Status,
    Waiver,
    roll_up,
)


def _finding(status: Status, severity: Severity, message: str = "m") -> Finding:
    return Finding(assertion="a", status=status, severity=severity, message=message)


def _result(status: Status, findings: list[Finding] | None = None) -> AssertionResult:
    return AssertionResult(
        id="a", title="t", status=status, checked=1, findings=findings or []
    )


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ([], "not_applicable"),
        (["pass", "pass"], "pass"),
        (["pass", "warn"], "warn"),
        (["warn", "fail"], "fail"),
        (["pass", "waived"], "waived"),
        (["waived", "warn"], "warn"),
        (["pass", "not_applicable"], "pass"),
        (["not_applicable", "not_applicable"], "not_applicable"),
    ],
)
def test_roll_up_reports_the_worst_status(statuses, expected):
    assert roll_up(statuses) == expected


def test_waived_does_not_read_as_pass():
    """A waived assertion must stay visible, or a waiver hides a defect."""
    assert roll_up(["pass", "waived"]) == "waived"


def test_errors_ignores_waived_assertions():
    waived = AssertionResult(
        id="temporal",
        title="t",
        status="waived",
        checked=1,
        findings=[_finding("fail", "error")],
        waiver=Waiver(assertion="temporal", reason="static reference data"),
    )
    live = _result("fail", [_finding("fail", "error")])
    assert ProfileReport(
        profile="p", profile_version="0", status="fail", assertions=[waived]
    ).ok
    assert not ProfileReport(
        profile="p", profile_version="0", status="fail", assertions=[waived, live]
    ).ok


def test_warnings_include_waived_assertions():
    """A waiver excuses a failure; it does not silence the advisory findings."""
    waived = AssertionResult(
        id="a",
        title="t",
        status="waived",
        checked=1,
        findings=[_finding("warn", "warning")],
        waiver=Waiver(assertion="a", reason="r"),
    )
    report = ProfileReport(
        profile="p", profile_version="0", status="warn", assertions=[waived]
    )
    assert len(report.warnings()) == 1


def test_report_round_trips_through_json():
    """The route serialises this model; a lossy dump breaks the API contract."""
    report = ProfileReport(
        profile="world-model",
        profile_version="0.1",
        subject="m.yaml",
        status="warn",
        assertions=[
            AssertionResult(
                id="a",
                title="t",
                status="waived",
                checked=3,
                findings=[_finding("fail", "error", "boom")],
                waiver=Waiver(assertion="a", reason="deliberate"),
            )
        ],
    )
    restored = ProfileReport.model_validate_json(report.model_dump_json())
    assert restored == report


def test_to_lines_renders_the_waiver_reason():
    """A waiver without its reason visible is a silent pass."""
    report = ProfileReport(
        profile="world-model",
        profile_version="0.1",
        subject="m.yaml",
        status="warn",
        assertions=[
            AssertionResult(
                id="temporal",
                title="Temporal validity is declared or waived",
                status="waived",
                checked=1,
                findings=[],
                waiver=Waiver(assertion="temporal", reason="static reference data"),
            )
        ],
    )
    text = "\n".join(report.to_lines())
    assert "WAIVED" in text
    assert "static reference data" in text


def test_waiver_requires_a_reason():
    with pytest.raises(ValueError):
        # Validated from a mapping rather than constructed, so the missing
        # field is a runtime refusal to assert on rather than a type error.
        Waiver.model_validate({"assertion": "temporal"})


def test_waivers_lookup_is_by_assertion_id():
    doc = ProfileWaivers(
        waivers=[Waiver(assertion="temporal", reason="r")],
    )
    assert doc.for_assertion("temporal") is not None
    assert doc.for_assertion("provenance") is None
