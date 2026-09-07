"""The conformance report, and the waiver document that can excuse part of it.

One report model serves all three surfaces a profile check is reachable from --
the ``graflo check`` CLI, a pack pre-flight check, and an HTTP route. Keeping
them on one model is what stops the three drifting into three vocabularies for
the same finding; :meth:`ProfileReport.to_lines` is the single text renderer,
and a caller wanting only the failures asks for :meth:`ProfileReport.errors`.

Waivers live in their own document rather than on the manifest. A waiver is a
statement by an *operator* about a deployment ("this model has no time axis and
that is deliberate"), not a property of the world model, and putting it on the
contract would make two manifests that describe the same world compare unequal.
The cost is stated plainly and is real: a waiver travels out of band, so a
registry cannot see one, and it is not covered by the manifest's content
address.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel

#: Outcome of one assertion, or of one element under it.
#:
#: ``waived`` is deliberately not ``pass``: an assertion excused by an operator
#: must stay visible in the report, or a waiver becomes indistinguishable from
#: conformance.
Status = Literal["pass", "fail", "warn", "waived", "not_applicable"]

Severity = Literal["error", "warning", "info"]

#: Ordered worst-first, for rolling element statuses up into an assertion and
#: assertions up into a report.
_STATUS_RANK: dict[str, int] = {
    "fail": 0,
    "warn": 1,
    "waived": 2,
    "pass": 3,
    "not_applicable": 4,
}


class Waiver(ConfigBaseModel):
    """An operator's decision to excuse one assertion, with its reason."""

    assertion: str = PydanticField(
        ...,
        description="Identifier of the assertion this waiver excuses, e.g. ``temporal``.",
    )
    reason: str = PydanticField(
        ...,
        description=(
            "Why the assertion does not apply to this deployment. Required: a "
            "waiver without a reason is a silent pass with extra steps."
        ),
    )
    granted_by: str | None = PydanticField(default=None)
    granted_at: datetime | None = PydanticField(default=None)
    expires: datetime | None = PydanticField(default=None)


class ProfileWaivers(ConfigBaseModel):
    """A sidecar document of waivers granted against one profile."""

    profile: str = PydanticField(default="world-model")
    subject: str | None = PydanticField(
        default=None,
        description=(
            "What these waivers were granted against -- a manifest name or "
            "content hash. Advisory: nothing enforces the match."
        ),
    )
    waivers: list[Waiver] = PydanticField(default_factory=list)

    def for_assertion(self, assertion_id: str) -> Waiver | None:
        """The waiver covering *assertion_id*, or ``None``."""
        for waiver in self.waivers:
            if waiver.assertion == assertion_id:
                return waiver
        return None


class Finding(ConfigBaseModel):
    """One assertion's verdict about one element."""

    assertion: str = PydanticField(...)
    status: Status = PydanticField(...)
    severity: Severity = PydanticField(...)
    target: str | None = PydanticField(
        default=None,
        description=(
            "The element this is about, in a stable address form: "
            "``vertex:Observation``, ``vertex:Observation.result_value``, "
            "``edge:Observation-hasFeatureOfInterest->Asset``, or ``manifest``."
        ),
    )
    message: str = PydanticField(
        ..., description="One line, already readable without the detail payload."
    )
    detail: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="Machine payload -- the IRI, the unit token, the identity mode.",
    )


class AssertionResult(ConfigBaseModel):
    """One assertion's outcome over the whole manifest."""

    id: str = PydanticField(...)
    title: str = PydanticField(...)
    required: bool = PydanticField(default=True)
    status: Status = PydanticField(...)
    checked: int = PydanticField(
        default=0,
        description=(
            "Elements examined. Zero means the assertion had nothing to say "
            "about this manifest, which is reported as ``not_applicable`` "
            "rather than as a pass."
        ),
    )
    findings: list[Finding] = PydanticField(default_factory=list)
    waiver: Waiver | None = PydanticField(default=None)


class ProfileReport(ConfigBaseModel):
    """The result of checking one manifest against one named profile."""

    profile: str = PydanticField(...)
    profile_version: str = PydanticField(...)
    graflo_version: str | None = PydanticField(default=None)
    subject: str | None = PydanticField(
        default=None,
        description="What was checked -- a path, or ``uuid@version``. Set by the caller.",
    )
    manifest_hash: str | None = PydanticField(
        default=None,
        description=(
            "Content address of the manifest checked, so a stored report can be "
            "matched back to the artifact that produced it."
        ),
    )
    status: Status = PydanticField(...)
    assertions: list[AssertionResult] = PydanticField(default_factory=list)

    @property
    def ok(self) -> bool:
        """No error-severity finding survived waivers."""
        return not self.errors()

    def errors(self) -> list[Finding]:
        """Every finding that makes the manifest non-conformant."""
        return [
            finding
            for result in self.assertions
            if result.waiver is None
            for finding in result.findings
            if finding.severity == "error"
        ]

    def warnings(self) -> list[Finding]:
        """Every advisory finding, waived assertions included."""
        return [
            finding
            for result in self.assertions
            for finding in result.findings
            if finding.severity == "warning"
        ]

    def to_lines(self) -> list[str]:
        """The report as text. The one renderer -- every surface calls this."""
        marks = {
            "pass": "PASS",
            "fail": "FAIL",
            "warn": "WARN",
            "waived": "WAIVED",
            "not_applicable": "N/A",
        }
        subject = self.subject or "<manifest>"
        lines = [
            f"profile {self.profile} v{self.profile_version} -- {subject}",
            f"  overall: {marks[self.status]}",
            "",
        ]
        for result in self.assertions:
            lines.append(
                f"  [{marks[result.status]:>6}] {result.id}: {result.title}"
                f"  ({result.checked} checked)"
            )
            if result.waiver is not None:
                lines.append(f"           waived: {result.waiver.reason}")
            for finding in result.findings:
                where = f"{finding.target}: " if finding.target else ""
                lines.append(f"           - {where}{finding.message}")
        return lines


def roll_up(statuses: list[Status]) -> Status:
    """The worst status in *statuses*; ``not_applicable`` when there are none.

    Ordered fail > warn > waived > pass > not_applicable, so an assertion that
    passed on nine elements and failed on one reports ``fail``, and a report
    with one waived assertion and the rest passing reports ``warn`` -- visible,
    but not a failure.
    """
    if not statuses:
        return "not_applicable"
    return min(statuses, key=lambda s: _STATUS_RANK[s])


__all__ = [
    "AssertionResult",
    "Finding",
    "ProfileReport",
    "ProfileWaivers",
    "Severity",
    "Status",
    "Waiver",
    "roll_up",
]
