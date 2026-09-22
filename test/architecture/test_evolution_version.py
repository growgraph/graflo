"""The version bump applied with a change set."""

from __future__ import annotations

import pytest

from graflo.architecture.evolution.version import bump_semver_minor, semver_core


@pytest.mark.parametrize(
    ("version", "bumped"),
    [
        ("1.2.3", "1.3.3"),
        ("1.2.3-rc.1+build5", "1.3.3-rc.1+build5"),
        ("v2.0.0", "v2.1.0"),
        (" 0.9.0 ", "0.10.0"),
    ],
)
def test_the_minor_is_incremented_and_the_rest_kept(version: str, bumped: str) -> None:
    assert bump_semver_minor(version) == bumped


@pytest.mark.parametrize("version", [None, "", "   "])
def test_a_missing_version_starts_at_0_1_0(version: str | None) -> None:
    assert bump_semver_minor(version) == "0.1.0"


@pytest.mark.parametrize("version", ["2024-Q3", "2.1", "latest"])
def test_a_version_that_is_not_semver_is_left_as_written(version: str) -> None:
    """Replacing it with ``0.1.0`` would move the version backwards."""
    assert bump_semver_minor(version) == version
    assert semver_core(version) is None


def test_a_bump_never_lowers_a_version() -> None:
    for version in ["0.0.1", "1.2.3", "v2.0.0", "10.20.30-beta"]:
        before, after = semver_core(version), semver_core(bump_semver_minor(version))
        assert before is not None and after is not None
        assert after > before


def test_a_leading_v_is_not_part_of_the_number() -> None:
    assert semver_core("v3.1.4") == semver_core("3.1.4") == (3, 1, 4)
