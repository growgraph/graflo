"""Semantic version bump helpers for manifest evolution."""

from __future__ import annotations

import re

_SEMVER_PREFIX = re.compile(r"^(\d+)\.(\d+)\.(\d+)")


def bump_semver_minor(version: str | None) -> str:
    """Return *version* with MINOR incremented (MAJOR.PATCH unchanged), preserving suffix.

    If the numeric prefix cannot be parsed, returns ``0.1.0``.
    """
    if version is None or not str(version).strip():
        return "0.1.0"
    raw = str(version).strip()
    m = _SEMVER_PREFIX.match(raw)
    if not m:
        return "0.1.0"
    major, minor, patch = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    suffix = raw[m.end() :]  # prerelease/build metadata after X.Y.Z
    return f"{major}.{minor + 1}.{patch}{suffix}"
