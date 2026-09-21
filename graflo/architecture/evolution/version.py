"""Semantic version bump helpers for manifest evolution."""

from __future__ import annotations

import re

# A leading ``v`` is how versions are commonly written and tagged; it is not
# part of the number and is carried through untouched.
_SEMVER_PREFIX = re.compile(r"^(v?)(\d+)\.(\d+)\.(\d+)")


def semver_core(version: str | None) -> tuple[int, int, int] | None:
    """``(MAJOR, MINOR, PATCH)`` of *version*, or ``None`` when it has no such prefix."""
    if version is None:
        return None
    m = _SEMVER_PREFIX.match(str(version).strip())
    if not m:
        return None
    return int(m.group(2)), int(m.group(3)), int(m.group(4))


def bump_semver_minor(version: str | None) -> str:
    """Return *version* with MINOR incremented (MAJOR.PATCH unchanged), preserving suffix.

    A missing or blank version starts at ``0.1.0``. A version that is not
    ``MAJOR.MINOR.PATCH`` -- a date, a two-part number -- is returned as it is:
    there is no minor to increment, and replacing an author's label with
    ``0.1.0`` would move the version backwards and lose what they wrote.
    """
    if version is None or not str(version).strip():
        return "0.1.0"
    raw = str(version).strip()
    m = _SEMVER_PREFIX.match(raw)
    if not m:
        return raw
    prefix, major, minor, patch = m.group(1), *(int(m.group(i)) for i in (2, 3, 4))
    suffix = raw[m.end() :]  # prerelease/build metadata after X.Y.Z
    return f"{prefix}{major}.{minor + 1}.{patch}{suffix}"
