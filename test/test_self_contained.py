"""The repository reads on its own: no issue ids from a tracker outside it.

Code, tests and docs state the mechanism a comment is about. An id such as
``ABC-DEF-001`` resolves only for someone holding the tracker, and goes stale
when the issue closes.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRACKER_ID = re.compile(
    r"\b(?:CORE|DEV|SRV|SCHEWEA|ARCH)-[A-Z0-9]+(?:-[A-Z0-9]+)*-\d{3}\b"
)
SCANNED = [
    ("graflo", "*.py"),
    ("test", "*.py"),
    ("docs", "*.md"),
    ("examples", "*.md"),
    (".", "README.md"),
    (".", "CHANGELOG.md"),
]


def test_no_tracker_ids_in_the_repository() -> None:
    hits = [
        f"{path.relative_to(ROOT)}:{number}"
        for folder, pattern in SCANNED
        for path in sorted(
            (ROOT / folder).glob(pattern)
            if folder == "."
            else (ROOT / folder).rglob(pattern)
        )
        if path != Path(__file__).resolve()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if TRACKER_ID.search(line)
    ]
    assert not hits, f"state the mechanism instead of an issue id: {hits}"
