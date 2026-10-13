"""Fail when the public API breaks against the last release tag.

Run from the repository root with griffe available:

    uv run --no-project --with "griffe>=2.3,<3" python .github/scripts/api_breaks.py [TAG]

TAG defaults to the newest ``v*`` tag. A changed constant value is not a break.
When ``pyproject.toml`` already declares a higher major version than TAG, breaks
are reported but do not fail.
"""

import subprocess
import sys
import tomllib
from pathlib import Path

import griffe


def _latest_tag() -> str:
    out = subprocess.run(
        ["git", "tag", "--list", "v*", "--sort=-v:refname"],
        capture_output=True,
        text=True,
        check=True,
    )
    tags = out.stdout.split()
    if not tags:
        sys.exit("no v* tag to compare against")
    return tags[0]


def main() -> int:
    tag = sys.argv[1] if len(sys.argv) > 1 else _latest_tag()
    version = tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"]
    major_bump = int(version.split(".")[0]) > int(tag.lstrip("v").split(".")[0])

    old = griffe.load_git("graflo", ref=tag, search_paths=["."])
    new = griffe.load("graflo", search_paths=["."])
    breaks = [
        b
        for b in griffe.find_breaking_changes(old, new)
        if b.kind is not griffe.BreakageKind.ATTRIBUTE_CHANGED_VALUE
    ]
    for b in breaks:
        print(b.explain())
        print()
    print(f"{len(breaks)} breaking change(s) against {tag} (version {version})")
    if breaks and major_bump:
        print("allowed: the version is a new major")
        return 0
    if breaks:
        print("deprecate first (graflo._compat) or bump the major version")
    return 1 if breaks else 0


if __name__ == "__main__":
    sys.exit(main())
