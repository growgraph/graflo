"""Every example explains itself, and its docs page is generated from it.

An example's ``README.md`` is the single source of its story: the docs build
turns it into the page ``docs/examples/<slug>/index.md``. These tests hold the
conventions that make that work, and the one that makes an example worth
opening: its title is the question it answers, and the reason to care comes
before any setup.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
DIR_NAME = re.compile(r"(\d{2})-([a-z0-9-]+)")
LINK = re.compile(r"(?:\]\(|src=\"|href=\")([^)\"\s]+)")
DOCS_ONLY_SYNTAX = ("!!! ", "??? ", "{{", "--8<--", "{ width")

EXAMPLE_DIRS = sorted(p for p in EXAMPLES.iterdir() if p.is_dir())


def _slug(directory: Path) -> str:
    """The slug of an example directory named ``NN-<slug>``."""
    match = DIR_NAME.fullmatch(directory.name)
    assert match, f"{directory.name} is not named NN-<slug>"
    return match.group(2)


def _prose(text: str) -> str:
    """The text outside fenced code blocks."""
    kept: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if line.strip().startswith(("```", "~~~")):
            in_fence = not in_fence
        elif not in_fence:
            kept.append(line)
    return "\n".join(kept)


def test_examples_are_numbered_without_gaps() -> None:
    numbers = []
    for directory in EXAMPLE_DIRS:
        match = DIR_NAME.fullmatch(directory.name)
        assert match, f"{directory.name} is not named NN-<slug>"
        numbers.append(int(match.group(1)))
    assert numbers == list(range(1, len(numbers) + 1))


def test_slugs_are_unique() -> None:
    slugs = [_slug(d) for d in EXAMPLE_DIRS]
    assert len(slugs) == len(set(slugs))


@pytest.mark.parametrize("directory", EXAMPLE_DIRS, ids=lambda d: d.name)
def test_readme_opens_with_a_question_and_a_motivation(directory: Path) -> None:
    readme = directory / "README.md"
    assert readme.is_file(), f"{directory.name} has no README.md"
    lines = readme.read_text(encoding="utf-8").splitlines()

    assert lines[0].startswith("# "), "the first line is the title"
    assert lines[0].rstrip().endswith("?"), "the title is the question answered"

    first_section = next(
        (i for i, line in enumerate(lines) if line.startswith("## ")), len(lines)
    )
    motivation = [
        line
        for line in _prose("\n".join(lines[1:first_section])).splitlines()
        if line.strip()
    ]
    assert motivation, "say why the example is useful before the first section"


@pytest.mark.parametrize("directory", EXAMPLE_DIRS, ids=lambda d: d.name)
def test_readme_renders_on_its_own_and_in_the_docs(directory: Path) -> None:
    prose = _prose((directory / "README.md").read_text(encoding="utf-8"))
    for syntax in DOCS_ONLY_SYNTAX:
        assert syntax not in prose, f"{syntax!r} only renders in the docs build"

    for target in LINK.findall(prose):
        if re.match(r"^(https?:|mailto:|#)", target):
            continue
        path = (directory / target.partition("#")[0]).resolve()
        assert path.exists(), f"{directory.name}: broken link {target}"


@pytest.mark.parametrize("directory", EXAMPLE_DIRS, ids=lambda d: d.name)
def test_docs_page_is_generated_and_in_the_nav(directory: Path) -> None:
    slug = _slug(directory)
    page = f"examples/{slug}/index.md"
    assert not (ROOT / "docs" / page).exists(), (
        f"docs/{page} is committed, so the generated page would be shadowed"
    )
    assert page in (ROOT / "properdocs.yml").read_text(encoding="utf-8")
