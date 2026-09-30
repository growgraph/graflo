"""Generate one docs page per example from the example's own README.

Each ``examples/NN-<slug>/README.md`` is the single source for its example.
This script turns it into the page ``examples/<slug>/index.md`` in the virtual
docs directory that ``mkdocs-gen-files`` feeds into the build, so the text is
never written twice and the page cannot drift from the example.

The page path carries the slug, not the number, so that reordering the examples
does not move their URLs.

Three things happen to a README on its way into the docs:

1. **Links are rewritten.** A README links with paths that work on GitHub:
   its own files (``manifest.yaml``, ``figs/x.svg``), another example
   (``../NN-<slug>/README.md``), a docs page (``../../docs/concepts/x.md``).
   Links to docs pages and to other examples become links between docs pages;
   links to anything else outside the example become GitHub URLs.
2. **Linked files are copied** next to the page, so that figures render and
   links to the example's own files resolve.
3. **A "Files" section is appended**, showing the example's manifests and
   scripts as they are on disk.

Pages generated here must not be committed under ``docs/examples/``: the
plugin opens each path in ``"w"`` mode, so a committed file at a generated path
is replaced before anything renders.
"""

import re
from pathlib import Path, PurePosixPath

import mkdocs_gen_files

EXAMPLES = Path("examples")
DOCS = Path("docs")
REPO_BLOB = "https://github.com/growgraph/graflo/blob/main"
REPO_TREE = "https://github.com/growgraph/graflo/tree/main"

DIR_NAME = re.compile(r"(\d{2})-([a-z0-9-]+)")
LINK = re.compile(r"(\]\(|src=\"|href=\")([^)\"\s]+)")
FENCE = re.compile(r"^(```|~~~)")

# Files shown in the "Files" section, in this order; longer files are linked.
SHOWN_SUFFIXES = {".yaml": "yaml", ".yml": "yaml", ".py": "python"}
MAX_SHOWN_LINES = 250


def example_dirs() -> list[tuple[Path, str]]:
    """Return ``(directory, slug)`` for every example that has a README."""
    found = []
    for directory in sorted(EXAMPLES.iterdir()):
        match = DIR_NAME.fullmatch(directory.name)
        if match and (directory / "README.md").is_file():
            found.append((directory, match.group(2)))
    return found


def normalize(path: PurePosixPath) -> PurePosixPath | None:
    """Collapse ``..`` segments; ``None`` when the path leaves the repository."""
    parts: list[str] = []
    for part in path.parts:
        if part == "..":
            if not parts:
                return None
            parts.pop()
        elif part != ".":
            parts.append(part)
    return PurePosixPath(*parts)


def rewrite_target(target: str, directory: Path, copies: set[PurePosixPath]) -> str:
    """Rewrite one link target of a README for use in its docs page."""
    if re.match(r"^(https?:|mailto:|#|/)", target):
        return target
    path_part, _, anchor = target.partition("#")
    suffix = f"#{anchor}" if anchor else ""
    resolved = normalize(PurePosixPath(directory.as_posix()) / path_part)
    if resolved is None:
        return target
    parts = resolved.parts

    if parts[:2] == (EXAMPLES.name, directory.name):
        if Path(resolved).is_file():
            copies.add(PurePosixPath(*parts[2:]))
            return target
        return f"{REPO_TREE}/{resolved}{suffix}"

    if parts[0] == EXAMPLES.name and len(parts) >= 2:
        match = DIR_NAME.fullmatch(parts[1])
        rest = parts[2:]
        if match and rest in ((), ("README.md",)):
            return f"../{match.group(2)}/index.md{suffix}"
        return f"{REPO_BLOB}/{resolved}{suffix}"

    if parts[0] == DOCS.name and resolved.suffix == ".md":
        return f"../../{PurePosixPath(*parts[1:])}{suffix}"

    kind = REPO_TREE if Path(resolved).is_dir() else REPO_BLOB
    return f"{kind}/{resolved}{suffix}"


def rewrite_links(text: str, directory: Path, copies: set[PurePosixPath]) -> str:
    """Rewrite link targets outside fenced code blocks."""
    out: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if FENCE.match(line.strip()):
            in_fence = not in_fence
        elif not in_fence:
            line = LINK.sub(
                lambda m: m.group(1) + rewrite_target(m.group(2), directory, copies),
                line,
            )
        out.append(line)
    return "\n".join(out) + "\n"


def files_section(directory: Path) -> str:
    """Render the example's manifests and scripts as collapsed code blocks."""
    shown = sorted(
        (p for p in directory.iterdir() if p.suffix in SHOWN_SUFFIXES and p.is_file()),
        key=lambda p: (p.suffix == ".py", p.name),
    )
    if not shown:
        return ""
    blocks = [
        "\n## Files\n",
        f"The example lives in [`{directory.as_posix()}`]"
        f"({REPO_TREE}/{directory.as_posix()}).\n",
    ]
    for path in shown:
        body = path.read_text(encoding="utf-8").rstrip("\n")
        if body.count("\n") + 1 > MAX_SHOWN_LINES:
            blocks.append(f"- [`{path.name}`]({REPO_BLOB}/{path.as_posix()})\n")
            continue
        indented = "\n".join(f"    {line}" if line else "" for line in body.split("\n"))
        blocks.append(
            f'??? example "{path.name}"\n\n'
            f"    ```{SHOWN_SUFFIXES[path.suffix]}\n{indented}\n    ```\n"
        )
    return "\n".join(blocks)


for example_dir, slug in example_dirs():
    readme = example_dir / "README.md"
    linked: set[PurePosixPath] = set()
    page_text = rewrite_links(readme.read_text(encoding="utf-8"), example_dir, linked)
    page_text += files_section(example_dir)

    page = Path("examples", slug, "index.md")
    with mkdocs_gen_files.open(page, "w") as f:
        f.write(page_text)
    mkdocs_gen_files.set_edit_path(page, Path("..", readme))

    for rel in sorted(linked):
        if rel.suffix == ".md":
            continue
        with mkdocs_gen_files.open(Path("examples", slug, rel), "wb") as f:
            f.write((example_dir / rel).read_bytes())
