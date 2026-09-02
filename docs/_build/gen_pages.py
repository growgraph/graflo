"""Generate the API reference tree at build time.

Walks ``graflo/**/*.py`` and writes one ``mkdocstrings`` page per module into
the virtual docs directory that ``mkdocs-gen-files`` feeds into the build.

These pages are **not** written to disk and must not be committed. A file in
``docs/reference/`` at a generated path is inert: the plugin opens each path in
``"w"`` mode, which replaces the entry in the MkDocs ``Files`` collection with
the generated one before any page renders. Committed stubs therefore go stale
silently -- with no build symptom -- which is why the tree was deleted rather
than maintained.

One kind of hand-written page is supported: a **section overview** at
``reference/<pkg>/index.md``. A package ``__init__.py`` would otherwise generate
``reference/<pkg>.md``, and under ``use_directory_urls`` both build to
``site/reference/<pkg>/index.html`` -- the same URL from two different source
paths, which MkDocs does not warn about and which the later write silently wins.
Rather than leave that to file ordering, a package whose overview exists on disk
is skipped here, so the override is a decision recorded in one place.

An overview that skips generation this way is responsible for the package's own
API docs: include ``::: graflo.<pkg>`` in it, or the package docstring and its
members are documented nowhere.

``reference/index.md`` is also hand-written; nothing is generated for it, since
the top-level ``graflo/__init__.py`` is skipped below.
"""

from pathlib import Path

import mkdocs_gen_files

PACKAGE = "graflo"
REFERENCE = Path("docs/reference")

for path in sorted(Path(PACKAGE).rglob("*.py")):
    is_pkg_init = path.name == "__init__.py"
    if is_pkg_init:
        if path.parent == Path(PACKAGE):
            continue
        pkg_dir = path.parent.relative_to(PACKAGE)
        # A hand-written overview owns this URL -- see the module docstring.
        if (REFERENCE / pkg_dir / "index.md").exists():
            continue
        doc_path = pkg_dir.with_suffix(".md")
    else:
        doc_path = path.relative_to(PACKAGE).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = list(doc_path.with_suffix("").parts)
    if not parts:
        continue

    with mkdocs_gen_files.open(full_doc_path, "w") as f:
        ident = ".".join([PACKAGE] + parts)
        if is_pkg_init:
            # `show_submodules: false` keeps a package page from inlining its
            # whole subtree (which would render `architecture` as one enormous
            # page); the summary table indexes that subtree instead, with each
            # submodule's one-line docstring summary -- so a package whose
            # `__init__` re-exports nothing says what is underneath it rather
            # than rendering as a dead end. The sidebar carries the links.
            f.write(
                f"# `{ident}`\n\n"
                f"::: {ident}\n"
                f"    options:\n"
                f"      show_submodules: false\n"
                f"      summary:\n"
                f"        modules: true\n"
            )
        else:
            f.write(f"# `{ident}`\n\n::: {ident}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)
