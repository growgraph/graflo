from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()
pname = "graflo"

for path in sorted(Path(pname).rglob("*.py")):
    is_pkg_init = path.name == "__init__.py"
    if is_pkg_init:
        if path.parent == Path(pname):
            continue
        doc_path = path.parent.relative_to(pname).with_suffix(".md")
    else:
        doc_path = path.relative_to(pname).with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = list(doc_path.with_suffix("").parts)
    if not parts:
        continue
    parts_str: tuple[str, ...] = tuple(parts)
    nav[parts_str] = str(full_doc_path)

    with mkdocs_gen_files.open(full_doc_path, "w") as f:
        ident = ".".join([pname] + parts)
        if is_pkg_init:
            f.write(
                f"# `{ident}`\n\n"
                f"::: {ident}\n"
                f"    options:\n"
                f"      show_submodules: false\n"
            )
        else:
            f.write(f"# `{ident}`\n\n::: {ident}\n")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)
