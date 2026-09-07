"""Loading and writing manifests, shared by the verbs that do both.

Extracted from ``graflo.cli.commit`` once ``check`` and ``compose`` needed the
same two lines. The load is not merely ``FileHandle.load`` -- it runs
``finish_init()``, which is where GraFlo validates cross-block references, so a
verb that skips it operates on a manifest that has not been checked.
"""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import Any

import click
from suthing import FileHandle

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import Vertex


def load_manifest(path: str | Path) -> GraphManifest:
    """Read and fully initialise the manifest at *path*."""
    manifest = GraphManifest.from_config(FileHandle.load(path))
    manifest.finish_init()
    return manifest


def load_mapping(path: str | Path) -> dict[str, Any]:
    """Read a YAML/JSON document that must be a mapping.

    Raises:
        click.ClickException: the document is a list or a scalar. Pydantic's
            own error for that names a model the user never wrote.
    """
    data = FileHandle.load(path)
    if not isinstance(data, dict):
        raise click.ClickException(
            f"{path}: expected a mapping at the top level, got {type(data).__name__}"
        )
    return data


#: Keys a generated manifest must state even when they equal the model default.
#:
#: A conformance profile asks two questions -- is an identity declared, is a
#: direction declared -- that only the *authored document* can answer, because
#: ``VertexConfig`` fills an unset identity and ``Edge.directed`` defaults to
#: ``True``. Writing with ``exclude_defaults`` therefore erases a declaration
#: that was deliberately made: a generated ``directed: true`` disappears and the
#: manifest reads as if nobody decided. Writing *without* it is no better --
#: every default becomes a declaration and the question stops meaning anything.
#:
#: So the writer forces exactly the keys the generator meant to declare. In
#: practice ``edge.directed`` is the only one a default-excluding dump loses:
#: a non-empty ``identity``, ``blank: true`` and a populated
#: ``hash_identity_properties`` all differ from their defaults and survive.
DECLARED_KEYS: tuple[str, ...] = ("edge.directed", "vertex.identity")


#: Fields each holder actually has, so a typo in a declared key is refused
#: rather than silently doing nothing -- which is the failure mode that would
#: leave a generated manifest quietly non-conformant.
_DECLARABLE: dict[str, frozenset[str]] = {
    "vertex": frozenset(Vertex.model_fields),
    "edge": frozenset(Edge.model_fields),
}


def _split_declared(token: str) -> tuple[str, str]:
    """``"edge.directed"`` -> ``("edge", "directed")``, or raise saying why not."""
    holder, _, key = token.partition(".")
    fields = _DECLARABLE.get(holder)
    if fields is None:
        raise ValueError(
            f"unknown declared key {token!r}: holder must be one of "
            f"{sorted(_DECLARABLE)}"
        )
    if key not in fields:
        raise ValueError(f"unknown declared key {token!r}: {holder} has no {key!r}")
    return holder, key


def _core_block(payload: dict[str, Any]) -> dict[str, Any] | None:
    """The ``core_schema`` block of a dumped manifest, under either alias."""
    schema = payload.get("schema") or payload.get("graph_schema")
    if not isinstance(schema, dict):
        return None
    core = schema.get("core_schema") or schema.get("graph")
    return core if isinstance(core, dict) else None


def _force_declared(
    payload: dict[str, Any], manifest: GraphManifest, declare: Collection[str]
) -> None:
    """Re-inject *declare* keys, read off the model, into a dumped *payload*.

    Positional: ``model_dump`` preserves list order, so the nth dumped entry is
    the nth model entry. Matching by name instead would have to cope with a
    dumped edge whose ``relation`` was dropped as ``None``.
    """
    core = _core_block(payload)
    if core is None:
        return
    schema = manifest.graph_schema
    if schema is None:
        return

    holders = {
        "vertex": (
            core.get("vertex_config", {}).get("vertices", []),
            schema.core_schema.vertex_config.vertices,
        ),
        "edge": (
            core.get("edge_config", {}).get("edges", []),
            schema.core_schema.edge_config.edges,
        ),
    }
    for token in declare:
        holder, key = _split_declared(token)
        dumped, models = holders[holder]
        for block, model in zip(dumped, models, strict=False):
            if isinstance(block, dict):
                block[key] = getattr(model, key)


def manifest_to_dict(
    manifest: GraphManifest, *, declare: Collection[str] = ()
) -> dict[str, Any]:
    """A compact dump of *manifest*, with *declare* keys stated explicitly."""
    payload = manifest.to_dict(skip_defaults=True)
    if declare:
        _force_declared(payload, manifest, declare)
    return payload


def dump_manifest(
    manifest: GraphManifest, path: Path | None, *, declare: Collection[str] = ()
) -> None:
    """Write *manifest* to *path*, announcing it. ``None`` writes nothing."""
    if path is None:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    FileHandle.dump(manifest_to_dict(manifest, declare=declare), path)
    click.echo(f"written: {path}")


__all__ = [
    "DECLARED_KEYS",
    "dump_manifest",
    "load_manifest",
    "load_mapping",
    "manifest_to_dict",
]
