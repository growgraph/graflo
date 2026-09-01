"""(De)serialization for contract operations.

The op models are already pydantic under an ``op``-discriminated union, but the
union was only ever used as a type annotation: there was no way to load a
*heterogeneous* list of ops from YAML, so change sets could not be stored,
transported or replayed. This module closes that gap.

``RevisionOp`` is ``ManifestOp`` minus
:class:`~graflo.architecture.evolution.ops.ComposeManifestsOp`, which is binary
(two manifests in, one out) and is rejected by the unary dispatcher anyway. A
revision applies to exactly one manifest, so composition is not a revision op.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field as PydanticField
from pydantic import TypeAdapter
from suthing import FileHandle

from .ops import (
    AddEdgeIndexesOp,
    AddEdgePropertiesOp,
    AddEdgesOp,
    AddInverseEdgesOp,
    AddResourceTransformsOp,
    AddSecondaryIdentitiesOp,
    AddVertexIndexesOp,
    AddVertexPropertiesOp,
    AddVerticesOp,
    ChangeFieldTypesOp,
    ManifestOp,
    MergeEdgesOp,
    MergeVerticesOp,
    ProjectManifestOp,
    RemoveEdgeIndexesOp,
    RemoveEdgePropertiesOp,
    RemoveEdgesOp,
    RemoveSecondaryIdentitiesOp,
    RemoveVertexIndexesOp,
    RemoveVertexPropertiesOp,
    RemoveVerticesOp,
    RenameEdgePropertiesOp,
    RenameRelationsOp,
    RenameResourcesOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
    ReplaceEdgeIdentitiesOp,
    ReplaceIdentityOp,
    RetargetEdgesOp,
    SanitizeOp,
    SetEdgeDirectedOp,
)

#: Every op that can appear in a revision: the full vocabulary except the
#: binary ``compose_manifests``.
RevisionOp = Annotated[
    RemoveVerticesOp
    | AddResourceTransformsOp
    | AddVerticesOp
    | AddEdgesOp
    | RetargetEdgesOp
    | AddSecondaryIdentitiesOp
    | RemoveSecondaryIdentitiesOp
    | ReplaceEdgeIdentitiesOp
    | ChangeFieldTypesOp
    | AddVertexIndexesOp
    | RemoveVertexIndexesOp
    | AddEdgeIndexesOp
    | RemoveEdgeIndexesOp
    | SetEdgeDirectedOp
    | MergeVerticesOp
    | RenameVertexPropertiesOp
    | RemoveVertexPropertiesOp
    | AddVertexPropertiesOp
    | RenameVerticesOp
    | RenameRelationsOp
    | RenameResourcesOp
    | RemoveEdgesOp
    | MergeEdgesOp
    | RenameEdgePropertiesOp
    | RemoveEdgePropertiesOp
    | AddEdgePropertiesOp
    | AddInverseEdgesOp
    | ProjectManifestOp
    | ReplaceIdentityOp
    | SanitizeOp,
    PydanticField(discriminator="op"),
]

_op_adapter: TypeAdapter[Any] = TypeAdapter(RevisionOp)
_op_list_adapter: TypeAdapter[list[Any]] = TypeAdapter(list[RevisionOp])


def op_from_dict(payload: dict[str, Any]) -> Any:
    """Validate one op payload into its concrete model, keyed by ``op``."""
    return _op_adapter.validate_python(payload)


def ops_from_dicts(payloads: list[dict[str, Any]]) -> list[Any]:
    """Validate a heterogeneous list of op payloads."""
    return _op_list_adapter.validate_python(payloads)


def op_to_dict(op: Any) -> dict[str, Any]:
    """Serialize one op to a payload that is guaranteed to load back.

    Compact form first (defaults dropped), then **verified** by re-validating
    it. That check is not paranoia: a nested discriminated union whose tag has
    a default — ``IdentityTarget.mode`` is one — loses its tag under
    ``exclude_defaults`` and becomes unloadable. Rather than special-casing
    every such field, fall back to the full form whenever the compact one does
    not reproduce the op.

    A change set exists to be replayed exactly; silently emitting something
    that cannot be read back is the one failure this layer must not have.
    """
    compact = op.to_dict(skip_defaults=True) | {"op": op.op}
    try:
        if _op_adapter.validate_python(compact) == op:
            return compact
    except Exception:
        # Any validation failure means the compact form is not loadable.
        pass
    return op.to_dict(skip_defaults=False) | {"op": op.op}


def ops_to_dicts(ops: list[Any]) -> list[dict[str, Any]]:
    """Serialize ops to plain dicts that load back identically."""
    return [op_to_dict(op) for op in ops]


def ops_from_yaml(source: str | Any) -> list[Any]:
    """Load ops from a YAML file path, or from a YAML string.

    Accepts either a bare list of op mappings or a mapping with an ``ops`` key.
    """
    if isinstance(source, str) and ("\n" in source or source.lstrip().startswith("-")):
        import yaml

        payload = yaml.safe_load(source)
    else:
        payload = FileHandle.load(source)
    if isinstance(payload, dict):
        payload = payload.get("ops", [])
    if not isinstance(payload, list):
        raise ValueError(
            "expected a list of operations, or a mapping with an 'ops' key; "
            f"got {type(payload).__name__}"
        )
    return ops_from_dicts(payload)


def ops_to_yaml_str(ops: list[Any]) -> str:
    """Serialize ops to a YAML list."""
    import yaml

    return yaml.safe_dump(ops_to_dicts(ops), sort_keys=False, default_flow_style=False)


def is_revision_op(op: ManifestOp) -> bool:
    """Whether *op* may appear in a revision (i.e. is not binary)."""
    return type(op).__name__ != "ComposeManifestsOp"


__all__ = [
    "RevisionOp",
    "is_revision_op",
    "op_from_dict",
    "op_to_dict",
    "ops_from_dicts",
    "ops_from_yaml",
    "ops_to_dicts",
    "ops_to_yaml_str",
]
