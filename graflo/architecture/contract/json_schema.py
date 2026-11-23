"""The GSTL JSON Schema: a manifest checkable without graflo.

:func:`manifest_json_schema` describes a manifest **as graflo writes it**
(:meth:`GraphManifest.to_minimal_canonical_dict`, the form every writer emits).
Pipeline steps are therefore described in their written shorthand
(``{"vertex": ...}``, ``{"edge": {...}}``, ``{"transform": {...}}``,
``{"descend": {...}}``, ``{"vertex_router": {...}}``), not in the internal
form discriminated on ``type``. Authoring shorthands that graflo also reads on
input (a bare string for a field, ``graph`` for ``core_schema``) are not part
of the schema.

JSON Schema covers shape only: cross-references (an identity field that exists,
an edge whose endpoints are declared) are checked by
:meth:`GraphManifest.finish_init`.
"""

from __future__ import annotations

import copy
import json
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import GSTL_VERSION

#: The ``$id`` of the GSTL 1.x manifest schema.
GSTL_SCHEMA_ID = "https://ontology.growgraph.dev/gstl/1.0/schema.json"

_DRAFT = "https://json-schema.org/draft/2020-12/schema"

#: Step kind -> (definition of its body, written key, wrapped in that key?).
_STEP_DEFS: dict[str, tuple[str, bool]] = {
    "vertex": ("VertexActorConfig", False),
    "edge": ("EdgeActorConfig", True),
    "transform": ("TransformActorConfig", True),
    "descend": ("DescendActorConfig", True),
    "vertex_router": ("VertexRouterActorConfig", True),
}


def _step_def_name(kind: str) -> str:
    return "".join(part.capitalize() for part in kind.split("_")) + "Step"


def _is_step_union(node: dict[str, Any]) -> bool:
    discriminator = node.get("discriminator")
    return (
        isinstance(discriminator, dict)
        and discriminator.get("propertyName") == "type"
        and set(discriminator.get("mapping", {})) == set(_STEP_DEFS)
    )


def _replace_step_unions(node: Any) -> Any:
    if isinstance(node, dict):
        if _is_step_union(node):
            return {
                "oneOf": [
                    {"$ref": f"#/$defs/{_step_def_name(kind)}"} for kind in _STEP_DEFS
                ]
            }
        return {key: _replace_step_unions(value) for key, value in node.items()}
    if isinstance(node, list):
        return [_replace_step_unions(item) for item in node]
    return node


def _rewrite_steps(schema: dict[str, Any]) -> dict[str, Any]:
    defs: dict[str, Any] = schema["$defs"]
    for kind, (def_name, wrapped) in _STEP_DEFS.items():
        body = defs[def_name]
        body.get("properties", {}).pop("type", None)
        if "required" in body:
            body["required"] = [name for name in body["required"] if name != "type"]
        if kind == "descend":
            # Written as `pipeline`; `apply` is an input alias only.
            properties = body["properties"]
            properties["pipeline"] = properties.pop("apply")
        if wrapped:
            defs[_step_def_name(kind)] = {
                "type": "object",
                "title": _step_def_name(kind),
                "properties": {kind: {"$ref": f"#/$defs/{def_name}"}},
                "required": [kind],
                "additionalProperties": False,
            }
        else:
            defs[_step_def_name(kind)] = {"$ref": f"#/$defs/{def_name}"}
    return _replace_step_unions(schema)


def manifest_json_schema() -> dict[str, Any]:
    """The JSON Schema (draft 2020-12) of a GSTL manifest, keys sorted.

    Returns:
        A fresh dict; callers may mutate it.
    """
    schema = copy.deepcopy(
        GraphManifest.model_json_schema(by_alias=True, mode="validation")
    )
    schema = _rewrite_steps(schema)
    schema["$schema"] = _DRAFT
    schema["$id"] = GSTL_SCHEMA_ID
    schema["title"] = "GSTL manifest"
    schema["x-gstl-version"] = GSTL_VERSION
    return json.loads(json.dumps(schema, sort_keys=True))
