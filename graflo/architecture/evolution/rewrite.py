"""Structured rewrite of vertex names in pipeline dicts and related resource fields."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from graflo.architecture.pipeline.runtime.actor.config.normalize import (
    normalize_actor_step,
)


def _map_name(name: str | None, mapping: dict[str, str]) -> str | None:
    if name is None:
        return None
    return mapping.get(name, name)


def rewrite_vertex_names_in_step(
    step: dict[str, Any], mapping: dict[str, str]
) -> dict[str, Any]:
    """Return a deep-copied step with vertex names rewritten per *mapping*."""
    if not mapping:
        return deepcopy(step)
    s = normalize_actor_step(dict(step))
    out = deepcopy(s)
    t = out.get("type")

    if t == "vertex":
        v = out.get("vertex")
        if isinstance(v, str) and v in mapping:
            out["vertex"] = mapping[v]

    elif t == "vertex_router":
        tm = out.get("type_map")
        if isinstance(tm, dict):
            out["type_map"] = {
                k: _map_name(str(v), mapping) or v for k, v in tm.items()
            }
        vfm = out.get("vertex_from_map")
        if isinstance(vfm, dict):
            new_vfm: dict[str, Any] = {}
            for k, v in vfm.items():
                nk = mapping.get(k, k)
                new_vfm[nk] = v
            out["vertex_from_map"] = new_vfm

    elif t == "edge":
        for key in ("source", "from"):
            if key in out:
                val = out[key]
                if isinstance(val, str) and val in mapping:
                    out[key] = mapping[val]
        for key in ("target", "to"):
            if key in out:
                val = out[key]
                if isinstance(val, str) and val in mapping:
                    out[key] = mapping[val]

    elif t == "descend":
        pl = out.get("pipeline")
        if isinstance(pl, list):
            out["pipeline"] = [
                rewrite_vertex_names_in_step(cast_step(x), mapping)
                for x in pl
                if isinstance(x, dict)
            ]

    return out


def cast_step(x: Any) -> dict[str, Any]:
    if not isinstance(x, dict):
        raise TypeError(f"expected dict step, got {type(x)}")
    return x


def rewrite_vertex_names_in_pipeline(
    pipeline: list[dict[str, Any]], mapping: dict[str, str]
) -> list[dict[str, Any]]:
    """Rewrite all steps in a resource pipeline."""
    if not mapping:
        return deepcopy(pipeline)
    return [rewrite_vertex_names_in_step(s, mapping) for s in pipeline]


def rewrite_vertex_names_in_value(obj: Any, mapping: dict[str, str]) -> Any:
    """Deep-rewrite *obj* (pipelines, infer specs, extra_weights, nested dicts)."""
    if not mapping:
        return deepcopy(obj) if isinstance(obj, (dict, list)) else obj
    if isinstance(obj, list):
        return [rewrite_vertex_names_in_value(x, mapping) for x in obj]
    if isinstance(obj, dict):
        if "edge" in obj and isinstance(obj["edge"], dict):
            inner = dict(obj)
            inner["edge"] = rewrite_vertex_names_in_value(obj["edge"], mapping)
            return inner
        t = obj.get("type")
        if t in ("vertex", "edge", "descend", "vertex_router"):
            return rewrite_vertex_names_in_step(obj, mapping)
        if t == "transform":
            return deepcopy(obj)
        if all(k in obj for k in ("source", "target")):
            out = deepcopy(obj)
            src = out.get("source")
            tgt = out.get("target")
            if isinstance(src, str) and src in mapping:
                out["source"] = mapping[src]
            if isinstance(tgt, str) and tgt in mapping:
                out["target"] = mapping[tgt]
            return out
        if "vertex" in obj and isinstance(obj["vertex"], str) and t is None:
            out = deepcopy(obj)
            if out["vertex"] in mapping:
                out["vertex"] = mapping[out["vertex"]]
            return out
        return {k: rewrite_vertex_names_in_value(v, mapping) for k, v in obj.items()}
    return obj


def pipeline_mentions_any_vertex(steps: list[dict[str, Any]], names: set[str]) -> bool:
    """Return True if any pipeline step references a vertex name in *names*."""
    if not names:
        return False
    for step in steps:
        if not isinstance(step, dict):
            continue
        s = normalize_actor_step(dict(step))
        t = s.get("type")
        if t == "vertex":
            if s.get("vertex") in names:
                return True
        elif t == "vertex_router":
            tm = s.get("type_map") or {}
            if any(v in names for v in tm.values() if isinstance(v, str)):
                return True
            vfm = s.get("vertex_from_map") or {}
            if any(k in names for k in vfm if isinstance(k, str)):
                return True
        elif t == "edge":
            for key in ("source", "from", "target", "to"):
                val = s.get(key)
                if isinstance(val, str) and val in names:
                    return True
        elif t == "descend":
            pl = s.get("pipeline") or []
            if isinstance(pl, list) and pipeline_mentions_any_vertex(
                [cast_step(x) for x in pl if isinstance(x, dict)], names
            ):
                return True
    return False
