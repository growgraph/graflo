"""Normalization of raw actor step dicts for validation."""

from __future__ import annotations

from typing import Any


def _steps_list(val: Any) -> list[Any]:
    """Ensure value is a list of steps (single dict becomes [dict])."""
    return val if isinstance(val, list) else [val]


def normalize_actor_step(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw step dict so it has 'type' and flat structure for validation."""
    if not isinstance(data, dict):
        return data
    data = dict(data)
    if "type" in data:
        return data

    if "vertex" in data:
        data["type"] = "vertex"
        return data

    if "edge" in data:
        inner = data.pop("edge")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "edge"
        return data
    if ("source" in data or "from" in data) and ("target" in data or "to" in data):
        data = dict(data)
        data["type"] = "edge"
        return data
    if "create_edge" in data:
        inner = data.pop("create_edge")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "edge"
        return data

    if "descend" in data:
        inner = data.pop("descend")
        if isinstance(inner, dict):
            if "pipeline" in inner:
                inner["pipeline"] = [
                    normalize_actor_step(s) for s in _steps_list(inner["pipeline"])
                ]
            elif "apply" in inner:
                inner["pipeline"] = [
                    normalize_actor_step(s) for s in _steps_list(inner["apply"])
                ]
                del inner["apply"]
            data.update(inner)
        data["type"] = "descend"
        if "pipeline" not in data and "apply" in data:
            data["pipeline"] = [
                normalize_actor_step(s) for s in _steps_list(data["apply"])
            ]
            del data["apply"]
        return data

    if "vertex_router" in data:
        inner = data.pop("vertex_router")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "vertex_router"
        return data

    if "edge_router" in data:
        inner = data.pop("edge_router")
        if isinstance(inner, dict):
            data.update(inner)
        data["type"] = "edge_router"
        return data

    if "transform" in data:
        inner = data.pop("transform")
        if not isinstance(inner, dict):
            raise ValueError("transform step must be an object with rename or call.")
        data.update(inner)
        data["type"] = "transform"
        return data

    if "apply" in data:
        data["type"] = "descend"
        data["pipeline"] = [normalize_actor_step(s) for s in _steps_list(data["apply"])]
        del data["apply"]
        return data
    if "pipeline" in data:
        data["type"] = "descend"
        data["pipeline"] = [
            normalize_actor_step(s) for s in _steps_list(data["pipeline"])
        ]
        return data

    if "type" not in data and ("rename" in data or "call" in data):
        data = dict(data)
        data["type"] = "transform"
        return data

    return data
