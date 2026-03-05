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

    if "transform" in data:
        inner = data.pop("transform")
        if isinstance(inner, dict):
            data.update(inner)
        if "switch" in data:
            switch = data.pop("switch")
            if isinstance(switch, dict) and switch:
                key = next(iter(switch))
                vals = switch[key]
                if isinstance(vals, (list, tuple)) and len(vals) >= 2:
                    data.setdefault("input", [key])
                    data.setdefault("dress", {"key": vals[0], "value": vals[1]})
        if "dress" in data and isinstance(data["dress"], (list, tuple)):
            vals = data["dress"]
            if len(vals) >= 2:
                data["dress"] = {"key": vals[0], "value": vals[1]}
        data["type"] = "transform"
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

    if "type" not in data and (
        "name" in data or "map" in data or "switch" in data or "dress" in data
    ):
        data = dict(data)
        if "switch" in data:
            switch = data.pop("switch")
            if isinstance(switch, dict) and switch:
                key = next(iter(switch))
                vals = switch[key]
                if isinstance(vals, (list, tuple)) and len(vals) >= 2:
                    data.setdefault("input", [key])
                    data.setdefault("dress", {"key": vals[0], "value": vals[1]})
        if "dress" in data and isinstance(data["dress"], (list, tuple)):
            vals = data["dress"]
            if len(vals) >= 2:
                data["dress"] = {"key": vals[0], "value": vals[1]}
        data["type"] = "transform"
        return data

    return data
