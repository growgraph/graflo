"""TigerGraph identifier validation against reserved words and invalid characters."""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)


class _TigerGraphNameRules(NamedTuple):
    reserved_words_upper: frozenset[str]
    forbidden_prefixes: tuple[str, ...]
    invalid_characters: tuple[str, ...]


@lru_cache(maxsize=1)
def load_tigergraph_name_rules() -> _TigerGraphNameRules | None:
    """Load ``reserved_words.json`` once per process (cached)."""
    json_path = Path(__file__).parent / "reserved_words.json"
    try:
        with open(json_path, "r") as f:
            reserved_data = json.load(f)
    except FileNotFoundError:
        logger.warning(
            "Could not find reserved_words.json at %s, skipping validation",
            json_path,
        )
        return None
    except json.JSONDecodeError as e:
        logger.warning(
            "Could not parse reserved_words.json: %s, skipping validation", e
        )
        return None

    reserved_words: set[str] = set()
    reserved_words.update(
        reserved_data.get("reserved_words", {}).get("gsql_keywords", [])
    )
    reserved_words.update(
        reserved_data.get("reserved_words", {}).get("cpp_keywords", [])
    )
    reserved_upper = frozenset(w.upper() for w in reserved_words)
    forbidden = tuple(reserved_data.get("forbidden_prefixes", []))
    invalid_chars = tuple(
        reserved_data.get("invalid_characters", {}).get("characters", [])
    )
    return _TigerGraphNameRules(
        reserved_words_upper=reserved_upper,
        forbidden_prefixes=forbidden,
        invalid_characters=invalid_chars,
    )


def validate_tigergraph_schema_name(name: str, name_type: str) -> None:
    """
    Validate a TigerGraph identifier against reserved words and invalid characters.

    Args:
        name: The identifier to validate
        name_type: Kind of identifier for error messages (e.g. ``"graph"``,
            ``"vertex"``, ``"edge"``, ``"vertex property"``, ``"edge attribute"``)

    Raises:
        ValueError: If the name is empty, reserved, uses a forbidden prefix,
            or contains invalid characters
    """
    if not name:
        raise ValueError(f"{name_type.capitalize()} name cannot be empty")

    rules = load_tigergraph_name_rules()
    if rules is None:
        return

    name_upper = name.upper()
    if name_upper in rules.reserved_words_upper:
        raise ValueError(
            f"{name_type.capitalize()} name '{name}' is a TigerGraph reserved word. "
            f"Reserved words cannot be used as identifiers. "
            f"Please choose a different name."
        )

    for prefix in rules.forbidden_prefixes:
        if name.startswith(prefix):
            raise ValueError(
                f"{name_type.capitalize()} name '{name}' starts with forbidden prefix '{prefix}'. "
                f"Please choose a different name."
            )

    found_chars = [char for char in rules.invalid_characters if char in name]
    if found_chars:
        raise ValueError(
            f"{name_type.capitalize()} name '{name}' contains invalid characters: {found_chars}. "
            f"TigerGraph identifiers should use alphanumeric characters and underscores only. "
            f"Special characters (especially hyphens and dots) are problematic for REST API endpoints. "
            f"Please choose a different name."
        )
