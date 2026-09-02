"""TigerGraph identifier validation against reserved words and invalid characters."""

from __future__ import annotations

import logging

from graflo.db.util import load_tigergraph_identifier_rules

logger = logging.getLogger(__name__)


def validate_tigergraph_schema_name(name: str, name_type: str) -> None:
    """
    Validate a TigerGraph identifier against reserved words and invalid characters.

    Rules come from ``reserved_words.json`` via
    :func:`graflo.db.util.load_tigergraph_identifier_rules`, the same loader the
    name sanitizer uses — so what this rejects is exactly what the sanitizer
    rewrites.

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

    rules = load_tigergraph_identifier_rules()
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
