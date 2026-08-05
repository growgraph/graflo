"""TigerGraph-specific type mappings and constants.

This module provides TigerGraph-specific type mappings and constants,
separating database-specific concerns from universal types defined at
the root GraFlo level.

Universal types (FieldType enum) are defined in graflo.architecture.vertex.
This module provides TigerGraph-specific mappings and aliases.
"""

from graflo.architecture.schema.vertex import SCALAR_FIELD_TYPE_VALUES, FieldType

# Type aliases for TigerGraph
# Maps common type name variants to standard FieldType values
# These are TigerGraph-specific mappings (e.g., "INTEGER" -> "INT" for TigerGraph)
TIGERGRAPH_TYPE_ALIASES: dict[str, str] = {
    "INTEGER": FieldType.INT.value,
    "STR": FieldType.STRING.value,
    "BOOLEAN": FieldType.BOOL.value,
    "DATE": FieldType.DATETIME.value,
    "TIME": FieldType.DATETIME.value,
}

# Bare scalar TigerGraph types (LIST is compositional: LIST<item>, not a bare type)
VALID_TIGERGRAPH_TYPES: set[str] = set(SCALAR_FIELD_TYPE_VALUES)


def field_type_from_gsql(declared: str | None) -> FieldType | None:
    """Map a GSQL DDL type token back to a ``FieldType``.

    Returns ``None`` for anything outside the mapping -- ``MAP<..>``, ``SET<..>``
    and user-defined tuples have no graflo equivalent, and a recovered schema is
    more useful with an untyped field than with a wrong one.
    """
    if not declared:
        return None
    token = declared.strip().upper()
    if token.startswith("LIST<"):
        return FieldType.LIST
    token = TIGERGRAPH_TYPE_ALIASES.get(token, token)
    if token in VALID_TIGERGRAPH_TYPES:
        return FieldType(token)
    return None
