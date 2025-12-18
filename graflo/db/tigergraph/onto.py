"""TigerGraph-specific type mappings and constants.

This module provides TigerGraph-specific type mappings and constants,
separating database-specific concerns from universal types defined at
the root GraFlo level.

Universal types (FieldType enum) are defined in graflo.architecture.vertex.
This module provides TigerGraph-specific mappings and aliases.
"""

from graflo.architecture.vertex import FieldType

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

# Set of valid TigerGraph type strings (FieldType enum values)
# FieldType enum values are already in TigerGraph format
VALID_TIGERGRAPH_TYPES: set[str] = {ft.value for ft in FieldType}
