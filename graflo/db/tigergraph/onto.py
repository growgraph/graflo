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
