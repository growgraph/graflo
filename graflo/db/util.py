"""Database utilities for graph operations.

This module provides utility functions for working with database operations,
including cursor handling, data serialization, and schema management.

Key Functions:
    - get_data_from_cursor: Retrieve data from a cursor with optional limit
    - serialize_value: Serialize non-serializable values (datetime, Decimal, etc.)
    - serialize_document: Serialize all values in a document dictionary
    - load_reserved_words: Load reserved words for a database flavor
    - sanitize_attribute_name: Sanitize attribute names to avoid reserved words

Example:
    >>> from graflo.onto import DBType    >>> # ArangoDB-specific AQL query (collection is ArangoDB terminology)
    >>> cursor = db.execute("FOR doc IN vertex_class RETURN doc")
    >>> batch = get_data_from_cursor(cursor, limit=100)
    >>> # Serialize datetime objects in a document
    >>> doc = {"id": 1, "created_at": datetime.now()}
    >>> serialized = serialize_document(doc)
    >>> # Sanitize reserved words
    >>> reserved = load_reserved_words(DBType.TIGERGRAPH)
    >>> sanitized = sanitize_attribute_name("SELECT", reserved)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from arango.exceptions import CursorNextError

from graflo.db.connection.onto import DBType

logger = logging.getLogger(__name__)


def get_data_from_cursor(cursor, limit=None):
    """Retrieve data from a cursor with optional limit.

    This function iterates over a database cursor and collects the results
    into a batch. It handles cursor iteration errors and supports an optional
    limit on the number of items retrieved.

    Args:
        cursor: Database cursor to iterate over
        limit: Optional maximum number of items to retrieve

    Returns:
        list: Batch of items retrieved from the cursor

    Note:
        The function will stop iteration if:
        - The limit is reached
        - The cursor is exhausted
        - A CursorNextError occurs
    """
    batch = []
    cnt = 0
    while True:
        try:
            if limit is not None and cnt >= limit:
                raise StopIteration
            item = next(cursor)
            batch.append(item)
            cnt += 1
        except StopIteration:
            return batch
        except CursorNextError:
            return batch


def serialize_value(value):
    """Serialize non-serializable values for database operations.

    Converts datetime, date, time, and Decimal objects to JSON-serializable types.
    This is useful for databases that require JSON-serializable parameters or
    when serializing data for storage.

    Args:
        value: Value to serialize

    Returns:
        Serialized value:
        - datetime/date/time objects become ISO format strings
        - Decimal objects become floats
        - Other values are returned unchanged

    Raises:
        TypeError: If the value type is not serializable and not handled

    Example:
        >>> from datetime import datetime
        >>> serialize_value(datetime(2023, 12, 25, 14, 30, 45))
        '2023-12-25T14:30:45'
        >>> from decimal import Decimal
        >>> serialize_value(Decimal('123.456'))
        123.456
    """
    from datetime import date, datetime, time

    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    # Handle Decimal if present (convert to float)
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)

    return value


def serialize_document(doc: dict) -> dict:
    """Serialize all values in a document dictionary.

    Recursively serializes all values in a document, converting datetime objects
    and other non-serializable types to JSON-serializable formats.

    Args:
        doc: Document dictionary to serialize

    Returns:
        Dictionary with all values serialized

    Example:
        >>> from datetime import datetime
        >>> doc = {"id": 1, "created_at": datetime.now(), "name": "test"}
        >>> serialized = serialize_document(doc)
        >>> assert isinstance(serialized["created_at"], str)
    """
    if not isinstance(doc, dict):
        return serialize_value(doc)

    serialized = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            # Recursively serialize nested dictionaries
            serialized[key] = serialize_document(value)
        elif isinstance(value, list):
            # Serialize each item in the list
            serialized[key] = [serialize_value(item) for item in value]
        else:
            serialized[key] = serialize_value(value)

    return serialized


def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code.

    This function is designed to be used as the `default` parameter for json.dumps().
    It handles datetime, date, time, and Decimal objects by converting them to
    JSON-serializable types.

    Args:
        obj: Object to serialize

    Returns:
        JSON-serializable representation

    Raises:
        TypeError: If the value type is not serializable

    Example:
        >>> import json
        >>> from datetime import datetime
        >>> data = {"id": 1, "created_at": datetime.now()}
        >>> json.dumps(data, default=json_serializer)
        '{"id": 1, "created_at": "2023-12-25T14:30:45"}'
    """
    serialized = serialize_value(obj)
    # If serialize_value didn't change the object, it's not a type we handle
    # Check if it's a type that json.dumps can't handle by default
    if serialized is obj and not isinstance(obj, (str, int, float, bool, type(None))):
        # Check if it's a container type that json can handle
        if not isinstance(obj, (list, dict)):
            raise TypeError(f"Type {type(obj)} not serializable")
    return serialized


def load_reserved_words(db_flavor: DBType) -> set[str]:
    """Load reserved words for a given database flavor.

    Args:
        db_flavor: The database flavor to load reserved words for

    Returns:
        Set of reserved words (uppercase) for the database flavor.
        Returns empty set if no reserved words file exists or for unsupported flavors.
    """
    if db_flavor != DBType.TIGERGRAPH:
        # Currently only TigerGraph has reserved words defined
        return set()

    # Load TigerGraph reserved words
    json_path = Path(__file__).parent / "tigergraph" / "reserved_words.json"
    try:
        with open(json_path, "r") as f:
            reserved_data = json.load(f)
    except FileNotFoundError:
        logger.warning(
            f"Could not find reserved_words.json at {json_path}, "
            f"no reserved word sanitization will be performed"
        )
        return set()
    except json.JSONDecodeError as e:
        logger.warning(
            f"Could not parse reserved_words.json: {e}, "
            f"no reserved word sanitization will be performed"
        )
        return set()

    reserved_words = set()
    reserved_words.update(
        reserved_data.get("reserved_words", {}).get("gsql_keywords", [])
    )
    reserved_words.update(
        reserved_data.get("reserved_words", {}).get("cpp_keywords", [])
    )

    # Return uppercase set for case-insensitive comparison
    return {word.upper() for word in reserved_words}


def sanitize_attribute_name(
    name: str, reserved_words: set[str], suffix: str = "_attr"
) -> str:
    """Sanitize an attribute name to avoid reserved words.

    This function deterministically replaces reserved attribute names with
    modified versions. The algorithm:
    1. Checks if the name (case-insensitive) is in the reserved words set
    2. If reserved, appends a suffix (default: "_attr")
    3. If the modified name is still reserved, appends a numeric suffix
       incrementally until a non-reserved name is found

    The algorithm is deterministic: the same input always produces the same output.

    Args:
        name: The attribute name to sanitize
        reserved_words: Set of reserved words (uppercase) to avoid
        suffix: Suffix to append if name is reserved (default: "_attr")

    Returns:
        Sanitized attribute name that is not in the reserved words set

    Examples:
        >>> reserved = {"SELECT", "FROM", "WHERE"}
        >>> sanitize_attribute_name("name", reserved)
        'name'
        >>> sanitize_attribute_name("SELECT", reserved)
        'SELECT_attr'
        >>> sanitize_attribute_name("SELECT_attr", reserved)
        'SELECT_attr_1'
    """
    if not name:
        return name

    if not reserved_words:
        return name

    name_upper = name.upper()

    # If name is not reserved, return as-is
    if name_upper not in reserved_words:
        return name

    # Name is reserved, try appending suffix
    candidate = f"{name}{suffix}"
    candidate_upper = candidate.upper()

    # If candidate is not reserved, use it
    if candidate_upper not in reserved_words:
        return candidate

    # Candidate is also reserved, append numeric suffix
    counter = 1
    while True:
        candidate = f"{name}{suffix}_{counter}"
        candidate_upper = candidate.upper()
        if candidate_upper not in reserved_words:
            return candidate
        counter += 1
        # Safety check to avoid infinite loop (should never happen in practice)
        if counter > 1000:
            logger.warning(
                f"Could not find non-reserved name for '{name}' after 1000 attempts, "
                f"returning '{candidate}'"
            )
            return candidate
