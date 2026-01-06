"""Database utilities for graph operations.

This module provides utility functions for working with database operations,
including cursor handling and data serialization.

Key Functions:
    - get_data_from_cursor: Retrieve data from a cursor with optional limit
    - serialize_value: Serialize non-serializable values (datetime, Decimal, etc.)
    - serialize_document: Serialize all values in a document dictionary

Example:
    >>> # ArangoDB-specific AQL query (collection is ArangoDB terminology)
    >>> cursor = db.execute("FOR doc IN vertex_class RETURN doc")
    >>> batch = get_data_from_cursor(cursor, limit=100)
    >>> # Serialize datetime objects in a document
    >>> doc = {"id": 1, "created_at": datetime.now()}
    >>> serialized = serialize_document(doc)
"""

from arango.exceptions import CursorNextError


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
