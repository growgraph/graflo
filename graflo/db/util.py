"""Database cursor utilities for graph operations.

This module provides utility functions for working with database cursors,
particularly for handling batch data retrieval and cursor iteration.

Key Functions:
    - get_data_from_cursor: Retrieve data from a cursor with optional limit

Example:
    >>> cursor = db.execute("FOR doc IN collection RETURN doc")
    >>> batch = get_data_from_cursor(cursor, limit=100)
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
