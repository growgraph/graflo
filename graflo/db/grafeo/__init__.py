"""Grafeo: an embedded graph database, queried in Cypher inside this process.

Install with ``graflo[grafeo]``.
"""

from graflo.db.grafeo.conn import GrafeoConnection
from graflo.db.grafeo.store import close_database

__all__ = ["GrafeoConnection", "close_database"]
