"""Graph runtime enumerations."""

from __future__ import annotations

from graflo.onto import BaseEnum


class EdgeMapping(BaseEnum):
    """Defines how edges are mapped between vertices.

    ALL: Maps all vertices to all vertices
    ONE_N: Maps one vertex to many vertices
    """

    ALL = "all"
    ONE_N = "1-n"


class EncodingType(BaseEnum):
    """Supported character encodings for data input/output."""

    ISO_8859 = "ISO-8859-1"
    UTF_8 = "utf-8"


class IndexType(BaseEnum):
    """Types of database indexes supported.

    PERSISTENT: Standard persistent index
    HASH: Hash-based index for fast lookups
    SKIPLIST: Sorted index using skip list data structure
    FULLTEXT: Index optimized for text search
    """

    PERSISTENT = "persistent"
    HASH = "hash"
    SKIPLIST = "skiplist"
    FULLTEXT = "fulltext"


class EdgeType(BaseEnum):
    """Defines how edges are handled in the graph database.

    INDIRECT: Uses pre-existing DB structures and may be used after data ingestion
    DIRECT: Generated during ingestion from resource pipelines
    """

    INDIRECT = "indirect"
    DIRECT = "direct"


class EdgeCastingType(BaseEnum):
    """Types of edge casting supported.

    PAIR: Edges are cast as pairs of vertices
    PRODUCT: Edges are cast as combinations of vertex sets
    """

    PAIR = "pair"
    PRODUCT = "product"
    COMBINATIONS = "combinations"
