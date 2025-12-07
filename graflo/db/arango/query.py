"""ArangoDB query utilities for graph operations.

This module provides utility functions for executing and profiling AQL queries
in ArangoDB. It includes functions for basic query execution, query profiling,
and field fetching operations.

Key Functions:
    - basic_query: Execute a basic AQL query with configurable parameters
    - profile_query: Profile query execution and save results
    - fetch_fields_query: Generate and execute field-fetching queries

Example:
    >>> cursor = basic_query("FOR doc IN users RETURN doc", db_name="mydb")
    >>> profile_query("FOR doc IN users RETURN doc", nq=1, profile_times=3, fpath=".")
"""

import gzip
import json
import logging
from os.path import join

from arango import ArangoClient

from graflo.filter.onto import Expression
from graflo.onto import DBFlavor

logger = logging.getLogger(__name__)


def basic_query(
    query,
    port=8529,
    hostname="127.0.0.1",
    cred_name="root",
    cred_pass="123",
    db_name="_system",
    profile=False,
    batch_size=10000,
    bind_vars=None,
):
    """Execute a basic AQL query in ArangoDB.

    This function provides a simple interface for executing AQL queries with
    configurable connection parameters and query options.

    Args:
        query: AQL query string to execute
        port: ArangoDB server port
        hostname: ArangoDB server hostname
        cred_name: Database username
        cred_pass: Database password
        db_name: Database name
        profile: Whether to enable query profiling
        batch_size: Size of result batches
        bind_vars: Query bind variables

    Returns:
        Cursor: ArangoDB cursor for the query results
    """
    hosts = f"http://{hostname}:{port}"
    client = ArangoClient(hosts=hosts)

    sys_db = client.db(db_name, username=cred_name, password=cred_pass)
    cursor = sys_db.aql.execute(
        query,
        profile=profile,
        stream=True,
        batch_size=batch_size,
        bind_vars=bind_vars,
    )
    return cursor


def profile_query(query, nq, profile_times, fpath, limit=None, **kwargs):
    """Profile AQL query execution and save results.

    This function executes a query multiple times with profiling enabled and
    saves both the profiling results and query results to files.

    Args:
        query: AQL query string to profile
        nq: Query number for file naming
        profile_times: Number of times to profile the query
        fpath: Path to save results
        limit: Optional limit on query results
        **kwargs: Additional query parameters passed to basic_query

    Note:
        Results are saved in two formats:
        - Profiling results: query{nq}_profile{limit}.json
        - Query results: query{nq}_result{limit}_batch_{n}.json.gz
    """
    limit_str = f"_limit_{limit}" if limit else ""
    if profile_times:
        logger.info(f"starting profiling: {limit}")
        profiling = []
        for n in range(profile_times):
            cursor = basic_query(query, profile=True, **kwargs)
            profiling += [cursor.profile()]
            cursor.close()
        with open(join(fpath, f"query{nq}_profile{limit_str}.json"), "w") as fp:
            json.dump(profiling, fp, indent=4)

    logger.info(f"starting actual query at {limit}")

    cnt = 0
    cursor = basic_query(query, **kwargs)
    chunk = list(cursor.batch())
    with gzip.open(
        join(fpath, f"./query{nq}_result{limit_str}_batch_{cnt}.json.gz"),
        "wt",
        encoding="ascii",
    ) as fp:
        json.dump(chunk, fp, indent=4)

    while cursor.has_more():
        cnt += 1
        with gzip.open(
            join(fpath, f"./query{nq}_result{limit_str}_batch_{cnt}.json.gz"),
            "wt",
            encoding="ascii",
        ) as fp:
            chunk = list(cursor.fetch()["batch"])
            json.dump(chunk, fp, indent=4)


def fetch_fields_query(
    collection_name,
    docs,
    match_keys,
    keep_keys,
    filters: list | dict | None = None,
):
    """Generate and execute a field-fetching AQL query.

    This function generates an AQL query to fetch specific fields from documents
    that match the given criteria. It supports filtering and field projection.

    Args:
        collection_name: Collection to query
        docs: List of documents to match against
        match_keys: Keys to use for matching documents
        keep_keys: Keys to return in the result
        filters: Additional query filters

    Returns:
        str: Generated AQL query string

    Example:
        >>> query = fetch_fields_query(
        ...     "users",
        ...     [{"email": "user@example.com"}],
        ...     ["email"],
        ...     ["name", "age"]
        ... )
    """
    docs_ = [{k: doc[k] for k in match_keys if k in doc} for doc in docs]
    for i, doc in enumerate(docs_):
        doc.update({"__i": i})

    docs_str = json.dumps(docs_)

    match_str = " &&".join([f" _cdoc['{key}'] == _doc['{key}']" for key in match_keys])

    keep_clause = f"KEEP(_x, {list(keep_keys)})" if keep_keys is not None else "_x"

    if filters is not None:
        ff = Expression.from_dict(filters)
        extrac_filter_clause = f" && {ff(doc_name='_cdoc', kind=DBFlavor.ARANGO)}"
    else:
        extrac_filter_clause = ""

    q0 = f"""
        FOR _cdoc in {collection_name}
            FOR _doc in {docs_str}
                FILTER {match_str} {extrac_filter_clause}      
                COLLECT i = _doc['__i'] into _group = _cdoc 
                LET gp = (for _x in _group return {keep_clause})                                
                    RETURN {{'__i' : i, '_group': gp}}"""
    return q0
