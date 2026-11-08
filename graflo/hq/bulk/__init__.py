"""Bulk load: cast and stage a whole input, fold it by identity, write it once.

A bulk run loads an empty target. Every vertex and edge is folded to one row
under the upsert merge rules before anything is written, so each backend needs
only a create-only write. The fold runs in DuckDB, from the optional ``bulk``
extra; importing this package does not load it.
"""
