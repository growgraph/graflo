"""The sync aspect: a record's sync hash on the vertex types it owns.

The lift adds one reserved field, ``sync_hash``, ``set_by: sync``, to each type a
resource's ``sync`` block owns. A bulk load and a sync run write it; an upsert
sets it to null, so the next sync rewrites the record.
"""

from __future__ import annotations

from .plan import SYNC_HASH, plan_sync, sync_field

__all__ = ["SYNC_HASH", "plan_sync", "sync_field"]
