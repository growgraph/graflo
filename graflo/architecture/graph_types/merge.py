"""Document fusion: several observations of one entity becoming one document.

This is the *fuse* plane -- two records becoming one node -- and not either
schema-level sense of "merge". Nothing here folds two declarations of a type;
it folds the documents a pipeline emitted for the same entity, keyed on the
fields they will be matched on, preserving order and handling both dict and
:class:`VertexRep` objects.

Key Functions:
    - fuse_doc_basis: fold documents sharing an index key, preserving order

"""

from collections.abc import Collection
from typing import Any, cast, overload

from graflo.architecture.graph_types.transform import VertexRep


@overload
def fuse_doc_basis(
    docs: list[dict],
    index_keys: tuple[str, ...],
    union_fields: Collection[str] = (),
) -> list[dict]: ...


@overload
def fuse_doc_basis(
    docs: list[VertexRep],
    index_keys: tuple[str, ...],
    union_fields: Collection[str] = (),
) -> list[VertexRep]: ...


def fuse_doc_basis(
    docs: list[dict] | list[VertexRep],
    index_keys: tuple[str, ...],
    union_fields: Collection[str] = (),
) -> list[dict] | list[VertexRep]:
    """Merge documents based on common index keys, preserving order.

    This function merges documents that share common index key-value combinations,
    preserving the order of documents based on the first occurrence of each index
    key combination. Documents without index keys are merged into the closest
    preceding document with index keys. If no documents have index keys, all
    documents are merged into a single document.

    For VertexRep objects, the merge is performed on the `vertex` attribute.

    VertexRep objects fuse only with reps carrying the same observation tags
    (``lookup_only``, ``find``): each tag set is fused on its own, and the
    groups follow one another in the order of their first rep. Fusing across
    tags would put writable properties under a reference, or the reverse.

    Args:
        docs: Homogeneous list of documents (all dict or all VertexRep) to merge
        index_keys: Tuple of key names to use for merging
        union_fields: List fields fused as the union of every document's
            members, in first-seen order; every other field is later-wins.

    Returns:
        Merged documents in order of first occurrence (same type as input)
    """
    if not docs:
        return docs
    if (
        type(docs) is list
        and type(docs[0]) is dict
        and all(type(doc) is dict for doc in docs)
    ):
        return _fuse_dicts(cast(list[dict], docs), index_keys, union_fields)
    if isinstance(docs[0], VertexRep):
        groups: dict[tuple[bool, str | None], list[VertexRep]] = {}
        for rep in docs:
            if not isinstance(rep, VertexRep):
                raise TypeError(f"expected VertexRep, got {type(rep).__name__}")
            groups.setdefault((rep.lookup_only, rep.find), []).append(rep)
        fused: list[VertexRep] = []
        for group in groups.values():
            fused.extend(
                cast(list[VertexRep], _fuse_group(group, index_keys, union_fields))
            )
        return fused
    return _fuse_group(docs, index_keys, union_fields)


def _fuse_dicts(
    docs: list[dict],
    index_keys: tuple[str, ...],
    union_fields: Collection[str],
) -> list[dict]:
    """Fuse ordinary dictionaries without per-call closures or rep dispatch.

    Only string key names let us queue empty fragments directly: other keys may
    raise or have observable hash behaviour even when tested against ``{}``.
    Keep the fragments until merging, since later key callbacks can fill them.
    The nonempty observations still use the same key construction and lookup
    order as the general path, including rejection of unhashable identities.
    """
    plain_index_keys = type(index_keys) is tuple and all(
        type(key) is str for key in index_keys
    )
    merged: list[dict] = []
    positions: dict[tuple, int] = {}
    pending: list[dict] = []
    for doc in docs:
        # A user-defined identity hash can mutate the input list mid-iteration.
        # Retain per-observation checks, rather than trusting the initial scan.
        if not isinstance(doc, dict):
            raise TypeError(f"expected dict, got {type(doc).__name__}")
        if plain_index_keys and type(doc) is dict and not doc:
            pending.append(doc)
            continue
        if any(key in doc for key in index_keys):
            index = tuple(sorted((k, v) for k, v in doc.items() if k in index_keys))
            if pending:
                target = merged[-1] if merged else doc
                for fragment in pending:
                    if type(target) is dict and type(fragment) is dict and not fragment:
                        continue
                    _fuse_doc(target, fragment, False, union_fields)
                pending.clear()
            if index in positions:
                _fuse_doc(merged[positions[index]], doc, False, union_fields)
            else:
                merged.append(doc.copy())
                positions[index] = len(merged) - 1
        else:
            pending.append(doc)
    if pending:
        if merged:
            target = merged[-1]
        else:
            target = {}
            merged.append(target)
        for fragment in pending:
            if type(target) is dict and type(fragment) is dict and not fragment:
                continue
            _fuse_doc(target, fragment, False, union_fields)
    return merged


def _union_update(
    target: dict[str, Any], source: dict[str, Any], union_fields: Collection[str]
) -> None:
    """``target.update(source)``, but *union_fields* keep both sides' members."""
    for key, value in source.items():
        if key in union_fields and isinstance(value, list):
            held = target.get(key)
            members = [*(held if isinstance(held, list) else []), *value]
            target[key] = list(dict.fromkeys(members))
        else:
            target[key] = value


def _get_index_tuple(
    doc: dict | VertexRep, is_vertexrep: bool, index_keys: tuple[str, ...]
) -> tuple:
    """Extract index tuple from a document."""
    if is_vertexrep:
        if not isinstance(doc, VertexRep):
            raise TypeError(f"expected VertexRep, got {type(doc).__name__}")
        data = doc.vertex
    else:
        if not isinstance(doc, dict):
            raise TypeError(f"expected dict, got {type(doc).__name__}")
        data = doc
    return tuple(sorted((k, v) for k, v in data.items() if k in index_keys))


def _has_index_keys(
    doc: dict | VertexRep, is_vertexrep: bool, index_keys: tuple[str, ...]
) -> bool:
    """Check if document has any index keys."""
    if is_vertexrep:
        if not isinstance(doc, VertexRep):
            raise TypeError(f"expected VertexRep, got {type(doc).__name__}")
        return any(k in doc.vertex for k in index_keys)
    else:
        if not isinstance(doc, dict):
            raise TypeError(f"expected dict, got {type(doc).__name__}")
        return any(k in doc for k in index_keys)


def _fuse_doc(
    target: dict | VertexRep,
    source: dict | VertexRep,
    is_vertexrep: bool,
    union_fields: Collection[str],
) -> None:
    """Merge source into target."""
    if is_vertexrep:
        if not (isinstance(target, VertexRep) and isinstance(source, VertexRep)):
            raise TypeError("expected VertexRep for both target and source")
        _union_update(target.vertex, source.vertex, union_fields)
    else:
        if not (isinstance(target, dict) and isinstance(source, dict)):
            raise TypeError("expected dict for both target and source")
        _union_update(target, source, union_fields)


def _copy_doc(doc: dict | VertexRep, is_vertexrep: bool) -> dict | VertexRep:
    """Create a copy of a document."""
    if is_vertexrep:
        if not isinstance(doc, VertexRep):
            raise TypeError(f"expected VertexRep, got {type(doc).__name__}")
        # Copy via the model so observation-level flags (lookup_only) are
        # carried over rather than silently reset to their defaults.
        return doc.model_copy(update={"vertex": doc.vertex.copy()})
    else:
        if not isinstance(doc, dict):
            raise TypeError(f"expected dict, got {type(doc).__name__}")
        return doc.copy()


def _fuse_group(
    docs: list[dict] | list[VertexRep],
    index_keys: tuple[str, ...],
    union_fields: Collection[str] = (),
) -> list[dict] | list[VertexRep]:
    """:func:`fuse_doc_basis` over documents that may all fuse with one another."""
    # Check if we're working with VertexRep objects
    is_vertexrep = isinstance(docs[0], VertexRep)

    # Track merged documents in order of first occurrence
    # Type: list[dict] if not is_vertexrep, list[VertexRep] if is_vertexrep
    merged_docs: list[dict | VertexRep] = []
    # Map from index tuple to position in merged_docs
    index_to_position: dict[tuple, int] = {}
    # Accumulate documents without index keys
    # Type: list[dict] if not is_vertexrep, list[VertexRep] if is_vertexrep
    pending_non_ids: list[dict | VertexRep] = []

    for doc in docs:
        if _has_index_keys(doc, is_vertexrep, index_keys):
            # This is a document with index keys
            index_tuple = _get_index_tuple(doc, is_vertexrep, index_keys)

            # First, handle any accumulated non-ID documents
            if pending_non_ids:
                if merged_docs:
                    # Merge accumulated non-IDs into the last ID doc
                    for pending in pending_non_ids:
                        _fuse_doc(merged_docs[-1], pending, is_vertexrep, union_fields)
                else:
                    # No previous ID doc, merge pending non-IDs into the current ID doc
                    for pending in pending_non_ids:
                        _fuse_doc(doc, pending, is_vertexrep, union_fields)
                pending_non_ids.clear()

            # Handle the current document with index keys
            if index_tuple in index_to_position:
                # Merge into existing document at that position
                _fuse_doc(
                    merged_docs[index_to_position[index_tuple]],
                    doc,
                    is_vertexrep,
                    union_fields,
                )
            else:
                # First occurrence of this index tuple, add new document
                merged_docs.append(_copy_doc(doc, is_vertexrep))
                index_to_position[index_tuple] = len(merged_docs) - 1
        else:
            # This is a document without index keys, accumulate it
            pending_non_ids.append(doc)

    # Handle any remaining non-ID documents at the end
    if pending_non_ids and merged_docs:
        # Merge into last ID doc
        for pending in pending_non_ids:
            _fuse_doc(merged_docs[-1], pending, is_vertexrep, union_fields)
    elif pending_non_ids:
        # No documents with index keys: merge all into a single document
        if is_vertexrep:
            # Keep the group's tags (lookup_only, find): a fresh rep would
            # turn a reference or an attached row into a write.
            first = pending_non_ids[0]
            if not isinstance(first, VertexRep):
                raise TypeError(f"expected VertexRep, got {type(first).__name__}")
            merged_doc = first.model_copy(update={"vertex": {}})
        else:
            merged_doc = {}
        for pending in pending_non_ids:
            _fuse_doc(merged_doc, pending, is_vertexrep, union_fields)
        merged_docs.append(merged_doc)

    # Type narrowing: return type matches input type due to homogeneous list requirement
    return cast(list[dict] | list[VertexRep], merged_docs)
