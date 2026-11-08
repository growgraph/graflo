"""Document fusion: several observations of one entity becoming one document.

This is the *fuse* plane -- two records becoming one node -- and not either
schema-level sense of "merge". Nothing here folds two declarations of a type;
it folds the documents a pipeline emitted for the same entity, keyed on the
fields they will be matched on, preserving order and handling both dict and
:class:`VertexRep` objects.

Key Functions:
    - fuse_doc_basis: fold documents sharing an index key, preserving order
    - union_members: the members a ``merge: union`` field holds after an upsert

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
        union_fields: List fields fused as :func:`union_members` of every
            document's value, the first included; every other field is
            later-wins.

    Returns:
        Merged documents in order of first occurrence (same type as input)
    """
    if not docs:
        return docs
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


def union_members(stored: Any, written: Any) -> list[Any]:
    """What a ``merge: union`` field holds once *written* is upserted onto *stored*.

    Stored members first, then new ones, each once, in first-seen order. Null
    members are dropped, a null value adds nothing and a scalar is one member:
    the union every backend that writes one applies.
    """
    out: list[Any] = []
    for value in (stored, written):
        if value is None:
            continue
        for member in value if isinstance(value, list) else [value]:
            if member is not None and member not in out:
                out.append(member)
    return out


def _union_update(
    target: dict[str, Any], source: dict[str, Any], union_fields: Collection[str]
) -> None:
    """``target.update(source)``, but *union_fields* take :func:`union_members`."""
    for key, value in source.items():
        if key in union_fields:
            target[key] = union_members(target.get(key), value)
        else:
            target[key] = value


def _union_first(doc: dict[str, Any], union_fields: Collection[str]) -> dict[str, Any]:
    """*doc*, its union fields already as an upsert onto nothing leaves them."""
    for key in union_fields:
        if key in doc:
            doc[key] = union_members(None, doc[key])
    return doc


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

    def get_index_tuple(doc: dict | VertexRep) -> tuple:
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

    def has_index_keys(doc: dict | VertexRep) -> bool:
        """Check if document has any index keys."""
        if is_vertexrep:
            if not isinstance(doc, VertexRep):
                raise TypeError(f"expected VertexRep, got {type(doc).__name__}")
            return any(k in doc.vertex for k in index_keys)
        else:
            if not isinstance(doc, dict):
                raise TypeError(f"expected dict, got {type(doc).__name__}")
            return any(k in doc for k in index_keys)

    def fuse_doc(target: dict | VertexRep, source: dict | VertexRep) -> None:
        """Merge source into target."""
        if is_vertexrep:
            if not (isinstance(target, VertexRep) and isinstance(source, VertexRep)):
                raise TypeError("expected VertexRep for both target and source")
            _union_update(target.vertex, source.vertex, union_fields)
        else:
            if not (isinstance(target, dict) and isinstance(source, dict)):
                raise TypeError("expected dict for both target and source")
            _union_update(target, source, union_fields)

    def copy_doc(doc: dict | VertexRep) -> dict | VertexRep:
        """Create a copy of a document."""
        if is_vertexrep:
            if not isinstance(doc, VertexRep):
                raise TypeError(f"expected VertexRep, got {type(doc).__name__}")
            # Copy via the model so observation-level flags (lookup_only) are
            # carried over rather than silently reset to their defaults.
            vertex = _union_first(doc.vertex.copy(), union_fields)
            return doc.model_copy(update={"vertex": vertex})
        else:
            if not isinstance(doc, dict):
                raise TypeError(f"expected dict, got {type(doc).__name__}")
            return _union_first(doc.copy(), union_fields)

    for doc in docs:
        if has_index_keys(doc):
            # This is a document with index keys
            index_tuple = get_index_tuple(doc)

            # First, handle any accumulated non-ID documents
            if pending_non_ids:
                if merged_docs:
                    # Merge accumulated non-IDs into the last ID doc
                    for pending in pending_non_ids:
                        fuse_doc(merged_docs[-1], pending)
                else:
                    # No previous ID doc, merge pending non-IDs into the current ID doc
                    for pending in pending_non_ids:
                        fuse_doc(doc, pending)
                pending_non_ids.clear()

            # Handle the current document with index keys
            if index_tuple in index_to_position:
                # Merge into existing document at that position
                fuse_doc(merged_docs[index_to_position[index_tuple]], doc)
            else:
                # First occurrence of this index tuple, add new document
                merged_docs.append(copy_doc(doc))
                index_to_position[index_tuple] = len(merged_docs) - 1
        else:
            # This is a document without index keys, accumulate it
            pending_non_ids.append(doc)

    # Handle any remaining non-ID documents at the end
    if pending_non_ids and merged_docs:
        # Merge into last ID doc
        for pending in pending_non_ids:
            fuse_doc(merged_docs[-1], pending)
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
            fuse_doc(merged_doc, pending)
        merged_docs.append(merged_doc)

    # Type narrowing: return type matches input type due to homogeneous list requirement
    return cast(list[dict] | list[VertexRep], merged_docs)
