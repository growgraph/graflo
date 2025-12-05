"""Document merging utilities.

This module provides functions for merging documents based on common index keys,
preserving order and handling both dict and VertexRep objects.

Key Functions:
    - merge_doc_basis: Merge documents based on common index keys, preserving order

"""

from typing import cast, overload

from graflo.architecture.onto import VertexRep


@overload
def merge_doc_basis(
    docs: list[dict],
    index_keys: tuple[str, ...],
) -> list[dict]: ...


@overload
def merge_doc_basis(
    docs: list[VertexRep],
    index_keys: tuple[str, ...],
) -> list[VertexRep]: ...


def merge_doc_basis(
    docs: list[dict] | list[VertexRep],
    index_keys: tuple[str, ...],
) -> list[dict] | list[VertexRep]:
    """Merge documents based on common index keys, preserving order.

    This function merges documents that share common index key-value combinations,
    preserving the order of documents based on the first occurrence of each index
    key combination. Documents without index keys are merged into the closest
    preceding document with index keys. If no documents have index keys, all
    documents are merged into a single document.

    For VertexRep objects, the merge is performed on the `vertex` attribute, and
    `ctx` dicts are merged among merged VertexReps.

    Args:
        docs: Homogeneous list of documents (all dict or all VertexRep) to merge
        index_keys: Tuple of key names to use for merging

    Returns:
        Merged documents in order of first occurrence (same type as input)
    """
    if not docs:
        return docs

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
            assert isinstance(doc, VertexRep)
            data = doc.vertex
        else:
            assert isinstance(doc, dict)
            data = doc
        return tuple(sorted((k, v) for k, v in data.items() if k in index_keys))

    def has_index_keys(doc: dict | VertexRep) -> bool:
        """Check if document has any index keys."""
        if is_vertexrep:
            assert isinstance(doc, VertexRep)
            return any(k in doc.vertex for k in index_keys)
        else:
            assert isinstance(doc, dict)
            return any(k in doc for k in index_keys)

    def merge_doc(target: dict | VertexRep, source: dict | VertexRep) -> None:
        """Merge source into target."""
        if is_vertexrep:
            assert isinstance(target, VertexRep) and isinstance(source, VertexRep)
            target.vertex.update(source.vertex)
            target.ctx.update(source.ctx)
        else:
            assert isinstance(target, dict) and isinstance(source, dict)
            target.update(source)

    def copy_doc(doc: dict | VertexRep) -> dict | VertexRep:
        """Create a copy of a document."""
        if is_vertexrep:
            assert isinstance(doc, VertexRep)
            return VertexRep(vertex=doc.vertex.copy(), ctx=doc.ctx.copy())
        else:
            assert isinstance(doc, dict)
            return doc.copy()

    for doc in docs:
        if has_index_keys(doc):
            # This is a document with index keys
            index_tuple = get_index_tuple(doc)

            # First, handle any accumulated non-ID documents
            if pending_non_ids:
                if merged_docs:
                    # Merge accumulated non-IDs into the last ID doc
                    for pending in pending_non_ids:
                        merge_doc(merged_docs[-1], pending)
                else:
                    # No previous ID doc, merge pending non-IDs into the current ID doc
                    for pending in pending_non_ids:
                        merge_doc(doc, pending)
                pending_non_ids.clear()

            # Handle the current document with index keys
            if index_tuple in index_to_position:
                # Merge into existing document at that position
                merge_doc(merged_docs[index_to_position[index_tuple]], doc)
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
            merge_doc(merged_docs[-1], pending)
    elif pending_non_ids:
        # No documents with index keys: merge all into a single document
        if is_vertexrep:
            merged_doc = VertexRep(vertex={}, ctx={})
        else:
            merged_doc = {}
        for pending in pending_non_ids:
            merge_doc(merged_doc, pending)
        merged_docs.append(merged_doc)

    # Type narrowing: return type matches input type due to homogeneous list requirement
    return cast(list[dict] | list[VertexRep], merged_docs)
