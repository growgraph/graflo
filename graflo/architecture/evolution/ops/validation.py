"""Checks shared by rename, merge, and vocabulary maps."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


def validate_rename_map_is_injective(
    renames: dict[str, str], *, kind: str, merge_hint: str
) -> None:
    """Reject a rename map that would collapse two names onto one.

    A rename is a relabelling: it must not change how many types exist. Two sources
    sharing a target is a *merge*, and the merge ops exist precisely because merging
    needs decisions a rename cannot express — which properties survive, how identity
    combines, what happens to edges that become self-loops. Left unchecked the
    collapse is silent: the name-keyed lookup maps in ``VertexConfig`` / ``EdgeConfig``
    keep the last definition and the earlier one is shadowed but still serialized.
    """
    collisions: dict[str, list[str]] = {}
    for source, target in renames.items():
        collisions.setdefault(target, []).append(source)
    collapsed = {
        target: sorted(sources)
        for target, sources in collisions.items()
        if len(sources) > 1
    }
    if collapsed:
        detail = "; ".join(
            f"{target!r} is the target of {sources}"
            for target, sources in sorted(collapsed.items())
        )
        raise ValueError(
            f"{kind} rename map is not injective: {detail}. "
            f"Renaming cannot merge types — use {merge_hint}."
        )


def validate_merge_sources(sources: Sequence[str], into: str, *, kind: str) -> None:
    """Reject a merge whose sources repeat or include the target.

    The docstrings promise both; enforcing them at parse time means a
    serialized change set fails where it is read rather than where it is
    replayed, after the ops before it have already been applied.
    """
    if len(set(sources)) != len(sources):
        raise ValueError(f"{kind}: sources list a name more than once: {list(sources)}")
    if into in sources:
        raise ValueError(f"{kind}: `into` {into!r} must not appear in `sources`")


def vocabulary_groups(mapping: Mapping[str, str]) -> dict[str, list[str]]:
    """``{target: [sources]}`` — the fibers of a vocabulary map, self entries included."""
    groups: dict[str, list[str]] = {}
    for source, target in mapping.items():
        groups.setdefault(target, []).append(source)
    return groups


def validate_vocabulary_map(
    vertices: Mapping[str, str],
    relations: Mapping[str, str],
    properties: Mapping[str, Mapping[str, str]],
    *,
    allow_merges: bool,
    kind: str,
    merge_hint: str,
) -> None:
    """Reject an unacknowledged collapse in a vocabulary map.

    A vocabulary map is a function on names, so its groups are its fibers:
    every source of one target, **including** a self entry ``t: t`` that
    declares an existing ``t`` a member of its own group. A group of more than
    one name is a merge — it fuses entities and can create self-relations — so
    it must be acknowledged rather than inferred from the map. Per-class
    attribute maps are plain renames and must be injective outright.
    """
    if not allow_merges:
        for noun, mapping in (("class", vertices), ("relation", relations)):
            collapsed = {
                target: sorted(sources)
                for target, sources in vocabulary_groups(mapping).items()
                if len(sources) > 1
            }
            if collapsed:
                detail = "; ".join(
                    f"{target!r} is the target of {sources}"
                    for target, sources in sorted(collapsed.items())
                )
                raise ValueError(
                    f"{kind}: {noun} map collapses names: {detail}. A merge is a "
                    f"stated intent — {merge_hint}."
                )
    for source_class, attr_map in properties.items():
        validate_rename_map_is_injective(
            dict(attr_map),
            kind=f"{kind} property (class {source_class!r})",
            merge_hint="a transform that combines the fields upstream",
        )


def validate_vocabulary_is_idempotent(mapping: Mapping[str, str], *, kind: str) -> None:
    """Reject a chain or a swap: a canonical vocabulary has fixed points.

    A name that is both a source that moves and a target of another entry
    (``{X: Z, Z: Q}``, ``{A: B, B: A}``) makes the map non-idempotent — applying
    it twice is not applying it once — so it cannot be read as a vocabulary,
    where a canonical name is by definition one nothing maps away from. Such a
    map is a *relabel*, which :class:`CanonicalizeOp` expresses (simultaneous
    application over the original schema).
    """
    moving = {source for source, target in mapping.items() if source != target}
    targets = {target for source, target in mapping.items() if source != target}
    chained = sorted(moving & targets)
    if chained:
        raise ValueError(
            f"{kind}: {chained} are both a source that moves and a canonical "
            "target. A canonical vocabulary has fixed points — a chain or a "
            "swap is a relabel, which CanonicalizeOp expresses."
        )
