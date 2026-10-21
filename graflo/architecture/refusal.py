"""The base class every refusal shares, and the registry of refusal kinds.

A refusal carries two structured fields beside its prose. ``check`` names the
rule that refused, as a stable id from :data:`REFUSAL_KINDS`, and ``subjects``
the names it is about, such as
:func:`~graflo.architecture.evolution.equivalence.subject` ids (``left:Firm``,
``left:Firm.firm_id``) or the vertex, relation and resource names an op
precondition names. Neither appears in the message, so a caller that only
reads ``str(exc)`` sees the same prose whatever the kind.

The two fields are what lets a caller classify a refusal without parsing its
prose: :mod:`graflo.architecture.evolution.preview` keys its finding kinds on
``check``, and a conformance run compares refusals by it.

This module sits at the bottom of the import order deliberately. The schema
layer raises refusals of its own, so the base cannot live beside the merge
machinery that consumes them.
"""

from __future__ import annotations

from typing import Literal, TypeGuard

#: Every refusal kind, by stable id. Ids are never reused or renamed; a
#: vertex/relation scope is carried by ``subjects``, not by the id.
RefusalKind = Literal[
    # ── op preconditions ─────────────────────────────────────────────────────
    "missing-block",
    "unknown-vertex",
    "unknown-edge",
    "unknown-relation",
    "unknown-property",
    "unknown-resource",
    "unknown-transform",
    "unknown-index",
    "unknown-secondary-identity",
    "unknown-inverse",
    "already-exists",
    "name-collision",
    "identity-field",
    "still-referenced",
    "definition-conflict",
    "pipeline-conflict",
    "self-relation",
    "invalid-result",
    "not-applicable",
    "invalid-op",
    # ── merge: declared names and maps ───────────────────────────────────────
    "cluster-overlap",
    "unknown-member",
    "unknown-key-member",
    "into-disagreement",
    "unnamed-cluster",
    "near-collision",
    "canonical-split",
    "canonical-clash",
    "canonical-retarget",
    "joins-merged-class",
    "shared-into",
    "occupied-into",
    "double-home",
    "dangling-entry",
    "derive-at-disagreement",
    "ambiguous-reference",
    "property-disagreement",
    "property-rename-collision",
    "property-retarget",
    "observation-fusion",
    # ── merge: identity ─────────────────────────────────────────────────────
    "identity-disagreement",
    "identity-coverage",
    "identity-collision",
    "key-space",
    "identifier",
    "origin",
    "identity-mode-conflict",
    "identity-funnel-conflict",
    "secondary-identity-conflict",
    # ── merge: schema union ─────────────────────────────────────────────────
    "edge-type-disagreement",
    "field-type-conflict",
    "field-units-conflict",
    "field-type-declaration",
    # ── query ────────────────────────────────────────────────────────────────
    "reserved-field",
]

#: What each refusal kind means, one line each.
REFUSAL_KINDS: dict[RefusalKind, str] = {
    "missing-block": "the op needs a manifest block (schema, ingestion model) that is absent",
    "unknown-vertex": "a vertex type the op names is not declared",
    "unknown-edge": "an edge (source, target, relation) the op names is not declared",
    "unknown-relation": "a relation the op names is not declared",
    "unknown-property": "a property the op names is not declared on its owner",
    "unknown-resource": "a resource the op names is not declared",
    "unknown-transform": "a transform a step uses is not registered",
    "unknown-index": "an index the op names is not declared",
    "unknown-secondary-identity": "a secondary identity the op names is not declared",
    "unknown-inverse": "a relation the op needs an inverse for has none declared",
    "already-exists": "the op adds something the target already declares",
    "name-collision": "a rename or merge lands on a name that is already taken",
    "identity-field": "the op removes or retypes a field an identity uses",
    "still-referenced": "the op removes something the manifest still uses",
    "definition-conflict": "two definitions of one name disagree",
    "pipeline-conflict": "the rewrite leaves an ingestion pipeline ambiguous",
    "self-relation": "the op turns an edge into a self-relation",
    "invalid-result": "the manifest the op produces fails validation",
    "not-applicable": "the op is well formed but the manifest's state forbids it",
    "invalid-op": "the op is ill formed",
    "cluster-overlap": "one name is a member of two declared clusters",
    "unknown-member": "a declared cluster names a member its side does not declare",
    "unknown-key-member": "a declared key names a field its member does not declare",
    "into-disagreement": "declarations disagree on the name a cluster merges into",
    "unnamed-cluster": "a cluster has no name to merge into",
    "near-collision": "two names differ only in spelling and no declaration settles them",
    "canonical-split": "one canonical name is reached by two unrelated names",
    "canonical-clash": "a canonical map sends one name to two targets",
    "canonical-retarget": "a canonical map moves a name that is also its target",
    "joins-merged-class": "a name would join a class that is already merged",
    "shared-into": "two clusters merge into the same name",
    "occupied-into": "a cluster merges into a name the other side already uses",
    "double-home": "one name is sent to two homes",
    "dangling-entry": "a declared map entry matches nothing",
    "derive-at-disagreement": "declarations disagree on where a key is derived",
    "ambiguous-reference": "a reference resolves to more than one name",
    "property-disagreement": "merged properties disagree on their definition",
    "property-rename-collision": "a property rename lands on an existing property",
    "property-retarget": "a property map moves a property the canonical map placed",
    "observation-fusion": "the merge fuses observations of distinct entities",
    "identity-disagreement": "merged vertices disagree on their identity",
    "identity-coverage": "a merged identity does not cover every member's key",
    "identity-collision": "two distinct entities would get the same identity",
    "key-space": "merged keys come from different key spaces",
    "identifier": "a generated name is too long for the target",
    "origin": "merged entities come from origins that cannot be reconciled",
    "identity-mode-conflict": "merged vertices declare different identity modes",
    "identity-funnel-conflict": "merged vertices declare conflicting identity funnels",
    "secondary-identity-conflict": "merged vertices declare conflicting secondary identities",
    "edge-type-disagreement": "merged edges disagree on their type",
    "field-type-conflict": "merged fields declare different types",
    "field-units-conflict": "merged fields declare different units",
    "field-type-declaration": "a field type declaration names a field that does not exist",
    "reserved-field": "a reserved field name is used",
}


def is_refusal_kind(value: str) -> TypeGuard[RefusalKind]:
    """Whether *value* is a registered refusal kind."""
    return value in REFUSAL_KINDS


class Refusal(ValueError):
    """A declaration that an operation will not carry out.

    Subclasses name the layer that refused -- an op precondition, the cluster
    resolver, the canonical map, the schema union, the field fold. Deriving
    from :class:`ValueError` rather than replacing it keeps every existing
    ``except ValueError`` and ``pytest.raises(ValueError)`` working.
    """

    def __init__(
        self,
        message: str,
        *,
        check: RefusalKind | Literal[""] = "",
        subjects: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.check: RefusalKind | Literal[""] = check
        self.subjects = subjects


class OpRefusal(Refusal):
    """An evolution op whose precondition does not hold on the manifest."""

    def __init__(
        self, message: str, *, check: RefusalKind, subjects: tuple[str, ...] = ()
    ) -> None:
        super().__init__(message, check=check, subjects=subjects)
