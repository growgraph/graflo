"""The refusals :func:`~graflo.architecture.evolution.merge.merge_manifests` raises on names and identity."""

from __future__ import annotations

from graflo.architecture.refusal import Refusal


class MergeNameConflictError(Refusal):
    """Two names denote one concept under different naming conventions.

    Distinct from ``MergeCanonicalConflictError`` in ``canonical.py``, which
    reports a *declared* CanonicalMap contradicting the op. This one fires on
    the residue neither side declared -- the undeclared path, where merge
    would otherwise produce two unrelated types with the data split between
    them and nothing raising.

    ``check`` names the rule that refused and ``subjects`` the names it is
    about, as :func:`~graflo.architecture.evolution.equivalence.subject` ids;
    see :class:`.Refusal`.
    """


class MergeIdentityError(Refusal):
    """A merged vertex's identity is ambiguous and nothing resolves it.

    Two or more cluster members disagree on their (canonical-name) identity
    field-set and the ``VertexEquivalence`` declares no ``identity``. The
    alternative -- silently taking the union of both field-sets as the new
    identity -- produces a natural key no record fully carries. Also raised
    for a declared ``identity`` some member cannot complete, or one whose
    funnel's synthetic ``id`` a member already declares as a property.

    ``subjects`` names the merged class and its disagreeing members, as
    :func:`~graflo.architecture.evolution.equivalence.subject` ids; it does not
    appear in the message.
    """

    def __init__(
        self, message: str, *, check: str = "", subjects: tuple[str, ...] = ()
    ) -> None:
        # The only refusal carrying a default check: every raise site is the
        # same rule, and three of them pass no subjects either.
        super().__init__(
            message, check=check or "identity disagreement", subjects=subjects
        )
