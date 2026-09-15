"""The base class every merge-time refusal shares.

A refusal carries two structured fields beside its prose. ``check`` names the
rule that refused -- the parenthesised phrase the message already spells out --
and ``subjects`` the names it is about, as
:func:`~graflo.architecture.evolution.equivalence.subject` ids such as
``left:Firm`` or ``left:Firm.firm_id``. Both are optional and **neither appears
in the message**, so a caller that only reads ``str(exc)`` sees exactly what it
saw before they existed.

The two fields are what lets a caller classify a refusal without parsing its
prose: :mod:`graflo.architecture.evolution.preview` keys its finding kinds on
``check``, and a refusal that carries none is one the preview is not asked to
anticipate.

This module sits at the bottom of the import order deliberately. The schema
layer raises refusals of its own, so the base cannot live beside the merge
machinery that consumes them.
"""

from __future__ import annotations


class Refusal(ValueError):
    """A declaration that a combining operation will not carry out.

    Subclasses name the layer that refused -- the cluster resolver, the
    canonical map, the schema union, the field fold. Deriving from
    :class:`ValueError` rather than replacing it keeps every existing
    ``except ValueError`` and ``pytest.raises(ValueError)`` working, which is
    what makes adding the structured fields a non-breaking change.
    """

    def __init__(
        self, message: str, *, check: str = "", subjects: tuple[str, ...] = ()
    ) -> None:
        super().__init__(message)
        self.check = check
        self.subjects = subjects
