"""Hypothesis example budgets, scaled by the loaded profile.

Two profiles: ``full`` runs every property test at the count it is written
with; ``ci`` (the default) runs a fifth of it, derandomized. Select one with
``HYPOTHESIS_PROFILE=full`` or ``--hypothesis-profile=full``.

A test that sets its own ``max_examples`` overrides the profile, so it states
its count at the full budget through :func:`examples` instead.
"""

from __future__ import annotations

from hypothesis import settings

#: ``max_examples`` of the ``full`` profile: Hypothesis's own default, so a test
#: with no count of its own keeps it.
FULL = 100


def register_profiles() -> None:
    settings.register_profile("full", max_examples=FULL, deadline=None)
    settings.register_profile(
        "ci",
        max_examples=FULL // 5,
        deadline=None,
        derandomize=True,
        database=None,
    )


def examples(at_full: int, *, floor: int = 10, reduced: int | None = None) -> int:
    """*at_full* scaled by the loaded profile's share of the full budget.

    Args:
        at_full: The count under the ``full`` profile.
        floor: The least count under a reduced profile.
        reduced: The count under a reduced profile, in place of the scaled one;
            for a test parametrized so widely that a fifth of its count per
            parameter is still the heaviest part of the suite.
    """
    share = settings().max_examples / FULL
    if share < 1 and reduced is not None:
        return reduced
    return max(floor, round(at_full * share))
