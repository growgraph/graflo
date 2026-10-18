"""``state_core`` still answers, with a warning that names the aspects."""

from __future__ import annotations

import pytest

from graflo.architecture.evolution import aspects, state_core


def test_every_old_name_warns_and_resolves_to_the_aspects_package() -> None:
    for name in ("plan_lift", "LiftSpec", "Grounding", "EdgeGrounding", "LiftError"):
        with pytest.warns(DeprecationWarning, match="evolution.aspects") as record:
            resolved = getattr(state_core, name)
        assert resolved is getattr(aspects, name)
        assert "since graflo 1.19.0" in str(record[0].message)


def test_an_unknown_name_is_still_an_attribute_error() -> None:
    with pytest.raises(AttributeError):
        state_core.vocabulary  # noqa: B018
