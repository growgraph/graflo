"""``state_core`` still answers, with a warning that names the aspects."""

from __future__ import annotations

import pytest

from graflo.architecture.evolution import aspects, state_core
from graflo.architecture.evolution.aspects import iris, provenance, state
from graflo.architecture.evolution.aspects import spec as aspects_spec
from graflo.architecture.evolution.state_core import plan, spec, vocabulary


def test_every_old_name_warns_and_resolves_to_the_aspects_package() -> None:
    for name in ("plan_lift", "LiftSpec", "Grounding", "EdgeGrounding", "LiftError"):
        with pytest.warns(DeprecationWarning, match="evolution.aspects") as record:
            resolved = getattr(state_core, name)
        assert resolved is getattr(aspects, name)
        assert "since graflo 1.19.0" in str(record[0].message)


@pytest.mark.parametrize(
    ("module", "name", "expected"),
    [
        (plan, "plan_lift", aspects.plan_lift),
        (plan, "LiftError", aspects.LiftError),
        (spec, "LiftSpec", aspects_spec.LiftSpec),
        (vocabulary, "DCTERMS_IS_PART_OF", iris.DCTERMS_IS_PART_OF),
        (vocabulary, "agent_vertex", provenance.vocabulary.agent_vertex),
        (vocabulary, "EVIDENCE", provenance.vocabulary.SOURCE_RECORD),
        (vocabulary, "evidence_vertex", provenance.vocabulary.source_record_vertex),
        (vocabulary, "state_vertex", state.vocabulary.state_vertex),
    ],
)
def test_old_submodule_names_warn_and_resolve(
    module: object, name: str, expected: object
) -> None:
    with pytest.warns(DeprecationWarning, match="evolution.aspects"):
        assert getattr(module, name) is expected


def test_every_listed_vocabulary_name_resolves() -> None:
    for name in vocabulary.__all__:
        with pytest.warns(DeprecationWarning):
            getattr(vocabulary, name)


def test_an_unknown_name_is_still_an_attribute_error() -> None:
    with pytest.raises(AttributeError):
        state_core.no_such_name  # noqa: B018
    with pytest.raises(AttributeError):
        vocabulary.EVIDENCE_ID  # noqa: B018
