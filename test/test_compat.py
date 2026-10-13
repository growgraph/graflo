"""Deprecation helpers: what each warning says and where it points."""

import warnings

import pytest
from pydantic import BaseModel, model_validator

from graflo._compat import deprecated, rename_key, warn_deprecated


def test_warning_names_versions_and_replacement() -> None:
    with pytest.warns(DeprecationWarning) as record:
        warn_deprecated("old_fn", since="1.19.0", removed_in="2.0.0", use="new_fn")
    message = str(record[0].message)
    assert "old_fn" in message
    assert "since graflo 1.19.0" in message
    assert "removed in 2.0.0" in message
    assert "use new_fn" in message


def test_deprecated_function_warns_at_the_caller_and_still_runs() -> None:
    @deprecated(since="1.19.0", removed_in="2.0.0")
    def add(a: int, b: int) -> int:
        return a + b

    with pytest.warns(DeprecationWarning, match="add is deprecated") as record:
        assert add(1, 2) == 3
    assert record[0].filename == __file__
    assert add.__name__ == "add"


class _Model(BaseModel):
    new: int

    @model_validator(mode="before")
    @classmethod
    def _renamed(cls, data: object) -> object:
        return rename_key(
            data, "old", "new", owner="_Model", since="1.19.0", removed_in="2.0.0"
        )


def test_renamed_key_is_moved_with_a_warning() -> None:
    with pytest.warns(
        DeprecationWarning, match=r"_Model\.old is deprecated.*use _Model\.new"
    ):
        assert _Model.model_validate({"old": 1}).new == 1


def test_current_key_does_not_warn() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert _Model.model_validate({"new": 2}).new == 2


def test_both_keys_are_refused() -> None:
    with pytest.raises(ValueError, match="set 'new' only"):
        _Model.model_validate({"old": 1, "new": 2})
