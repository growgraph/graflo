"""Data transformation and mapping system for graph databases.

This module provides a flexible system for transforming and mapping data in graph
databases. It supports both functional transformations and declarative mappings,
with support for field switching and parameter configuration.

Key Components:
    - ProtoTransform: Base class for transform definitions
    - Transform: Concrete transform implementation
    - TransformException: Custom exception for transform errors

The transform system supports:
    - Functional transformations through imported modules
    - Field mapping and switching
    - Parameter configuration
    - Input/output field specification
    - Transform composition and inheritance

Example:
    >>> transform = Transform(
    ...     module="my_module",
    ...     foo="process_data",
    ...     input=("field1", "field2"),
    ...     output=("result1", "result2")
    ... )
    >>> result = transform({"field1": 1, "field2": 2})
"""

from __future__ import annotations

import importlib
import logging
from copy import deepcopy
from typing import Any, Self

from pydantic import Field, PrivateAttr, model_validator

from graflo.architecture.base import ConfigBaseModel

logger = logging.getLogger(__name__)


def _tuple_it(x: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Convert input to tuple format.

    Args:
        x: Input to convert (string, list, or tuple)

    Returns:
        tuple: Converted tuple
    """
    if isinstance(x, str):
        x = [x]
    if isinstance(x, list):
        x = tuple(x)
    return x


class TransformException(BaseException):
    """Base exception for transform-related errors."""

    pass


class ProtoTransform(ConfigBaseModel):
    """Base class for transform definitions.

    This class provides the foundation for data transformations, supporting both
    functional transformations and declarative mappings.

    Attributes:
        name: Optional name of the transform
        module: Optional module containing the transform function
        params: Dictionary of transform parameters
        foo: Optional name of the transform function
        input: Tuple of input field names
        output: Tuple of output field names
        _foo: Internal reference to the transform function
    """

    name: str | None = Field(
        default=None,
        description="Optional name for this transform (e.g. for reference in schema.transforms).",
    )
    module: str | None = Field(
        default=None,
        description="Python module path containing the transform function (e.g. my_package.transforms).",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Extra parameters passed to the transform function at runtime.",
    )
    foo: str | None = Field(
        default=None,
        description="Name of the callable in module to use as the transform function.",
    )
    input: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Input field names passed to the transform function.",
    )
    output: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Output field names produced by the transform (defaults to input if unset).",
    )

    _foo: Any = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def _normalize_input_output(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        for key in ("input", "output"):
            if key in data:
                if data[key] is not None:
                    data[key] = _tuple_it(data[key])
                else:
                    data[key] = ()
        return data

    @model_validator(mode="after")
    def _init_foo_and_output(self) -> Self:
        if self.module is not None and self.foo is not None:
            try:
                _module = importlib.import_module(self.module)
            except Exception as e:
                raise TypeError(f"Provided module {self.module} is not valid: {e}")
            try:
                object.__setattr__(self, "_foo", getattr(_module, self.foo))
            except Exception as e:
                raise ValueError(
                    f"Could not instantiate transform function. Exception: {e}"
                )
        if not self.output and self.input:
            object.__setattr__(self, "output", self.input)
        return self

    @classmethod
    def get_fields_members(cls) -> list[str]:
        """Get list of field members (public model fields)."""
        return list(cls.model_fields.keys())

    def __lt__(self, other: object) -> bool:
        """Compare transforms for ordering.

        Args:
            other: Other transform to compare with

        Returns:
            bool: True if this transform should be ordered before other
        """
        if not isinstance(other, ProtoTransform):
            return NotImplemented
        if self._foo is None and other._foo is not None:
            return True
        return False


class Transform(ProtoTransform):
    """Concrete transform implementation.

    This class extends ProtoTransform with additional functionality for
    field mapping, switching, and transform composition.

    Attributes:
        fields: Tuple of fields to transform
        map: Dictionary mapping input fields to output fields
        switch: Dictionary for field switching logic
        functional_transform: Whether this is a functional transform
    """

    fields: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Field names for declarative transform (used to derive input when input unset).",
    )
    map: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of output_key -> input_key for pure field renaming (no function).",
    )
    switch: dict[str, Any] = Field(
        default_factory=dict,
        description="Switch/case-style mapping for conditional field values (key -> output spec).",
    )

    functional_transform: bool = Field(
        default=False,
        description="True when a callable (module.foo) is set; False for pure map/switch transforms.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        if "fields" in data and data["fields"] is not None:
            data["fields"] = _tuple_it(data["fields"])
        return data

    @model_validator(mode="after")
    def _init_derived(self) -> Self:
        object.__setattr__(self, "functional_transform", self._foo is not None)
        self._init_input_from_fields()
        self._init_io_from_map()
        self._init_from_switch()
        self._default_output_from_input()
        self._init_map_from_io()
        self._validate_configuration()
        return self

    def _init_input_from_fields(self) -> None:
        """Populate input from fields when provided."""
        if self.fields and not self.input:
            object.__setattr__(self, "input", self.fields)

    def _init_io_from_map(self, force_init: bool = False) -> None:
        """Populate input/output tuples from an explicit map."""
        if not self.map:
            return
        if force_init or (not self.input and not self.output):
            input_fields, output_fields = zip(*self.map.items())
            object.__setattr__(self, "input", tuple(input_fields))
            object.__setattr__(self, "output", tuple(output_fields))
        elif not self.input:
            object.__setattr__(self, "input", tuple(self.map.keys()))
        elif not self.output:
            object.__setattr__(self, "output", tuple(self.map.values()))

    def _init_from_switch(self) -> None:
        """Fallback initialization using switch definitions."""
        if self.switch and not self.input and not self.output:
            object.__setattr__(self, "input", tuple(self.switch))
            first_key = self.input[0]
            object.__setattr__(self, "output", _tuple_it(self.switch[first_key]))

    def _default_output_from_input(self) -> None:
        """Ensure output mirrors input when not explicitly provided."""
        if not self.output and self.input:
            object.__setattr__(self, "output", self.input)

    def _init_map_from_io(self) -> None:
        """Derive map from input/output when possible."""
        if self.map or not self.input or not self.output:
            return
        if len(self.input) != len(self.output):
            return
        object.__setattr__(
            self, "map", {src: dst for src, dst in zip(self.input, self.output)}
        )

    def _validate_configuration(self) -> None:
        """Validate that the transform has enough information to operate."""
        if not self.input and not self.output and not self.name:
            raise ValueError(
                "Either input/output, fields, map or name must be provided in Transform "
                "constructor."
            )

    def _refresh_derived(self) -> None:
        """Re-run derived state (e.g. map from input/output) after mutating attributes."""
        self._init_map_from_io()

    def __call__(self, *nargs: Any, **kwargs: Any) -> dict[str, Any] | Any:
        """Execute the transform.

        Args:
            *nargs: Positional arguments for the transform
            **kwargs: Keyword arguments for the transform

        Returns:
            dict: Transformed data
        """
        if self.is_mapping:
            input_doc = nargs[0]
            if isinstance(input_doc, dict):
                output_values = [input_doc[k] for k in self.input]
            else:
                output_values = list(nargs)
        else:
            if nargs and isinstance(input_doc := nargs[0], dict):
                new_args = [input_doc[k] for k in self.input]
                output_values = self._foo(*new_args, **kwargs, **self.params)
            else:
                output_values = self._foo(*nargs, **kwargs, **self.params)

        if self.output:
            r = self._dress_as_dict(output_values)
        else:
            r = output_values
        return r

    @property
    def is_mapping(self) -> bool:
        """True when the transform is pure mapping (no function)."""
        return self._foo is None

    def _dress_as_dict(self, transform_result: Any) -> dict[str, Any]:
        """Convert transform result to dictionary format.

        Args:
            transform_result: Result of the transform

        Returns:
            dict: Dictionary representation of the result
        """
        if isinstance(transform_result, (list, tuple)) and not self.switch:
            upd = {k: v for k, v in zip(self.output, transform_result)}
        else:
            # TODO : temporary solution works only there is one switch clause
            upd = {self.output[-1]: transform_result}
        for k0, (q, qq) in self.switch.items():
            upd.update({q: k0})
        return upd

    @property
    def is_dummy(self) -> bool:
        """Check if this is a dummy transform.

        Returns:
            bool: True if this is a dummy transform
        """
        return (self.name is not None) and (not self.map and self._foo is None)

    def merge_from(self, t: Transform) -> Transform:
        """Merge another transform's configuration into a copy of it.

        Returns a new Transform with values from self overriding t where set.
        Does not override ConfigBaseModel.update (in-place); use this for
        copy-and-merge semantics.

        Args:
            t: Transform to merge from

        Returns:
            Transform: New transform with merged configuration
        """
        t_copy = deepcopy(t)
        if self.input:
            t_copy.input = self.input
        if self.output:
            t_copy.output = self.output
        if self.params:
            t_copy.params = {**t_copy.params, **self.params}
        t_copy._refresh_derived()
        return t_copy

    def get_barebone(
        self, other: Transform | None
    ) -> tuple[Transform | None, Transform | None]:
        """Get the barebone transform configuration.

        Args:
            other: Optional transform to use as base

        Returns:
            tuple[Transform | None, Transform | None]: Updated self transform
            and transform to store in library
        """
        self_param = self.to_dict(exclude_defaults=True)
        if self.foo is not None:
            # self will be the lib transform
            return None, self
        elif other is not None and other.foo is not None:
            # init self from other
            self_param.pop("foo", None)
            self_param.pop("module", None)
            other_param = other.to_dict(exclude_defaults=True)
            other_param.update(self_param)
            return Transform(**other_param), None
        else:
            return None, None
