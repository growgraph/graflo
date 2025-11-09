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

import dataclasses
import importlib
import logging
from copy import deepcopy
from typing import Any, Optional

from graflo.onto import BaseDataclass

logger = logging.getLogger(__name__)


class TransformException(BaseException):
    """Base exception for transform-related errors."""

    pass


@dataclasses.dataclass
class ProtoTransform(BaseDataclass):
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

    name: Optional[str] = None
    module: Optional[str] = None
    params: dict[str, Any] = dataclasses.field(default_factory=dict)
    foo: Optional[str] = None
    input: str | list[str] | tuple[str, ...] = dataclasses.field(default_factory=tuple)
    output: str | list[str] | tuple[str, ...] = dataclasses.field(default_factory=tuple)

    def __post_init__(self):
        """Initialize the transform after dataclass initialization.

        Sets up the transform function and input/output field specifications.
        """
        self._foo = None
        self._init_foo()

        self.input = self._tuple_it(self.input)

        if not self.output:
            self.output = self.input
        self.output = self._tuple_it(self.output)

    @staticmethod
    def _tuple_it(x):
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

    def _init_foo(self):
        """Initialize the transform function from module.

        Imports the specified module and gets the transform function.

        Raises:
            TypeError: If module import fails
            ValueError: If function lookup fails
        """
        if self.module is not None and self.foo is not None:
            try:
                _module = importlib.import_module(self.module)
            except Exception as e:
                raise TypeError(f"Provided module {self.module} is not valid: {e}")
            try:
                self._foo = getattr(_module, self.foo)
            except Exception as e:
                raise ValueError(
                    f"Could not instantiate transform function. Exception: {e}"
                )

    def __lt__(self, other):
        """Compare transforms for ordering.

        Args:
            other: Other transform to compare with

        Returns:
            bool: True if this transform should be ordered before other
        """
        if self._foo is None and other._foo is not None:
            return True
        return False


@dataclasses.dataclass(kw_only=True)
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

    fields: str | list[str] | tuple[str, ...] = dataclasses.field(default_factory=tuple)
    map: dict[str, str] = dataclasses.field(default_factory=dict)
    switch: dict[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        """Initialize the transform after dataclass initialization.

        Sets up field specifications and validates transform configuration.

        Raises:
            ValueError: If transform configuration is invalid
        """
        super().__post_init__()
        self.functional_transform = False
        if self._foo is not None:
            self.functional_transform = True

        self.input = self._tuple_it(self.input)

        self.fields = self._tuple_it(self.fields)

        self.input = self.fields if self.fields and not self.input else self.input
        if not self.output:
            self.output = self.input
        self.output = self._tuple_it(self.output)

        if not self.input and not self.output:
            if self.map:
                items = list(self.map.items())
                self.input = tuple(x for x, _ in items)
                self.output = tuple(x for _, x in items)
            elif self.switch:
                self.input = tuple([k for k in self.switch])
                self.output = tuple(self.switch[self.input[0]])
            elif not self.name:
                raise ValueError(
                    "Either input and output, fields, map or name should be"
                    " provided in Transform constructor."
                )

    def __call__(self, *nargs, **kwargs):
        """Execute the transform.

        Args:
            *nargs: Positional arguments for the transform
            **kwargs: Keyword arguments for the transform

        Returns:
            dict: Transformed data
        """
        is_mapping = self._foo is None

        if is_mapping:
            input_doc = nargs[0]
            if isinstance(input_doc, dict):
                output_values = [input_doc[k] for k in self.input]
            else:
                output_values = nargs
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

    def _dress_as_dict(self, transform_result):
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
    def is_dummy(self):
        """Check if this is a dummy transform.

        Returns:
            bool: True if this is a dummy transform
        """
        return (self.name is not None) and (not self.map and self._foo is None)

    def update(self, t: Transform):
        """Update this transform with another transform's configuration.

        Args:
            t: Transform to update from

        Returns:
            Transform: Updated transform
        """
        t_copy = deepcopy(t)
        if self.input:
            t_copy.input = self.input
        if self.output:
            t_copy.output = self.output
        if self.params:
            t_copy.params.update(self.params)
        t_copy.__post_init__()
        return t_copy

    def get_barebone(
        self, other: Optional[Transform]
    ) -> tuple[Optional[Transform], Optional[Transform]]:
        """Get the barebone transform configuration.

        Args:
            other: Optional transform to use as base

        Returns:
            tuple[Optional[Transform], Optional[Transform]]: Updated self transform
            and transform to store in library
        """
        self_param = self.to_dict(skip_defaults=True)
        if self.foo is not None:
            # self will be the lib transform
            return None, self
        elif other is not None and other.foo is not None:
            # init self from other
            self_param.pop("foo", None)
            self_param.pop("module", None)
            other_param = other.to_dict(skip_defaults=True)
            other_param.update(self_param)
            return Transform(**other_param), None
        else:
            return None, None
