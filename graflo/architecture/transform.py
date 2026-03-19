"""Data transformation and mapping system for graph databases.

This module provides a flexible system for transforming and mapping data in graph
databases. It supports both functional transformations and declarative mappings,
with support for output dressing and parameter configuration.

Key Components:
    - ProtoTransform: Base class for transform definitions (raw function wrapper)
    - Transform: Concrete transform with input extraction, output dressing,
      and field mapping
    - TransformException: Custom exception for transform errors

The transform system supports:
    - Functional transformations through imported modules
    - Field mapping and dressing
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
from typing import Any, Literal, Self

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


def _tuple_groups_it(
    groups: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
) -> tuple[tuple[str, ...], ...]:
    """Normalize nested group input into tuple-of-tuples."""
    normalized: list[tuple[str, ...]] = []
    for group in groups:
        normalized.append(_tuple_it(group))
    return tuple(normalized)


class TransformException(BaseException):
    """Base exception for transform-related errors."""

    pass


class DressConfig(ConfigBaseModel):
    """Output dressing specification for pivoted transforms.

    When a transform function returns a single scalar (e.g. ``round_str``
    returns ``6.43``), DressConfig describes how to package that scalar together
    with the input field name into a dict.

    Attributes:
        key: Output field that receives the **input field name** (e.g. "Open").
        value: Output field that receives the **function result** (e.g. 6.43).
    """

    key: str = Field(description="Output field name for the input key.")
    value: str = Field(description="Output field name for the function result.")


class KeySelectionConfig(ConfigBaseModel):
    """Selection of document keys for key-target transforms."""

    mode: Literal["all", "include", "exclude"] = Field(
        default="all",
        description=(
            "How keys are selected for target='keys': all=all keys, "
            "include=only specified keys, exclude=all except specified keys."
        ),
    )
    names: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Keys used by include/exclude modes.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_names(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        names = data.get("names")
        if isinstance(names, str):
            data["names"] = (names,)
        elif isinstance(names, list):
            data["names"] = tuple(names)
        elif names is None:
            data["names"] = ()
        return data

    @model_validator(mode="after")
    def _validate_mode_names(self) -> Self:
        if self.mode == "all" and self.names:
            raise ValueError("keys.names must be empty when keys.mode='all'.")
        if self.mode in {"include", "exclude"} and not self.names:
            raise ValueError(
                "keys.names must be provided when keys.mode is include/exclude."
            )
        return self


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
        description="Optional name for this transform (e.g. for reference in ingestion_model.transforms).",
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
    input_groups: tuple[tuple[str, ...], ...] = Field(
        default_factory=tuple,
        description=(
            "Explicit groups of input fields for repeated tuple-style value calls."
        ),
    )
    output_groups: tuple[tuple[str, ...], ...] = Field(
        default_factory=tuple,
        description=(
            "Explicit output field groups aligned with input_groups for grouped value calls."
        ),
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
        for key in ("input_groups", "output_groups"):
            if key in data:
                if data[key] is None:
                    data[key] = ()
                else:
                    data[key] = _tuple_groups_it(data[key])
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

    def apply(self, *args: Any, **kwargs: Any) -> Any:
        """Apply the raw transform function to the given arguments.

        This is the core function invocation without any input extraction or
        output dressing — purely ``self._foo(*args, **kwargs, **self.params)``.

        Raises:
            TransformException: If no transform function has been set.
        """
        if self._foo is None:
            raise TransformException("No transform function set")
        return self._foo(*args, **kwargs, **self.params)

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

    Wraps a ProtoTransform with input extraction, output dressing, field
    mapping, and transform composition.

    Attributes:
        fields: Tuple of fields to transform
        map: Dictionary mapping input fields to output fields
        dress: Optional DressConfig for pivoted output
        functional_transform: Whether this is a functional transform
    """

    fields: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Field names for declarative transform (used to derive input when input unset).",
    )
    map: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of input_key -> output_key for pure field renaming (no function).",
    )
    dress: DressConfig | None = Field(
        default=None,
        description=(
            "Dressing spec for pivoted output. "
            "dress.key receives the input field name, dress.value receives the "
            "function result. E.g. dress={key: name, value: value} with "
            "input=(Open,) produces {name: 'Open', value: <result>}."
        ),
    )
    strategy: Literal["single", "each", "all"] = Field(
        default="single",
        description=(
            "Functional call strategy. "
            "single: call function once with all input values. "
            "each: call function once per input field (unary). "
            "all: pass full document as a single argument."
        ),
    )
    target: Literal["values", "keys"] = Field(
        default="values",
        description=(
            "Transform target. values=apply function to input values; "
            "keys=apply function to selected document keys."
        ),
    )
    keys: KeySelectionConfig = Field(
        default_factory=KeySelectionConfig,
        description="Key selection for key-target transforms.",
    )
    passthrough_group_output: bool = Field(
        default=True,
        description=(
            "When grouped mode omits outputs, map function results back to input group keys."
        ),
    )

    functional_transform: bool = Field(
        default=False,
        description="True when a callable (module.foo) is set; False for pure map/dress transforms.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        if "fields" in data and data["fields"] is not None:
            data["fields"] = _tuple_it(data["fields"])
        if "switch" in data:
            raise ValueError(
                "Legacy `switch` is no longer supported. Use `input` + `dress`."
            )
        if "dress" in data and isinstance(data["dress"], (list, tuple)):
            raise ValueError(
                "List-style `dress` is no longer supported. "
                "Use a dict: dress={key: ..., value: ...}."
            )
        if "keys" in data:
            keys = data["keys"]
            if isinstance(keys, str):
                data["keys"] = {"mode": "include", "names": (keys,)}
            elif isinstance(keys, list):
                data["keys"] = {"mode": "include", "names": tuple(keys)}
            elif isinstance(keys, tuple):
                data["keys"] = {"mode": "include", "names": keys}
            elif isinstance(keys, dict):
                keys = dict(keys)
                names = keys.get("names")
                if isinstance(names, str):
                    keys["names"] = (names,)
                elif isinstance(names, list):
                    keys["names"] = tuple(names)
                data["keys"] = keys
        return data

    @model_validator(mode="after")
    def _init_derived(self) -> Self:
        object.__setattr__(self, "functional_transform", self._foo is not None)
        self._init_output_from_grouped_input()
        self._init_input_from_fields()
        self._init_io_from_map()
        self._init_output_from_dress()
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

    def _init_output_from_dress(self) -> None:
        """Derive output from dress — always takes precedence when set."""
        if self.dress is not None:
            object.__setattr__(self, "output", (self.dress.key, self.dress.value))

    def _init_output_from_grouped_input(self) -> None:
        """Default scalar output names for grouped calls when omitted."""
        if not self.input_groups:
            return
        if self.output or self.output_groups:
            return
        if not self.passthrough_group_output:
            return
        scalar_names: list[str] = []
        for group in self.input_groups:
            if len(group) != 1:
                return
            scalar_names.append(group[0])
        if scalar_names:
            object.__setattr__(self, "output", tuple(scalar_names))

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
        if self.target == "keys":
            if self.input_groups or self.output_groups:
                raise ValueError(
                    "target='keys' does not accept input_groups/output_groups."
                )
            if self._foo is None:
                raise ValueError("target='keys' requires a functional transform.")
            if self.map:
                raise ValueError("target='keys' cannot be combined with map.")
            if self.input or self.output or self.fields:
                raise ValueError(
                    "target='keys' does not accept input/output/fields; use keys selector."
                )
            if self.dress is not None:
                raise ValueError("target='keys' is not compatible with dress.")
            if self.strategy != "single":
                raise ValueError(
                    "target='keys' uses implicit per-key execution and does not accept strategy."
                )
            return

        # Reject only user-specified map+function conflict. A derived map
        # (from input/output defaults) is valid for functional transforms.
        if "map" in self.model_fields_set and self.map and self._foo is not None:
            raise ValueError("map and functional transform cannot be used together.")
        if self.dress is not None:
            if self._foo is None:
                raise ValueError(
                    "dress requires a functional transform (module + foo)."
                )
            if len(self.input) != 1:
                raise ValueError("dress requires exactly one input field.")
        if self.strategy != "single" and self._foo is None:
            raise ValueError("strategy applies only to functional transforms.")
        if self.input_groups:
            if self._foo is None:
                raise ValueError(
                    "input_groups requires a functional transform (module + foo)."
                )
            if self.strategy != "single":
                raise ValueError(
                    "input_groups mode is explicit grouped execution and does not accept strategy."
                )
            if self.input or self.fields:
                raise ValueError("input_groups cannot be combined with input/fields.")
            if self.map:
                raise ValueError("input_groups cannot be combined with map.")
            if self.dress is not None:
                raise ValueError("input_groups is not compatible with dress.")
            if self.output_groups and self.output:
                raise ValueError(
                    "Provide either output or output_groups for input_groups mode, not both."
                )
            if self.output_groups and len(self.output_groups) != len(self.input_groups):
                raise ValueError(
                    "output_groups must have same number of groups as input_groups."
                )
            if self.output and len(self.output) != len(self.input_groups):
                raise ValueError(
                    "When using input_groups with scalar outputs, output length must match number of input_groups."
                )
        elif self.output_groups:
            raise ValueError("output_groups requires input_groups.")
        if self._foo is not None and not self.input:
            if self.strategy != "all" and not self.input_groups:
                raise ValueError(
                    "Functional transforms require `input` (string or list of field names)."
                )
        if self.strategy == "all":
            if self.input or self.fields:
                raise ValueError("strategy='all' does not accept input/fields.")
            if self.dress is not None:
                raise ValueError("strategy='all' is not compatible with dress.")
        if self.strategy == "each":
            if not self.input:
                raise ValueError("strategy='each' requires one or more input fields.")
            if self.output and len(self.input) != len(self.output):
                raise ValueError(
                    "strategy='each' requires output length to match input length."
                )
        if (
            self._foo is None
            and self.dress is None
            and self.input
            and self.output
            and len(self.input) != len(self.output)
        ):
            raise ValueError(
                "Non-functional transforms require input/output to have the same length."
            )
        if (
            not self.input
            and not self.output
            and not self.input_groups
            and not self.output_groups
            and not self.name
            and not (self._foo is not None and self.strategy == "all")
        ):
            raise ValueError(
                "Either input/output, fields, map or name must be provided in "
                "Transform constructor."
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
        if self.target == "keys":
            input_doc = nargs[0] if nargs and isinstance(nargs[0], dict) else None
            if input_doc is None:
                raise TransformException(
                    "target='keys' requires a document dictionary."
                )
            return self._transform_keys(input_doc, **kwargs)

        if self.input_groups:
            input_doc = nargs[0] if nargs and isinstance(nargs[0], dict) else None
            if input_doc is None:
                raise TransformException(
                    "input_groups transforms require a document dictionary."
                )
            return self._transform_input_groups(input_doc, **kwargs)

        if self.is_mapping:
            input_doc = nargs[0]
            if isinstance(input_doc, dict):
                output_values = [input_doc[k] for k in self.input]
            else:
                output_values = list(nargs)
        else:
            if self.strategy == "all":
                if nargs and isinstance(nargs[0], dict):
                    output_values = self.apply(nargs[0], **kwargs)
                else:
                    output_values = self.apply(*nargs, **kwargs)
            elif self.strategy == "each":
                if nargs and isinstance(input_doc := nargs[0], dict):
                    output_values = [
                        self.apply(input_doc[k], **kwargs) for k in self.input
                    ]
                else:
                    output_values = [self.apply(value, **kwargs) for value in nargs]
            else:
                if nargs and isinstance(input_doc := nargs[0], dict):
                    new_args = [input_doc[k] for k in self.input]
                    output_values = self.apply(*new_args, **kwargs)
                else:
                    output_values = self.apply(*nargs, **kwargs)

        if self.output:
            r = self._dress_as_dict(output_values)
        else:
            r = output_values
        return r

    def _apply_grouped_result(
        self,
        out: dict[str, Any],
        result: Any,
        input_group: tuple[str, ...],
        output_group: tuple[str, ...] | None,
        *,
        group_index: int,
    ) -> None:
        if output_group is not None:
            if isinstance(result, (list, tuple)):
                values = list(result)
            else:
                values = [result]
            if len(values) != len(output_group):
                raise TransformException(
                    f"input_groups[{group_index}] produced {len(values)} values, "
                    f"but output_groups[{group_index}] expects {len(output_group)}."
                )
            pairs = zip(output_group, values)
        elif self.output:
            pairs = ((self.output[group_index], result),)
        else:
            if isinstance(result, (list, tuple)):
                values = list(result)
                if len(values) != len(input_group):
                    raise TransformException(
                        f"input_groups[{group_index}] has {len(input_group)} fields, "
                        f"but transform returned {len(values)} values. "
                        "Provide output/output_groups explicitly to resolve mapping."
                    )
                pairs = zip(input_group, values)
            else:
                if len(input_group) != 1:
                    raise TransformException(
                        f"input_groups[{group_index}] has {len(input_group)} fields "
                        "but transform returned a scalar. "
                        "Provide output/output_groups explicitly for scalar group results."
                    )
                pairs = ((input_group[0], result),)
        for key, value in pairs:
            if key in out:
                raise TransformException(
                    f"Grouped transform produced duplicate output key '{key}'."
                )
            out[key] = value

    def _transform_input_groups(
        self, doc: dict[str, Any], **kwargs: Any
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for idx, input_group in enumerate(self.input_groups):
            values = [doc[k] for k in input_group]
            result = self.apply(*values, **kwargs)
            output_group = self.output_groups[idx] if self.output_groups else None
            self._apply_grouped_result(
                out,
                result,
                input_group,
                output_group,
                group_index=idx,
            )
        return out

    @property
    def is_mapping(self) -> bool:
        """True when the transform is pure mapping (no function)."""
        return self._foo is None

    def _dress_as_dict(self, transform_result: Any) -> dict[str, Any]:
        """Convert transform result to dictionary format.

        When ``dress`` is set the result is pivoted: the input field name is
        stored under ``dress.key`` and the function result under ``dress.value``.
        Otherwise the result is mapped positionally to ``output`` fields.
        """
        if self.dress is not None:
            return {
                self.dress.key: self.input[0],
                self.dress.value: transform_result,
            }
        elif isinstance(transform_result, (list, tuple)):
            return {k: v for k, v in zip(self.output, transform_result)}
        else:
            return {self.output[-1]: transform_result}

    def _selected_keys(self, doc: dict[str, Any]) -> set[str]:
        if self.keys.mode == "all":
            return set(doc.keys())
        selected = set(self.keys.names)
        if self.keys.mode == "include":
            return selected
        return {k for k in doc if k not in selected}

    def _transform_keys(self, doc: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        selected = self._selected_keys(doc)
        out: dict[str, Any] = {}
        for key, value in doc.items():
            new_key = self.apply(key, **kwargs) if key in selected else key
            if not isinstance(new_key, str):
                raise TransformException(
                    "Key transform functions must return str values."
                )
            if new_key in out:
                raise TransformException(
                    f"Key transform collision detected for key '{new_key}'."
                )
            out[new_key] = value
        return out

    @property
    def is_dummy(self) -> bool:
        """Check if this is a dummy transform.

        Returns:
            bool: True if this is a dummy transform
        """
        return self.name is not None and not self.map and self._foo is None

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
