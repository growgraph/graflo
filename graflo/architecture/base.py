"""Base model for Graflo configuration classes with YAML support."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Self

import yaml
from pydantic import BaseModel, ConfigDict


class ConfigBaseModel(BaseModel):
    """Base model for all Graflo configuration classes.

    Provides YAML serialization/deserialization and standard configuration
    for all Pydantic models in the system.

    This replaces the JSONWizard/YAMLWizard functionality from dataclass-wizard
    with Pydantic's superior validation and type safety.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        use_enum_values=True,
        validate_assignment=True,
    )

    @classmethod
    def from_yaml(cls, path: str) -> Self:
        """Load a single instance from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)

    @classmethod
    def from_yaml_list(cls, path: str) -> list[Self]:
        """Load a list of instances from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        if not isinstance(data, list):
            raise ValueError(f"Expected list in YAML file, got {type(data)}")
        return [cls.model_validate(item) for item in data]

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        """Load from a dictionary (or list for root model)."""
        return cls.model_validate(data)

    def to_yaml(self, path: str, **kwargs: Any) -> None:
        """Save instance to a YAML file."""
        with open(path, "w") as f:
            yaml.safe_dump(
                self.model_dump(by_alias=True, exclude_none=True),
                f,
                default_flow_style=False,
                sort_keys=False,
                **kwargs,
            )

    def to_yaml_str(self, **kwargs: Any) -> str:
        """Convert instance to a YAML string."""
        return yaml.safe_dump(
            self.model_dump(by_alias=True, exclude_none=True),
            default_flow_style=False,
            sort_keys=False,
            **kwargs,
        )

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Convert instance to a dictionary.

        Supports skip_defaults=True (mapped to exclude_defaults) for backward
        compatibility with dataclass-wizard style APIs.
        """
        if kwargs.get("skip_defaults"):
            kwargs = dict(kwargs)
            kwargs.pop("skip_defaults", None)
            kwargs["exclude_defaults"] = True
        return self.model_dump(by_alias=True, exclude_none=True, **kwargs)

    def update(self, other: Self) -> None:
        """Update this instance with values from another instance of the same type.

        Performs in-place merge: lists are concatenated, dicts/sets are merged,
        nested ConfigBaseModel instances are updated recursively. None values
        in other do not overwrite existing values.

        Args:
            other: Another instance of the same type to copy from

        Raises:
            TypeError: If other is not an instance of the same type
        """
        if type(other) is not type(self):
            raise TypeError(
                f"Expected {type(self).__name__} instance, got {type(other).__name__}"
            )
        for name in self.model_fields:
            current = getattr(self, name)
            other_val = getattr(other, name)
            if other_val is None:
                continue
            if isinstance(other_val, list):
                setattr(self, name, current + deepcopy(other_val))
            elif isinstance(other_val, set):
                setattr(self, name, current | deepcopy(other_val))
            elif isinstance(other_val, dict):
                setattr(self, name, {**current, **deepcopy(other_val)})
            elif isinstance(other_val, ConfigBaseModel):
                if current is not None:
                    current.update(other_val)
                else:
                    setattr(self, name, deepcopy(other_val))
            else:
                if current is None:
                    setattr(self, name, other_val)
