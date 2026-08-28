"""Schema metadata and versioning."""

from __future__ import annotations

import re

from pydantic import Field as PydanticField
from pydantic import field_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.naming import NamingConvention
from graflo.architecture.schema.semantics import Semantics

_SEMVER_RE = re.compile(
    r"^\d+\.\d+\.\d+"
    r"(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?"
    r"(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$"
)


class GraphMetadata(ConfigBaseModel):
    """Schema metadata and versioning information.

    Holds metadata about the schema, including its name, version, and
    description.  Used for schema identification and versioning.
    Suitable for LLM-generated schema constituents.
    """

    name: str = PydanticField(
        ...,
        description="Name of the schema (e.g. graph or database identifier).",
    )
    version: str | None = PydanticField(
        default=None,
        description="Semantic version of the schema (e.g. '1.0.0', '2.1.3-beta+build.42').",
    )
    description: str | None = PydanticField(
        default=None,
        description="Optional human-readable description of the schema.",
    )
    semantics: Semantics | None = PydanticField(
        default=None,
        description="Optional external-vocabulary anchors for the schema as a whole.",
    )
    naming: NamingConvention | None = PydanticField(
        default=None,
        description=(
            "Optional declaration of the naming style this schema's invented "
            "identifiers follow. Purely descriptive: nothing consults it at "
            "runtime. Its audience is the next author — human or agent — "
            "extending this schema, who would otherwise have to infer the "
            "convention from the names and usually infers it wrong."
        ),
    )

    @field_validator("version")
    @classmethod
    def _validate_semver(cls, v: str | None) -> str | None:
        if v is not None and not _SEMVER_RE.match(v):
            raise ValueError(
                f"version '{v}' is not a valid semantic version "
                f"(expected MAJOR.MINOR.PATCH[-prerelease][+build])"
            )
        return v
