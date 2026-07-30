"""Identity funnel: ordered fallback branches for synthetic vertex keys.

A funnel is *schema policy*: an ordered list of branches, each naming the fields
that identify a vertex when they are all present. The first complete branch wins
and its field values are digested into the synthetic identity. When no branch is
complete the vertex has no identity — the caller leaves it empty rather than
inventing one, and the caster drops the document.

This generalizes :attr:`~graflo.architecture.schema.vertex.Vertex.hash_identity_properties`,
which is the single-branch case. Both authored forms are accepted; neither is
rewritten into the other, so legacy digests stay byte-identical.

Example:
    >>> funnel = IdentityFunnel(
    ...     branches=[
    ...         {"id": "email", "fields": ["email"]},
    ...         {"id": "phone", "when_all_present": ["phone", "country"],
    ...          "fields": ["phone", "country"]},
    ...     ]
    ... )
    >>> ", ".join(funnel.branch_ids)
    'email, phone'
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field as PydanticField
from pydantic import model_validator

from graflo.architecture.base import ConfigBaseModel

# Payload key carrying the branch id into the digest when ``include_branch_id``.
BRANCH_PAYLOAD_KEY = "_branch"

DigestCodec = Literal["sha256"]


class IdentityBranch(ConfigBaseModel):
    """One fallback branch of an identity funnel.

    Attributes:
        id: Branch name. Unique within the funnel; part of the digest payload
            when the funnel sets ``include_branch_id``.
        fields: Field names whose values are digested when this branch wins.
        when_all_present: Fields that must all be present and non-empty for this
            branch to fire. Defaults to ``fields`` when omitted.
    """

    id: str = PydanticField(
        ...,
        description="Branch name, unique within the funnel.",
    )
    fields: list[str] = PydanticField(
        ...,
        min_length=1,
        description="Field names digested when this branch wins.",
    )
    when_all_present: list[str] | None = PydanticField(
        default=None,
        description=(
            "Fields that must all be present and non-empty for this branch to "
            "fire. Defaults to ``fields``."
        ),
    )

    @model_validator(mode="after")
    def _validate_branch(self) -> IdentityBranch:
        if not self.id.strip():
            raise ValueError("identity branch id must be a non-empty string")
        if any(not name.strip() for name in self.fields):
            raise ValueError(
                f"identity branch '{self.id}': field names must be non-empty"
            )
        if self.when_all_present is not None:
            if not self.when_all_present:
                raise ValueError(
                    f"identity branch '{self.id}': when_all_present must be "
                    "non-empty when given — omit it to default to fields"
                )
            unknown = [
                name for name in self.when_all_present if name not in set(self.fields)
            ]
            if unknown:
                raise ValueError(
                    f"identity branch '{self.id}': when_all_present names "
                    f"{unknown} that are not among its fields {self.fields}. A "
                    "condition on a field the branch does not digest cannot "
                    "affect the key."
                )
        return self

    @property
    def required_fields(self) -> list[str]:
        """Fields that must be present for this branch to fire."""
        return self.when_all_present if self.when_all_present else self.fields

    @property
    def all_field_names(self) -> list[str]:
        """Every field this branch references, deduped, order preserved."""
        seen: dict[str, None] = {}
        for name in list(self.fields) + list(self.when_all_present or []):
            seen.setdefault(name, None)
        return list(seen)


class IdentityFunnel(ConfigBaseModel):
    """Ordered fallback branches producing a synthetic vertex identity.

    Attributes:
        digest: Digest codec. Only ``sha256`` in v1 — ``uuid5`` needs a namespace
            policy, not merely a UUID helper.
        include_branch_id: Include the winning branch id in the digest payload.
            On by default so two branches over the same values cannot collide.
        branches: Ordered branches; the first complete one wins.
    """

    digest: DigestCodec = PydanticField(
        default="sha256",
        description="Digest codec used to derive the synthetic identity.",
    )
    include_branch_id: bool = PydanticField(
        default=True,
        description=(
            "Include the winning branch id in the digest payload, so branches "
            "over equal values produce distinct keys."
        ),
    )
    branches: list[IdentityBranch] = PydanticField(
        ...,
        min_length=1,
        description="Ordered fallback branches; the first complete one wins.",
    )

    @model_validator(mode="after")
    def _validate_funnel(self) -> IdentityFunnel:
        seen: set[str] = set()
        duplicates: list[str] = []
        for branch in self.branches:
            if branch.id in seen:
                duplicates.append(branch.id)
            seen.add(branch.id)
        if duplicates:
            raise ValueError(
                f"identity funnel: duplicate branch ids {sorted(set(duplicates))}; "
                "branch ids must be unique because they take part in the digest"
            )
        return self

    @property
    def field_names(self) -> list[str]:
        """Every field referenced by any branch, deduped, order preserved."""
        seen: dict[str, None] = {}
        for branch in self.branches:
            for name in branch.all_field_names:
                seen.setdefault(name, None)
        return list(seen)

    @property
    def branch_ids(self) -> list[str]:
        """Branch ids in order."""
        return [branch.id for branch in self.branches]
