"""Profile definition, registry, and the entry points every surface calls.

A profile is a named, versioned list of assertions over a manifest. Nothing
here knows what any assertion means -- that keeps adding a profile to a
declaration rather than a code change in the runner.

:func:`check_manifest_config` is the primary entry point and takes the authored
document, because two of the world-model assertions are about what the author
declared and the parsed model has already normalized that away. See
:mod:`graflo.architecture.profile.context`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.profile.context import CheckContext, VocabularyResolver
from graflo.architecture.profile.model import (
    AssertionResult,
    ProfileReport,
    ProfileWaivers,
    roll_up,
)


@dataclass(frozen=True, slots=True)
class Assertion:
    """One mechanically checkable claim about a manifest."""

    id: str
    title: str
    required: bool
    run: Callable[[CheckContext], AssertionResult]


@dataclass(frozen=True, slots=True)
class Profile:
    """A named conformance level."""

    name: str
    version: str
    assertions: tuple[Assertion, ...]


def _graflo_version() -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("graflo")
    except PackageNotFoundError:
        return None


def _registry() -> dict[str, Profile]:
    """The known profiles.

    Built on call rather than at import so ``world_model`` can import the
    runner's types without a cycle.
    """
    from graflo.architecture.profile.world_model import (
        WORLD_MODEL_PROFILE,
    )

    return {WORLD_MODEL_PROFILE.name: WORLD_MODEL_PROFILE}


def list_profiles() -> list[tuple[str, str]]:
    """``(name, version)`` for every known profile."""
    return sorted((p.name, p.version) for p in _registry().values())


def get_profile(name: str) -> Profile:
    """The profile called *name*.

    Raises:
        KeyError: no such profile, naming the ones that exist.
    """
    profiles = _registry()
    if name not in profiles:
        known = ", ".join(sorted(profiles))
        raise KeyError(f"unknown profile {name!r}; known profiles: {known}")
    return profiles[name]


def run_profile(profile: Profile, context: CheckContext) -> ProfileReport:
    """Run every assertion of *profile* against *context*."""
    results: list[AssertionResult] = []
    for assertion in profile.assertions:
        result = assertion.run(context)
        waiver = (
            context.waivers.for_assertion(assertion.id)
            if context.waivers is not None
            else None
        )
        if waiver is not None and result.status in ("fail", "warn"):
            result = result.model_copy(update={"status": "waived", "waiver": waiver})
        results.append(result)
    return ProfileReport(
        profile=profile.name,
        profile_version=profile.version,
        graflo_version=_graflo_version(),
        status=roll_up([r.status for r in results]),
        assertions=results,
    )


def check_manifest(
    manifest: GraphManifest,
    *,
    profile: str = "world-model",
    authored: Mapping[str, Any] | None = None,
    waivers: ProfileWaivers | None = None,
    subject: str | None = None,
    resolver: VocabularyResolver | None = None,
) -> ProfileReport:
    """Check an already-parsed *manifest*.

    Prefer :func:`check_manifest_config` when the authored document is
    available: without it the declaration assertions can only warn.
    """
    context = CheckContext(manifest=manifest, authored=authored, waivers=waivers)
    if resolver is not None:
        context.resolver = resolver
    report = run_profile(get_profile(profile), context)
    return report.model_copy(update={"subject": subject})


def check_manifest_config(
    config: Mapping[str, Any],
    *,
    profile: str = "world-model",
    waivers: ProfileWaivers | None = None,
    subject: str | None = None,
    resolver: VocabularyResolver | None = None,
) -> ProfileReport:
    """Check the manifest *config* as authored.

    The primary entry point. Parses *config* into a manifest and keeps the
    original mapping alongside it, so an assertion can tell "the author did not
    declare this" from "the author declared the value that is also the default".
    """
    manifest = GraphManifest.from_config(dict(config))
    manifest.finish_init()
    return check_manifest(
        manifest,
        profile=profile,
        authored=config,
        waivers=waivers,
        subject=subject,
        resolver=resolver,
    )


__all__ = [
    "Assertion",
    "Profile",
    "check_manifest",
    "check_manifest_config",
    "get_profile",
    "list_profiles",
    "run_profile",
]
