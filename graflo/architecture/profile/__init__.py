"""Named conformance profiles over a manifest.

A profile is a versioned list of mechanically checkable assertions. It reads
only fields the contract already has, introduces no semantics and touches no
backend -- so it can run against manifests this package did not author, which
is the point.

Eager re-export: the subpackage imports nothing heavier than the contract
models, so there is no lazy-facade cost to avoid here.
"""

from graflo.architecture.profile.context import (
    CheckContext,
    PrefixAllowListResolver,
    VocabularyResolver,
    VocabularyStatus,
)
from graflo.architecture.profile.model import (
    AssertionResult,
    Finding,
    ProfileReport,
    ProfileWaivers,
    Severity,
    Status,
    Waiver,
)
from graflo.architecture.profile.runner import (
    Assertion,
    Profile,
    check_manifest,
    check_manifest_config,
    get_profile,
    list_profiles,
    run_profile,
)
from graflo.architecture.profile.world_model import WORLD_MODEL_PROFILE

__all__ = [
    "WORLD_MODEL_PROFILE",
    "Assertion",
    "AssertionResult",
    "CheckContext",
    "Finding",
    "PrefixAllowListResolver",
    "Profile",
    "ProfileReport",
    "ProfileWaivers",
    "Severity",
    "Status",
    "VocabularyResolver",
    "VocabularyStatus",
    "Waiver",
    "check_manifest",
    "check_manifest_config",
    "get_profile",
    "list_profiles",
    "run_profile",
]
