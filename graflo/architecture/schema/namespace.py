"""Physical graph namespace: the database, graph or space a schema deploys into.

``Schema.metadata.name`` is a **label**. It is free-form on purpose: merges fold
two names into ``left+right``, agents write prose into it, and it is excluded
from every content hash. It is not an identifier, and handing it verbatim to
``CREATE DATABASE`` / ``CREATE GRAPH`` / ``CREATE SPACE`` fails on the first
character a backend does not accept.

The namespace a schema actually deploys into is resolved here, in one place,
with one precedence:

1. an explicit override (a call argument or connection config), validated;
2. ``db_profile.target_namespace``, validated -- an explicit value is refused
   when the flavor would reject it, never silently rewritten;
3. :func:`sanitize_namespace` over ``metadata.name`` -- a deterministic,
   idempotent projection into the flavor's identifier rules.

The derived name is deliberately *not* stored back onto the profile.
``db_profile`` is part of the schema's content hash and ``metadata`` is not, so
a stored mirror would make renaming a schema move its content address, and
merging two schemas would have two auto-filled mirrors to reconcile.
"""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from typing import TYPE_CHECKING

from graflo.onto import DBType

if TYPE_CHECKING:
    from .document import Schema

logger = logging.getLogger(__name__)

#: Used when a label sanitizes to nothing (empty, or no ASCII-representable
#: character at all).
FALLBACK_NAMESPACE = "graph"

#: Documented namespace length limits; flavors absent here are not truncated.
_MAX_LENGTH: dict[DBType, int] = {
    DBType.ARANGO: 64,
    DBType.NEO4J: 63,
}

_NEO4J_MIN_LENGTH = 3
_HASH_LENGTH = 8

_NEO4J_VALID = re.compile(r"[a-z][a-z0-9.-]*", re.IGNORECASE)
_ARANGO_VALID = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")
_WORD_VALID = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PATH_UNSAFE = re.compile(r"[/\\\x00-\x1f]")


class InvalidNamespaceError(ValueError):
    """An explicit namespace the target flavor would reject."""


def _ascii(name: str) -> str:
    return unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()


def _truncate(value: str, flavor: DBType, sep: str) -> str:
    limit = _MAX_LENGTH.get(flavor)
    if limit is None or len(value) <= limit:
        return value
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:_HASH_LENGTH]
    head = value[: limit - _HASH_LENGTH - len(sep)].rstrip(sep)
    return f"{head}{sep}{digest}"


#: Characters a derived namespace may keep, per flavor. A label already made of
#: these passes through unchanged, so deployments whose name was valid before
#: resolution existed keep their namespace.
_KEEP: dict[DBType, str] = {
    DBType.ARANGO: "A-Za-z0-9_-",
    DBType.TIGERGRAPH: "A-Za-z0-9_",
    DBType.NEBULA: "A-Za-z0-9_",
}
#: Flavors without documented namespace rules: graph keys and single-database
#: backends, where a conservative, path- and query-safe form is still wanted.
_KEEP_DEFAULT = "A-Za-z0-9_-"


def _sanitize_neo4j(name: str) -> str:
    # Neo4j names are case-insensitive and stored lowercase; `_` is illegal.
    # Dots are legal but address composite-database constituents, so a label's
    # dots are folded like every other separator.
    value = re.sub(r"[^a-z0-9-]+", "-", name.lower())
    if not re.search(r"[a-z0-9]", value):
        value = FALLBACK_NAMESPACE
    if not value[0].isalpha() or value.startswith("system"):
        value = f"g-{value}"
    if len(value) < _NEO4J_MIN_LENGTH:
        value = f"{value}-db"
    return _truncate(value, DBType.NEO4J, "-")


def _sanitize_word(name: str, flavor: DBType) -> str:
    keep = _KEEP.get(flavor, _KEEP_DEFAULT)
    value = re.sub(f"[^{keep}]+", "_", name)
    if not re.search(r"[A-Za-z0-9]", value):
        value = FALLBACK_NAMESPACE
    # Arango needs a leading letter; TigerGraph and Nebula also accept `_`.
    first = value[0]
    needs_prefix = (
        not first.isalpha()
        if flavor == DBType.ARANGO
        else flavor in (DBType.TIGERGRAPH, DBType.NEBULA) and first.isdigit()
    )
    if needs_prefix:
        value = f"g_{value}"
    if flavor == DBType.TIGERGRAPH:
        from graflo.db.util import (
            load_tigergraph_identifier_rules,
            sanitize_tigergraph_identifier,
        )

        rules = load_tigergraph_identifier_rules()
        if rules is not None:
            value = sanitize_tigergraph_identifier(
                value,
                set(rules.reserved_words_upper),
                rules.forbidden_prefixes,
                rules.invalid_characters,
                suffix="_graph",
            )
    return _truncate(value, flavor, "_")


def sanitize_namespace(name: str, flavor: DBType) -> str:
    """Project a free-form schema label onto a namespace *flavor* accepts.

    Only what *flavor* would reject is rewritten: every run of disallowed
    characters becomes one separator, a leading character the flavor refuses
    gets a ``g_`` prefix, and TigerGraph reserved words and forbidden prefixes
    are escaped. Neo4j names are additionally lowercased, ``-`` separated and
    padded to three characters. Over-long results keep a stable hash of the
    full name as a suffix, so two long labels sharing a prefix do not collide.
    Flavors without documented rules keep letters, digits, ``_`` and ``-``.

    Deterministic and idempotent: ``sanitize_namespace(sanitize_namespace(x, f), f)``
    equals ``sanitize_namespace(x, f)``.

    Args:
        name: The schema label, typically ``Schema.metadata.name``.
        flavor: Target backend.

    Returns:
        A namespace that passes :func:`validate_namespace` for *flavor*.
    """
    flavor = DBType(flavor)
    ascii_name = _ascii(name)
    if flavor == DBType.NEO4J:
        return _sanitize_neo4j(ascii_name)
    return _sanitize_word(ascii_name, flavor)


def namespace_problem(name: str, flavor: DBType) -> str | None:
    """Why *flavor* would reject *name* as a namespace, or ``None`` if it would not."""
    flavor = DBType(flavor)
    if not name:
        return "namespace cannot be empty"
    limit = _MAX_LENGTH.get(flavor)
    if limit is not None and len(name) > limit:
        return f"longer than {limit} characters"
    if flavor == DBType.NEO4J:
        if len(name) < _NEO4J_MIN_LENGTH:
            return f"shorter than {_NEO4J_MIN_LENGTH} characters"
        if not _NEO4J_VALID.fullmatch(name):
            return "must start with a letter and hold only letters, digits, '.' and '-'"
        if name.lower().startswith("system"):
            return "must not start with 'system'"
        return None
    if flavor == DBType.ARANGO:
        if not _ARANGO_VALID.fullmatch(name):
            return "must start with a letter and hold only letters, digits, '_' and '-'"
        return None
    if flavor in (DBType.TIGERGRAPH, DBType.NEBULA):
        if not _WORD_VALID.fullmatch(name):
            return (
                "must start with a letter or '_' and hold only letters, digits and '_'"
            )
        if flavor == DBType.TIGERGRAPH:
            from graflo.db.tigergraph.name_validation import (
                validate_tigergraph_schema_name,
            )

            try:
                validate_tigergraph_schema_name(name, "graph")
            except ValueError as exc:
                return str(exc)
        return None
    if _PATH_UNSAFE.search(name):
        return "must not contain path separators or control characters"
    return None


def validate_namespace(name: str, flavor: DBType) -> None:
    """Refuse an explicit namespace *flavor* would reject.

    Raises:
        InvalidNamespaceError: naming the problem and the sanitized spelling.
    """
    flavor = DBType(flavor)
    problem = namespace_problem(name, flavor)
    if problem is not None:
        raise InvalidNamespaceError(
            f"Invalid {flavor.value} namespace {name!r}: {problem}. "
            f"A valid spelling is {sanitize_namespace(name, flavor)!r}."
        )


def resolve_namespace(
    schema: Schema,
    flavor: DBType | None = None,
    override: str | None = None,
) -> str:
    """The namespace *schema* deploys into on *flavor*.

    Precedence: *override*, then ``db_profile.target_namespace`` (both
    validated), then the sanitized ``metadata.name``.

    Args:
        schema: The schema being deployed.
        flavor: Target backend; defaults to ``schema.db_profile.db_flavor``.
        override: An explicit namespace from the caller, e.g. a call argument.

    Raises:
        InvalidNamespaceError: An explicit namespace *flavor* would reject.
    """
    target = DBType(flavor if flavor is not None else schema.db_profile.db_flavor)
    explicit = override if override is not None else schema.db_profile.target_namespace
    if explicit is not None:
        validate_namespace(explicit, target)
        return explicit
    label = schema.metadata.name
    resolved = sanitize_namespace(label, target)
    if resolved != label:
        logger.info(
            "Schema name %r is not a valid %s namespace; deploying into %r "
            "(set db_profile.target_namespace to choose one)",
            label,
            target.value,
            resolved,
        )
    return resolved


__all__ = [
    "FALLBACK_NAMESPACE",
    "InvalidNamespaceError",
    "namespace_problem",
    "resolve_namespace",
    "sanitize_namespace",
    "validate_namespace",
]
