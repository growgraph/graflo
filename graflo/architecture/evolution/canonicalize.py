"""Canonical form: the payload a content hash is taken over.

Two manifests that describe the same world model must hash equal. The minimal
canonical dict produced by
:meth:`~graflo.architecture.base.ConfigBaseModel.to_minimal_canonical_dict`
already normalizes defaults, ``None``, aliases and key order. What it does not
normalize is **list order** -- and most lists in the contract are declaration
order over a set, so two identical schemas authored in different order, or one
authored and one replayed (``apply_add_vertices`` appends), hash differently.

This module adds exactly that one normalization, driven by :data:`LIST_ORDER`
below: a total classification of every list-typed field reachable from
``GraphManifest`` as :attr:`ListOrder.SORTED` or :attr:`ListOrder.PRESERVED`.

The asymmetry that decides every doubtful case
----------------------------------------------

The two mistakes are not equally bad.

* Marking an order-**significant** list ``SORTED`` makes two *different* world
  models hash **equal**. A false positive: lineage merges two things that are
  not the same, and nothing downstream can detect it.
* Marking an order-**insignificant** list ``PRESERVED`` makes two *identical*
  world models hash **differently**. A false negative: a missed dedup, a
  redundant commit. Visible, harmless, and fixable later by moving the entry.

So **when in doubt, preserve**. Every ``SORTED`` entry is a positive claim that
order carries no meaning in that field, and needs a reason. ``PRESERVED`` is the
safe default and is used for anything whose ordering semantics are not settled.

Sorting is by the canonical JSON rendering of each element, not by a per-field
key. That is total over heterogeneous unions (``BindingsRegistry.connectors``
holds five connector types), needs no tie-break rule, and cannot be
input-order-dependent: elements that compare equal are byte-identical and
therefore interchangeable. Nothing reads this order -- it is hash-side only, and
authored YAML keeps its declaration order.

Nesting is independent. ``Edge.identities`` is ``list[list[str]]`` marked
``SORTED``: the outer list of alternative keys is sorted, while each inner
composite key keeps its order, because ``(a, b)`` and ``(b, a)`` are different
keys.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any

#: Mixed into the hashed bytes so a future change to this module's rules is
#: explicit and collision-free rather than a silent reinterpretation of old
#: hashes. Bump it whenever :data:`LIST_ORDER` or the sort rule changes.
CANON_VERSION: str = "graflo/canon@2"


class ListOrder(str, Enum):
    """Whether a list field's order carries meaning."""

    #: Order is not meaning: the list is a set written down in some order.
    SORTED = "sorted"
    #: Order is meaning: a program, a precedence chain, a composite key, a
    #: projection, or anything not yet established to be otherwise.
    PRESERVED = "preserved"


SORTED = ListOrder.SORTED
PRESERVED = ListOrder.PRESERVED

#: The audit table. Keyed by ``(model class name, field name)`` -- names only,
#: so this module imports no contract model and adds no layering edge.
#:
#: Every list-typed field reachable from ``GraphManifest`` must appear here;
#: ``test_canonicalize.py`` walks the model tree and fails on any that does not,
#: so a new list field cannot ship silently classified.
LIST_ORDER: dict[tuple[str, str], ListOrder] = {
    # ── Schema: vertices, edges, fields ─────────────────────────────────────
    # Declaration sets, matched by name or endpoints everywhere they are read.
    ("VertexConfig", "vertices"): SORTED,
    ("EdgeConfig", "edges"): SORTED,
    ("Vertex", "properties"): SORTED,
    ("Edge", "properties"): SORTED,
    ("Vertex", "secondary_identities"): SORTED,
    # Alternative edge keys, already deduped by tuple in `normalize_identity_keys`
    # and read by iterating all of them -- no precedence. Each inner key keeps
    # its own order, which the sort does not touch.
    ("Edge", "identities"): SORTED,
    # ── Identity: preserved, deliberately ───────────────────────────────────
    # `Vertex.identity` order is load-bearing at two confirmed sites: a
    # backend's `vertex_address` resolves an endpoint through the *first*
    # identity field present, and PostgreSQL's edge FK is `identity_fields[0]`
    # (CORE-PG-002). Composite primary-key column order is significant besides.
    ("Vertex", "identity"): PRESERVED,
    # `identity_digest._digest` dumps its payload with `sort_keys=True`, so
    # reordering these two does *not* change the digest they feed -- they could
    # be SORTED. They are preserved anyway: the evidence is one implementation
    # detail of a function whose docstring says "do not add keys to this
    # payload", and a false positive on identity is the worst outcome this
    # table can produce. Moving them costs a CANON_VERSION bump, nothing more.
    ("Vertex", "hash_identity_properties"): PRESERVED,
    ("IdentityBranch", "fields"): PRESERVED,
    ("IdentityBranch", "when_all_present"): PRESERVED,
    # First branch whose fields are all present wins: this is a precedence
    # chain, and the ontology records it with an explicit `gf:artifactIndex`.
    ("IdentityFunnel", "branches"): PRESERVED,
    # A composite lookup key: order decides the key tuple and the index column
    # order, exactly as for `Vertex.identity`.
    ("SecondaryIdentity", "fields"): PRESERVED,
    # ── Indexes and physical profile ────────────────────────────────────────
    # Compound-index column order is significant to every backend that has one.
    ("Index", "fields"): PRESERVED,
    # ...but the set of indexes on a vertex or edge is not ordered.
    ("DatabaseProfile", "vertex_indexes"): SORTED,
    ("DatabaseProfile", "edge_specs"): SORTED,
    ("EdgePhysicalSpec", "indexes"): SORTED,
    ("DefaultPropertyValues", "edges"): SORTED,
    # ── Semantics: sets of external terms ───────────────────────────────────
    ("Semantics", "exact_match"): SORTED,
    ("Semantics", "synonyms"): SORTED,
    ("FieldSemantics", "exact_match"): SORTED,
    ("FieldSemantics", "synonyms"): SORTED,
    # ── Ingestion ───────────────────────────────────────────────────────────
    # Registries, resolved by name.
    ("IngestionModel", "resources"): SORTED,
    ("IngestionModel", "transforms"): SORTED,
    # An ordered actor program. The single most order-significant field in the
    # contract, and the reason a pipeline is one indivisible slot to three-way
    # merge as well.
    ("ResourceConfig", "pipeline"): PRESERVED,
    # Selector and membership sets.
    ("ResourceConfig", "merge_collections"): SORTED,
    ("ResourceConfig", "infer_edge_only"): SORTED,
    ("ResourceConfig", "infer_edge_except"): SORTED,
    ("ResourceConfig", "extra_weights"): SORTED,
    ("ResourceExtraWeightEntry", "vertex_weights"): SORTED,
    # A field tuple feeding a weight lookup; preserved for the same reason as
    # every other field tuple here.
    ("Weight", "fields"): PRESERVED,
    # A transform's argument binding: these name the doc fields fed to a
    # function's positional parameters and the fields its results are written
    # back to. Order *is* the binding. Declared as `tuple[str, ...]` rather than
    # a list, which is why the classification has to cover sequences and not
    # just `list` -- the runtime guard is what caught these.
    ("ProtoTransform", "input"): PRESERVED,
    ("ProtoTransform", "output"): PRESERVED,
    ("ProtoTransform", "input_groups"): PRESERVED,
    ("ProtoTransform", "output_groups"): PRESERVED,
    # Which document keys a key-selection step includes or excludes: membership,
    # not order. The mode (`all` / `include` / `exclude`) is a separate field.
    ("KeySelectionConfig", "names"): SORTED,
    # ── Bindings ────────────────────────────────────────────────────────────
    # Wiring entries and registries, all resolved by name.
    ("BindingsRegistry", "connectors"): SORTED,
    ("BindingsRegistry", "connector_templates"): SORTED,
    ("BindingsRegistry", "connector_connection"): SORTED,
    ("BindingsRegistry", "resource_connector"): SORTED,
    ("BindingsRegistry", "staging_proxy"): SORTED,
    # Membership sets on a connector.
    ("KafkaConnector", "topics"): SORTED,
    ("APIConnector", "retry_status_forcelist"): SORTED,
    # SQL shape: join order and projection order are both meaning.
    ("TableConnector", "joins"): PRESERVED,
    ("TableConnector", "select_columns"): PRESERVED,
    ("JoinClause", "select_fields"): PRESERVED,
    # ── Filters ─────────────────────────────────────────────────────────────
    # A filter is an expression tree. `deps` are operands (comparison and
    # range operators are not commutative) and `value` is an operand tuple.
    # Both preserved: they render to six dialects and the orderings that are
    # safe to sort have not been established per operator.
    ("FilterExpression", "deps"): PRESERVED,
    ("FilterExpression", "value"): PRESERVED,
    ("TableConnector", "filters"): PRESERVED,
    ("Vertex", "filters"): PRESERVED,
}


#: Mapping fields whose *values* are still contract structure, so classification
#: continues through them.
#:
#: Descending into a mapping otherwise stops classification. Almost every dict
#: in the contract is free-form user payload -- transform ``params``, connector
#: ``row_annotations`` and ``headers``, ``force_types``, weight ``map`` and
#: ``filter``, property defaults -- and a list inside one of those is arbitrary
#: user data with no ordering semantics GraFlo may assert. Preserving it is
#: right, and raising on it would be noise.
#:
#: ``DatabaseProfile.vertex_indexes`` (``dict[str, list[Index]]``) is currently
#: the only typed mapping in the tree. A new one added later is not classified
#: by default -- the safe direction -- and ``test_canonicalize.py`` flags it.
CLASSIFIED_MAPPINGS: frozenset[tuple[str, str]] = frozenset(
    {
        ("DatabaseProfile", "vertex_indexes"),
    }
)


class UnclassifiedListField(LookupError):
    """A list field reached during canonicalization is absent from LIST_ORDER.

    Raised rather than guessed: a default of either kind would silently decide
    a hash question that this module exists to make explicit.
    """

    def __init__(self, model_name: str, field_name: str) -> None:
        super().__init__(
            f"list field {model_name}.{field_name} is not classified in "
            f"LIST_ORDER ({__name__}). Add it as SORTED (order carries no "
            f"meaning -- state why) or PRESERVED (order is meaning, or is not "
            f"settled), and bump CANON_VERSION if any existing entry moved."
        )
        self.model_name = model_name
        self.field_name = field_name


def _sort_key(element: Any) -> str:
    """Total order over canonically rendered elements."""
    return json.dumps(element, sort_keys=True, separators=(",", ":"), default=str)


def _model_fields(obj: Any) -> dict[str, Any] | None:
    """Pydantic ``model_fields`` for *obj*, or None when it is not a model."""
    fields = getattr(type(obj), "model_fields", None)
    return fields if isinstance(fields, dict) else None


def _dump_key(name: str, info: Any) -> str:
    """The key *name* is rendered under by ``model_dump(by_alias=True)``.

    ``serialization_alias`` must be consulted first: the contract sets several
    fields through ``AliasChoices`` (``graph_schema`` dumps as ``schema``), and
    those leave ``alias`` itself as ``None``. Reading only ``alias`` silently
    misses the key in the payload and skips that whole subtree -- which reads
    exactly like the canonicalization not being applied at all.
    """
    return getattr(info, "serialization_alias", None) or info.alias or name


def _canonicalize_node(obj: Any, payload: Any) -> Any:
    """Rewrite *payload* -- the dump of *obj* -- into canonical order.

    Walks the model instances and their rendered payload in lockstep: the
    instances carry the exact classes the audit table is keyed by, and the
    payload carries the serialization rules already settled elsewhere. Both
    orders agree because ``model_dump`` preserves list order.
    """
    fields = _model_fields(obj)
    if fields is None or not isinstance(payload, dict):
        return payload

    for name, info in fields.items():
        key = _dump_key(name, info)
        if key not in payload:
            continue
        payload[key] = _canonicalize_value(
            getattr(obj, name, None),
            payload[key],
            model_name=type(obj).__name__,
            field_name=name,
            classify=True,
        )
    return payload


def _canonicalize_value(
    value: Any,
    node: Any,
    *,
    model_name: str,
    field_name: str,
    classify: bool,
) -> Any:
    """Canonicalize one rendered *node*, guided by its live *value*.

    ``classify`` says whether a list found here is still *this field's* list.
    Descending through a mapping keeps it true -- ``vertex_indexes`` is a dict
    of index lists, and each list is the field's own. Descending into a list's
    *elements* sets it false: an inner list is a nested structure with its own
    ordering semantics (a composite key inside ``Edge.identities``), never a
    second application of the outer field's policy. Entering a nested model
    resets attribution entirely and classification resumes there.
    """
    if _model_fields(value) is not None and isinstance(node, dict):
        return _canonicalize_node(value, node)

    if isinstance(node, list):
        items = value if isinstance(value, list) and len(value) == len(node) else None
        node = [
            _canonicalize_value(
                items[index] if items is not None else None,
                element,
                model_name=model_name,
                field_name=field_name,
                classify=False,
            )
            for index, element in enumerate(node)
        ]
        if not classify:
            return node
        order = LIST_ORDER.get((model_name, field_name))
        if order is None:
            raise UnclassifiedListField(model_name, field_name)
        return sorted(node, key=_sort_key) if order is SORTED else node

    if isinstance(node, dict):
        source = value if isinstance(value, dict) else {}
        inner = classify and (model_name, field_name) in CLASSIFIED_MAPPINGS
        return {
            map_key: _canonicalize_value(
                source.get(map_key),
                element,
                model_name=model_name,
                field_name=field_name,
                classify=inner,
            )
            for map_key, element in node.items()
        }

    return node


def canonical_payload(model: Any) -> Any:
    """Canonical, hashable payload for any GraFlo config model.

    Equivalent to ``to_minimal_canonical_dict()`` with every order-insignificant
    list sorted. Raises :class:`UnclassifiedListField` if the model tree reaches
    a list field the audit table does not classify.
    """
    payload = model.to_minimal_canonical_dict()
    return _canonicalize_node(model, payload)
