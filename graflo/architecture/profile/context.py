"""What a profile check reads, and the vocabularies it recognises.

Two things live here. :class:`CheckContext` is the bundle every assertion is
handed -- the parsed manifest *and* the document the author actually wrote,
which are not interchangeable (see below). The rest is the vocabulary tables,
kept in one module so the IRIs a check recognises can be audited in one place
rather than found by grepping the assertion bodies.

**Why the authored document is carried alongside the model.** Two assertions
ask what the author *declared*, and the model has already lost that by the time
it exists: ``VertexConfig`` fills an unset ``Vertex.identity`` from every
property when ``identity_from_all_properties`` is set, and ``Edge.directed``
defaults to ``True``. So "identity was not declared" and "identity was declared
as every property" are the same model, and so are "directed was declared" and
"directed was defaulted". Every real caller has the authored mapping in hand --
a CLI reads the YAML, a pack holds its manifests as plain dicts, a registry
stores the config it was pushed -- so the check takes it rather than guessing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.profile.model import ProfileWaivers

# --- Vocabulary tables ------------------------------------------------------

#: Namespaces the bundled resolver recognises as live. Deliberately short: it
#: covers the vocabularies a world model is expected to ground in, and anything
#: outside it is reported as a warning rather than an error, because "we have
#: not heard of this namespace" is not the same claim as "this IRI is dead".
KNOWN_NAMESPACES: tuple[str, ...] = (
    "http://www.w3.org/ns/prov#",
    "http://www.w3.org/ns/sosa/",
    "http://www.w3.org/ns/ssn/",
    "http://www.w3.org/2006/time#",
    "http://qudt.org/schema/qudt/",
    "http://qudt.org/vocab/unit/",
    "http://www.w3.org/2004/02/skos/core#",
    "http://purl.org/dc/terms/",
    "https://schema.org/",
    "http://schema.org/",
    "http://xmlns.com/foaf/0.1/",
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/2000/01/rdf-schema#",
)

#: Properties that carry a unit *on the row* rather than in the schema. A type
#: whose instances each measure something different -- an abstract
#: ``Observation`` is the case that forces this -- cannot name one unit in its
#: contract without lying, so declaring the unit as data is the honest form.
UNIT_PROPERTY_IRIS: frozenset[str] = frozenset(
    {
        "http://qudt.org/schema/qudt/hasUnit",
        "http://qudt.org/schema/qudt/ucumCode",
        "http://qudt.org/schema/qudt/unit",
        "http://qudt.org/schema/qudt/hasQuantityKind",
    }
)

#: Datatype properties that assert a validity or observation time. All are
#: literal-ranged on purpose: OWL-Time's ``hasBeginning``/``hasEnd`` range over
#: ``time:Instant``, not over a literal, so grounding a DATETIME field in them
#: would be a claim that becomes false once schemas project to OWL.
TEMPORAL_PROPERTY_IRIS: frozenset[str] = frozenset(
    {
        "http://www.w3.org/ns/prov#generatedAtTime",
        "http://www.w3.org/ns/prov#invalidatedAtTime",
        "http://www.w3.org/ns/prov#startedAtTime",
        "http://www.w3.org/ns/prov#endedAtTime",
        "http://www.w3.org/ns/sosa/resultTime",
        "http://www.w3.org/ns/sosa/phenomenonTime",
    }
)

#: Edge groundings that attach a fact to where it came from.
PROVENANCE_EDGE_IRIS: frozenset[str] = frozenset(
    {
        "http://www.w3.org/ns/prov#wasDerivedFrom",
        "http://www.w3.org/ns/prov#wasAttributedTo",
        "http://www.w3.org/ns/prov#wasGeneratedBy",
        "http://www.w3.org/ns/prov#used",
        "http://www.w3.org/ns/sosa/madeBySensor",
    }
)

#: Vertex groundings that denote an actor a fact can be attributed to.
AGENT_TYPE_IRIS: frozenset[str] = frozenset(
    {
        "http://www.w3.org/ns/prov#Agent",
        "http://www.w3.org/ns/prov#SoftwareAgent",
        "http://www.w3.org/ns/sosa/Sensor",
        "http://xmlns.com/foaf/0.1/Agent",
    }
)


# --- Vocabulary liveness ----------------------------------------------------

VocabularyStatus = Literal["live", "unknown", "malformed"]


class VocabularyResolver(Protocol):
    """Decides whether an IRI resolves to a vocabulary worth grounding in.

    A protocol rather than a class so the bundled prefix check can be replaced
    by a real registry lookup without touching the report model or any of the
    surfaces that render it.
    """

    def resolve(self, iri: str) -> VocabularyStatus: ...


@dataclass(frozen=True, slots=True)
class PrefixAllowListResolver:
    """Recognises :data:`KNOWN_NAMESPACES`; everything else is ``unknown``.

    The honest v0.1 answer to "does this resolve to a *live* vocabulary": it
    checks the shape and the namespace and says so, rather than dereferencing
    anything. ``unknown`` is reported as a warning, never as a failure.
    """

    namespaces: tuple[str, ...] = KNOWN_NAMESPACES

    def resolve(self, iri: str) -> VocabularyStatus:
        if "://" not in iri or iri.startswith("://"):
            return "malformed"
        scheme = iri.split("://", 1)[0]
        if not scheme or not scheme.isascii() or " " in iri:
            return "malformed"
        if any(iri.startswith(ns) for ns in self.namespaces):
            return "live"
        return "unknown"


# --- The bundle handed to every assertion -----------------------------------


@dataclass(slots=True)
class CheckContext:
    """Everything an assertion may read.

    Attributes:
        manifest: The parsed, ``finish_init``-ed manifest.
        authored: The document the author wrote, when the caller has it.
            ``None`` degrades the two declaration assertions to warnings
            instead of letting them pass on a normalized model.
        waivers: Operator waivers to apply, if any.
        resolver: Vocabulary liveness backend.
    """

    manifest: GraphManifest
    authored: Mapping[str, Any] | None = None
    waivers: ProfileWaivers | None = None
    resolver: VocabularyResolver = field(default_factory=PrefixAllowListResolver)

    @property
    def has_authored(self) -> bool:
        """Whether declaration-sensitive assertions can be decided at all."""
        return self.authored is not None

    def authored_vertices(self) -> dict[str, Mapping[str, Any]]:
        """Authored vertex blocks by name; empty when the document is absent."""
        return {
            str(vertex["name"]): vertex
            for vertex in self._authored_path("vertex_config", "vertices")
            if isinstance(vertex, Mapping) and "name" in vertex
        }

    def authored_edges(self) -> list[Mapping[str, Any]]:
        """Authored edge blocks; empty when the document is absent."""
        return [
            edge
            for edge in self._authored_path("edge_config", "edges")
            if isinstance(edge, Mapping)
        ]

    def _authored_path(self, holder: str, key: str) -> list[Any]:
        """``schema.graph.<holder>.<key>`` out of the authored document.

        Two levels carry a serialized alias beside the field name --
        ``schema``/``graph_schema`` on the manifest and ``graph``/``core_schema``
        on the schema -- and authored documents use the aliases while a dumped
        model may use either. Reading only one spelling silently finds nothing,
        which would make the declaration assertions pass on every real file
        instead of failing loudly.
        """
        if self.authored is None:
            return []
        schema = self.authored.get("schema") or self.authored.get("graph_schema")
        if not isinstance(schema, Mapping):
            return []
        core = schema.get("graph") or schema.get("core_schema")
        if not isinstance(core, Mapping):
            return []
        block = core.get(holder)
        if not isinstance(block, Mapping):
            return []
        value = block.get(key)
        return list(value) if isinstance(value, list) else []


__all__ = [
    "AGENT_TYPE_IRIS",
    "KNOWN_NAMESPACES",
    "PROVENANCE_EDGE_IRIS",
    "TEMPORAL_PROPERTY_IRIS",
    "UNIT_PROPERTY_IRIS",
    "CheckContext",
    "PrefixAllowListResolver",
    "VocabularyResolver",
    "VocabularyStatus",
]
