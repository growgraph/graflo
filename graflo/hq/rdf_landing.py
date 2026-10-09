"""Land RDF facts on a manifest: read each class a resource handles from a file.

Facts extracted from documents arrive as an RDF file of instances (an ABox),
often with the ontology they were extracted against in a second file.
:func:`land_rdf_facts` turns them into manifest blocks:

- A manifest with a schema and resources keeps both. Every class in the facts
  whose local name is a resource gets a :class:`SparqlConnector` over the facts
  file (re-landing the same class replaces its connector); the other classes are
  reported as ``unbound``.
- A manifest without them takes schema, resources and bindings inferred from
  the ontology (:class:`RdfInferenceManager`), every connector reading the
  facts file.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.bindings import Bindings, SparqlConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.hq.rdf_inferencer import (
    RdfInferenceManager,
    _load_graph,
    _local_name,
    _object_edges,
    _ontology_classes,
    _typed_properties,
)
from graflo.onto import DBType

#: Vocabularies whose classes describe the RDF itself, not the domain.
_META_NAMESPACES = (
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/2001/XMLSchema#",
    "http://www.w3.org/ns/shacl#",
)


class LandedClass(ConfigBaseModel):
    """A class found in the facts."""

    iri: str
    name: str = Field(description="Local name; the resource it lands on")
    instances: int


class RdfLanding(ConfigBaseModel):
    """The manifest with the facts landed, and what landed where."""

    manifest: GraphManifest
    bound: list[LandedClass] = Field(
        default_factory=list, description="Classes now read from the facts file"
    )
    unbound: list[LandedClass] = Field(
        default_factory=list,
        description="Classes in the facts that no resource of the manifest handles",
    )


def fact_classes(facts: Path) -> list[LandedClass]:
    """Domain classes instantiated in *facts*, with instance counts, in IRI order."""
    from rdflib import RDF, URIRef

    graph = _load_graph(facts)
    counts: dict[str, int] = {}
    for _, cls in graph.subject_objects(RDF.type):
        iri = str(cls)
        if isinstance(cls, URIRef) and not iri.startswith(_META_NAMESPACES):
            counts[iri] = counts.get(iri, 0) + 1
    return [
        LandedClass(iri=iri, name=_local_name(iri), instances=counts[iri])
        for iri in sorted(counts)
    ]


def land_rdf_facts(
    manifest: GraphManifest | None,
    facts: Path,
    *,
    ontology: Path | None = None,
    db_flavor: DBType = DBType.ARANGO,
) -> RdfLanding:
    """Return *manifest* with *facts* bound to the resources that handle them.

    Args:
        manifest: The manifest to land on; ``None`` or one without a schema and
            resources starts from the ontology.
        facts: RDF file of instances. Its path is written into the connectors
            as given, so pass the path the ingesting process will read.
        ontology: RDF file declaring the classes. Required when the manifest has
            no schema; otherwise used only to read multi-range properties by
            class (``typed_objects``).
        db_flavor: Target flavour of a schema inferred from the ontology.

    Raises:
        ValueError: The manifest has no schema and no ontology was given.
    """
    found = fact_classes(facts)
    if (
        manifest is None
        or manifest.graph_schema is None
        or manifest.ingestion_model is None
    ):
        if ontology is None:
            raise ValueError(
                "An ontology is required to land facts on a manifest without "
                "a schema and resources."
            )
        landed = _from_ontology(manifest, facts, ontology, db_flavor)
    else:
        landed = _onto_resources(manifest, facts, ontology)

    landed.finish_init()
    resources = {r.name for r in landed.require_ingestion_model().resources}
    return RdfLanding(
        manifest=landed,
        bound=[c for c in found if c.name in resources],
        unbound=[c for c in found if c.name not in resources],
    )


def _from_ontology(
    manifest: GraphManifest | None, facts: Path, ontology: Path, db_flavor: DBType
) -> GraphManifest:
    inferencer = RdfInferenceManager(target_db_flavor=db_flavor)
    schema, ingestion_model = inferencer.infer_schema(ontology)
    bindings = inferencer.create_bindings(ontology, data_file=facts)
    return GraphManifest(
        graph_schema=schema,
        ingestion_model=ingestion_model,
        bindings=bindings,
        metadata=manifest.metadata if manifest is not None else None,
    )


def _onto_resources(
    manifest: GraphManifest, facts: Path, ontology: Path | None
) -> GraphManifest:
    typed: dict[str, list[str]] = {}
    if ontology is not None:
        graph = _load_graph(ontology)
        typed = _typed_properties(_object_edges(graph, _ontology_classes(graph)))

    bindings = (
        Bindings.from_dict(manifest.bindings.to_dict())
        if manifest.bindings is not None
        else Bindings()
    )
    resources = {r.name for r in manifest.require_ingestion_model().resources}
    for cls in fact_classes(facts):
        if cls.name not in resources:
            continue
        connector = SparqlConnector(
            rdf_class=cls.iri,
            rdf_file=facts,
            typed_objects=typed.get(cls.name, []),
        )
        previous = next(
            (
                c
                for c in bindings.get_connectors_for_resource(cls.name)
                if isinstance(c, SparqlConnector)
                and c.rdf_class == cls.iri
                and c.rdf_file is not None
            ),
            None,
        )
        if previous is not None:
            bindings.replace_connector(previous, connector)
        else:
            bindings.add_connector(connector)
            bindings.bind_resource(cls.name, connector)

    return manifest.model_copy(update={"bindings": bindings})


__all__ = ["LandedClass", "RdfLanding", "fact_classes", "land_rdf_facts"]
