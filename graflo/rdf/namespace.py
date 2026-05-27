"""GraFlo RDF namespace and vocabulary term constants."""

from __future__ import annotations

from rdflib import Namespace
from rdflib.term import URIRef

from graflo.architecture.contract.bindings.connectors import (
    FileConnector as FileConnectorModel,
    SparqlConnector as SparqlConnectorModel,
    TableConnector as TableConnectorModel,
)

GF_ONTOLOGY_IRI = "https://ontology.growgraph.dev/graflo"
GF_VERSION = "1.0.0"
GF_VERSION_IRI = f"{GF_ONTOLOGY_IRI}/{GF_VERSION}"
GF_BASE = "https://ontology.growgraph.dev/graflo/"
GF = Namespace(GF_BASE)

# Classes
GraphManifest = GF.GraphManifest
Schema = GF.Schema
CoreSchema = GF.CoreSchema
VertexConfig = GF.VertexConfig
EdgeConfig = GF.EdgeConfig
GraphMetadata = GF.GraphMetadata
DatabaseProfile = GF.DatabaseProfile
Vertex = GF.Vertex
Edge = GF.Edge
Field = GF.Field
Identity = GF.Identity
IngestionModel = GF.IngestionModel
Resource = GF.Resource
EdgeInferSpec = GF.EdgeInferSpec
ProtoTransform = GF.ProtoTransform
Transform = GF.Transform
DressConfig = GF.DressConfig
KeySelectionConfig = GF.KeySelectionConfig
Actor = GF.Actor
VertexProducingActor = GF.VertexProducingActor
VertexActorStep = GF.VertexActor
EdgeActorStep = GF.EdgeActor
TransformActorStep = GF.TransformActor
DescendActorStep = GF.DescendActor
VertexRouterActorStep = GF.VertexRouterActor
Bindings = GF.Bindings
BoundConnector = GF.BoundConnector
FileConnector = GF.FileConnector
TableConnector = GF.TableConnector
SparqlConnector = GF.SparqlConnector
ResourceConnectorBinding = GF.ResourceConnectorBinding
ConnectorConnectionBinding = GF.ConnectorConnectionBinding
StagingProxyBinding = GF.StagingProxyBinding
EdgePhysicalSpec = GF.EdgePhysicalSpec
Index = GF.Index

# Object properties
hasSchema = GF.hasSchema
hasIngestionModel = GF.hasIngestionModel
hasBindings = GF.hasBindings
hasCoreSchema = GF.hasCoreSchema
hasVertexConfig = GF.hasVertexConfig
hasEdgeConfig = GF.hasEdgeConfig
hasMetadata = GF.hasMetadata
hasDatabaseProfile = GF.hasDatabaseProfile
hasVertex = GF.hasVertex
hasEdge = GF.hasEdge
hasField = GF.hasField
hasIdentity = GF.hasIdentity
edgeSource = GF.edgeSource
edgeTarget = GF.edgeTarget
hasResource = GF.hasResource
hasTransform = GF.hasTransform
hasActor = GF.hasActor
targetsVertex = GF.targetsVertex
targetsEdge = GF.targetsEdge
executesTransform = GF.executesTransform
hasDress = GF.hasDress
hasKeySelection = GF.hasKeySelection
hasConnector = GF.hasConnector
bindsResourceToConnector = GF.bindsResourceToConnector
bindsConnectorToConnProxy = GF.bindsConnectorToConnProxy
hasStagingProxy = GF.hasStagingProxy
hasVertexIndex = GF.hasVertexIndex
hasEdgeSpec = GF.hasEdgeSpec
refinesEdge = GF.refinesEdge
hasIndex = GF.hasIndex
hasEdgeInferOnly = GF.hasEdgeInferOnly
hasEdgeInferExcept = GF.hasEdgeInferExcept

# Datatype properties
name = GF.name
version = GF.version
description = GF.description
enumValue = GF.enumValue
fieldType = GF.fieldType
dbFlavor = GF.dbFlavor
targetNamespace = GF.targetNamespace
identityName = GF.identityName
relation = GF.relation
blank = GF.blank
edgesOnDuplicate = GF.edgesOnDuplicate
resourceName = GF.resourceName
connectorName = GF.connectorName
connProxy = GF.connProxy
actorType = GF.actorType
stepIndex = GF.stepIndex
stepPayload = GF.stepPayload
transformModule = GF.transformModule
transformFunction = GF.transformFunction
transformInput = GF.transformInput
transformOutput = GF.transformOutput
transformParams = GF.transformParams
transformTarget = GF.transformTarget
transformStrategy = GF.transformStrategy
renameMap = GF.renameMap
dressKey = GF.dressKey
dressValue = GF.dressValue
keySelectionMode = GF.keySelectionMode
keySelectionName = GF.keySelectionName
boundSourceKind = GF.boundSourceKind
connectorPayload = GF.connectorPayload
resourcePayload = GF.resourcePayload
profilePayload = GF.profilePayload
edgePayload = GF.edgePayload
vertexPayload = GF.vertexPayload
artifactIndex = GF.artifactIndex
forceTypes = GF.forceTypes
identityFromAllProperties = GF.identityFromAllProperties
edgeIdentities = GF.edgeIdentities
edgeType = GF.edgeType
edgeBy = GF.edgeBy
vertexIndexes = GF.vertexIndexes
edgeSpecs = GF.edgeSpecs
profileVertexName = GF.profileVertexName
indexName = GF.indexName
indexField = GF.indexField
indexUnique = GF.indexUnique
indexType = GF.indexType
indexDeduplicate = GF.indexDeduplicate
indexSparse = GF.indexSparse
indexExcludeEdgeEndpoints = GF.indexExcludeEdgeEndpoints
specSource = GF.specSource
specTarget = GF.specTarget
specRelation = GF.specRelation
specPurpose = GF.specPurpose
specRelationName = GF.specRelationName
specIndexesMode = GF.specIndexesMode

# Actor step type mapping
ACTOR_STEP_CLASSES: dict[str, object] = {
    "vertex": VertexActorStep,
    "edge": EdgeActorStep,
    "transform": TransformActorStep,
    "descend": DescendActorStep,
    "vertex_router": VertexRouterActorStep,
}

# DBType enum value -> named individual
DB_TYPE_INDIVIDUALS: dict[str, object] = {
    "arango": GF.ArangoDB,
    "neo4j": GF.Neo4j,
    "tigergraph": GF.TigerGraph,
    "falkordb": GF.FalkorDB,
    "memgraph": GF.Memgraph,
    "nebula": GF.Nebula,
    "postgres": GF.Postgres,
    "mysql": GF.MySQL,
    "mongodb": GF.MongoDB,
    "sqlite": GF.SQLite,
    "sparql": GF.SparqlEndpoint,
}

FIELD_TYPE_INDIVIDUALS: dict[str, object] = {
    "INT": GF.INT,
    "UINT": GF.UINT,
    "FLOAT": GF.FLOAT,
    "DOUBLE": GF.DOUBLE,
    "BOOL": GF.BOOL,
    "STRING": GF.STRING,
    "DATETIME": GF.DATETIME,
}

BOUND_SOURCE_KIND_INDIVIDUALS: dict[str, object] = {
    "file": GF.FileSource,
    "sql_table": GF.SqlTableSource,
    "sparql": GF.SparqlSource,
}

TRANSFORM_TARGET_INDIVIDUALS: dict[str, object] = {
    "values": GF.ValuesTarget,
    "keys": GF.KeysTarget,
}

TRANSFORM_STRATEGY_INDIVIDUALS: dict[str, object] = {
    "single": GF.SingleStrategy,
    "each": GF.EachStrategy,
    "all": GF.AllStrategy,
}

KEY_SELECTION_MODE_INDIVIDUALS: dict[str, object] = {
    "all": GF.AllKeysMode,
    "include": GF.IncludeKeysMode,
    "exclude": GF.ExcludeKeysMode,
}

EDGE_DUPLICATE_POLICY_INDIVIDUALS: dict[str, object] = {
    "ignore": GF.IgnoreDuplicate,
    "upsert": GF.UpsertDuplicate,
}

CONNECTOR_CLASSES: dict[str, object] = {
    "FileConnector": FileConnector,
    "TableConnector": TableConnector,
    "SparqlConnector": SparqlConnector,
}

CONNECTOR_CLASS_BY_RDF_TYPE: dict[URIRef, str] = {
    FileConnector: "FileConnector",
    TableConnector: "TableConnector",
    SparqlConnector: "SparqlConnector",
}

CONNECTOR_MODELS = {
    "FileConnector": FileConnectorModel,
    "TableConnector": TableConnectorModel,
    "SparqlConnector": SparqlConnectorModel,
}

ENUM_REGISTRIES: dict[str, dict[str, object]] = {
    "db_type": DB_TYPE_INDIVIDUALS,
    "field_type": FIELD_TYPE_INDIVIDUALS,
    "bound_source_kind": BOUND_SOURCE_KIND_INDIVIDUALS,
    "transform_target": TRANSFORM_TARGET_INDIVIDUALS,
    "transform_strategy": TRANSFORM_STRATEGY_INDIVIDUALS,
    "key_selection_mode": KEY_SELECTION_MODE_INDIVIDUALS,
    "edge_duplicate_policy": EDGE_DUPLICATE_POLICY_INDIVIDUALS,
}

MODEL_PAYLOAD_EXCLUDES: dict[str, set[str]] = {
    "database_profile": {
        "db_flavor",
        "target_namespace",
        "vertex_indexes",
        "edge_specs",
    },
    "resource": {"name", "pipeline"},
    "connector": {"hash", "name", "resource_name"},
}
