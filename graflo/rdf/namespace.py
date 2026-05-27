"""GraFlo RDF namespace and vocabulary term constants."""

from __future__ import annotations

from rdflib import Namespace

GF_ONTOLOGY_IRI = "https://ontology.growgraph.dev/graflo"
GF_VERSION = "1.0.0"
GF_VERSION_IRI = f"{GF_ONTOLOGY_IRI}/{GF_VERSION}"
GF_BASE = "https://ontology.growgraph.dev/graflo/"
GF = Namespace(GF_BASE)

# Classes
GraphManifest = GF.GraphManifest
Schema = GF.Schema
CoreSchema = GF.CoreSchema
GraphMetadata = GF.GraphMetadata
DatabaseProfile = GF.DatabaseProfile
VertexDefinition = GF.VertexDefinition
EdgeDefinition = GF.EdgeDefinition
FieldDefinition = GF.FieldDefinition
IngestionModel = GF.IngestionModel
ResourceDefinition = GF.ResourceDefinition
EdgeInferSpec = GF.EdgeInferSpec
ProtoTransform = GF.ProtoTransform
Transform = GF.Transform
DressConfig = GF.DressConfig
KeySelectionConfig = GF.KeySelectionConfig
ActorStep = GF.ActorStep
VertexActorStep = GF.VertexActorStep
EdgeActorStep = GF.EdgeActorStep
TransformActorStep = GF.TransformActorStep
DescendActorStep = GF.DescendActorStep
VertexRouterActorStep = GF.VertexRouterActorStep
BindingsConfig = GF.BindingsConfig
BoundConnector = GF.BoundConnector
FileConnector = GF.FileConnector
TableConnector = GF.TableConnector
SparqlConnector = GF.SparqlConnector
ResourceConnectorBinding = GF.ResourceConnectorBinding
ConnectorConnectionBinding = GF.ConnectorConnectionBinding
StagingProxyBinding = GF.StagingProxyBinding

# Object properties
hasSchema = GF.hasSchema
hasIngestionModel = GF.hasIngestionModel
hasBindings = GF.hasBindings
hasCoreSchema = GF.hasCoreSchema
hasMetadata = GF.hasMetadata
hasDatabaseProfile = GF.hasDatabaseProfile
definesVertex = GF.definesVertex
definesEdge = GF.definesEdge
hasField = GF.hasField
edgeSource = GF.edgeSource
edgeTarget = GF.edgeTarget
hasResource = GF.hasResource
hasTransform = GF.hasTransform
hasPipelineStep = GF.hasPipelineStep
hasDress = GF.hasDress
hasKeySelection = GF.hasKeySelection
hasConnector = GF.hasConnector
bindsResourceToConnector = GF.bindsResourceToConnector
bindsConnectorToConnProxy = GF.bindsConnectorToConnProxy
hasStagingProxy = GF.hasStagingProxy
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
identityField = GF.identityField
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
