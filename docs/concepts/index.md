# Concepts

Here we introduce the main concepts of graflo, a framework for transforming data into property graphs.

## System Overview

graflo transforms data sources into property graphs through a pipeline of components:

1. **Data Sources** → **Resources** → **Actors** → **Vertices/Edges** → **Graph Database**

Each component plays a specific role in this transformation process.

## Core Components

### Schema
The `Schema` is the central configuration that defines how data sources are transformed into a property graph. It encapsulates:
 
- Vertex and edge definitions
- Resource mappings
- Data transformations
- Index configurations

### Vertex
A `Vertex` describes vertices and their database indexes. It supports:
 
- Single or compound indexes (e.g., `["first_name", "last_name"]` instead of `"full_name"`)
- Property definitions
- Filtering conditions
- Optional blank vertex configuration

### Edge
An `Edge` describes edges and their database indexes. It allows:
 
- Definition at any level of a hierarchical document
- Reliance on vertex principal index
- Weight configuration using `source_fields`, `target_fields`, and `direct` parameters
- Uniqueness constraints with respect to `source`, `target`, and `weight` fields

### Edge Attributes and Configuration

Edges in graflo support a rich set of attributes that enable flexible relationship modeling:

#### Basic Attributes
- **`source`**: Source vertex name (required)
- **`target`**: Target vertex name (required)
- **`indexes`**: List of database indexes for the edge
- **`weights`**: Optional weight configuration for edge properties

#### Relationship Type Configuration 
- **`relation`**: Explicit relationship name (primarily for Neo4j)
- **`relation_field`**: Field name containing relationship type values (for CSV/tabular data)
- **`relation_from_key`**: Use JSON key names as relationship types (for nested JSON data)

#### Weight Configuration
- **`weights.vertices`**: List of weight configurations from vertex properties
- **`weights.direct`**: List of direct field mappings as edge properties
- **`weights.source_fields`**: Fields from source vertex to use as weights
- **`weights.target_fields`**: Fields from target vertex to use as weights

#### Edge Behavior Control
- **`aux`**: Whether this is an auxiliary edge (collection created, but not considered by graflo)
- **`purpose`**: Additional identifier for utility collections between same vertex types

#### Matching and Filtering
- **`match_source`**: Select source items from a specific branch of json
- **`match_target`**: Select target items from a specific branch of json
- **`match`**: General matching field for edge creation

#### Advanced Configuration
- **`type`**: Edge type (DIRECT or INDIRECT)
- **`by`**: Vertex name for indirect edges
- **`graph_name`**: Custom graph name (auto-generated if not specified)
- **`collection_name`**: Custom collection name (auto-generated if not specified)
- **`db_flavor`**: Database flavor (ARANGO or NEO4J)

#### When to Use Different Attributes

**`relation_field`** (Example 3):
 
- Use with CSV/tabular data
- When relationship types are stored in a dedicated column
- For data like: `company_a, company_b, relation, date`

**`relation_from_key`** (Example 4):
 
- Use with nested JSON data
- When relationship types are implicit in the data structure
- For data like: `{"dependencies": {"depends": [...], "conflicts": [...]}}`

**`weights.direct`**:
 
- Use when you want to add properties directly to edges
- For temporal data (dates), quantitative values, or metadata
- Example: `weights: {direct: ["date", "confidence_score"]}`

**`match_source`/`match_target`**:
 
- For scenarios where we have multiple leaves of json containing the same vertex class
- Example: Creating edges between specific subsets of vertices

### Resource
A `Resource` is a set of mappings and transformations of a data source to vertices and edges, defined as a hierarchical structure of `Actors`. It supports:
 
- Table-like data (CSV, SQL)
- Tree-like data (JSON, XML)
- Complex nested structures

### Actor
An `Actor` describes how the current level of the document should be mapped/transformed to the property graph vertices and edges. There are four types that act on the provided document in this order:
 
- `DescendActor`: Navigates to the next level in the hierarchy. Supports:
  - `key`: Process a specific key in a dictionary
  - `any_key`: Process all keys in a dictionary (useful when you want to handle multiple keys dynamically)
- `TransformActor`: Applies data transformations
- `VertexActor`: Creates vertices from the current level
- `EdgeActor`: Creates edges between vertices

### Transform
A `Transform` defines data transforms, from renaming and type-casting to arbitrary transforms defined as Python functions. Transforms can be:
 
- Provided in the `transforms` section of `Schema`
- Referenced by their `name`
- Applied to both vertices and edges

## Key Features

### Schema Features
- **Flexible Indexing**: Support for compound indexes on vertices and edges
- **Hierarchical Edge Definition**: Define edges at any level of nested documents
- **Weighted Edges**: Configure edge weights from document fields or vertex properties
- **Blank Vertices**: Create intermediate vertices for complex relationships
- **Actor Pipeline**: Process documents through a sequence of specialized actors
- **Smart Navigation**: Automatic handling of both single documents and lists
- **Edge Constraints**: Ensure edge uniqueness based on source, target, and weight
- **Reusable Transforms**: Define and reference transformations by name
- **Vertex Filtering**: Filter vertices based on custom conditions

### Performance Optimization
- **Batch Processing**: Process large datasets in configurable batches (`batch_size` parameter of `Caster`)
- **Parallel Execution**: Utilize multiple cores for faster processing (`n_cores` parameter of `Caster`)
- **Efficient Resource Handling**: Optimized processing of both table and tree-like data
- **Smart Caching**: Minimize redundant operations

## Best Practices
1. Use compound indexes for frequently queried vertex properties
2. Leverage blank vertices for complex relationship modeling
3. Define transforms at the schema level for reusability
4. Configure appropriate batch sizes based on your data volume
5. Enable parallel processing for large datasets
6. Choose the right relationship attribute based on your data format:
   - `relation_field` - extract relation from document field
   - `relation_from_key` - extract relation from the key above
   - `relation` for explicit relationship names
7. Use edge weights to capture temporal or quantitative relationship properties
8. Leverage key matching (`match_source`, `match_target`) for complex matching scenarios

