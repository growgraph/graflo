# How do I hand my manifest to a tool that does not use GraFlo?

GraFlo writes a manifest in three formats that other tools read without GraFlo:
a JSON Schema that checks a manifest, the schema as an OWL ontology with SHACL
shapes, and the manifest as Apache Ossie documents. At the end you have the
files and know what each one leaves out.

For the schema in a few lines of YAML, or as a neo4j-graphrag `GraphSchema`,
see [Writing a short schema](short_schema.md).

## What you need

- GraFlo installed; see [Installation](../getting_started/installation.md).
- A manifest with a `schema`. Ossie also reads its `ingestion_model` and
  `bindings`.

## Steps

### 1. Check a manifest without GraFlo

```bash
graflo json-schema -o gstl.schema.json
```

The schema (JSON Schema draft 2020-12, `$id`
`https://ontology.growgraph.dev/gstl/1.0/schema.json`) describes a manifest as
GraFlo writes it, with pipeline steps in their short form (`{edge: {...}}`). Any
JSON Schema validator can use it. It checks shape only: whether an identity
field exists, or an edge's endpoints are declared, is checked by
`GraphManifest.finish_init()`.

### 2. Write the schema as OWL and SHACL

```bash
graflo schema-to-owl manifest.yaml --base-iri https://example.org/hr/ -o ontology.ttl
graflo schema-to-shacl manifest.yaml --base-iri https://example.org/hr/ -o shapes.ttl
```

| Schema | OWL |
|---|---|
| vertex | `owl:Class` |
| field | `owl:DatatypeProperty` with one domain and an XSD range; `owl:FunctionalProperty` unless a `LIST` |
| `identity`, `secondary_identities` | `owl:hasKey`, one axiom each |
| edge | `owl:ObjectProperty`, domain and range |
| `subclass_of`, `subproperty_of` | `rdfs:subClassOf`, `rdfs:subPropertyOf` |
| `inverses`, `symmetric` | `owl:inverseOf`, `owl:SymmetricProperty` |

Edge properties, edge identities, undirected and indirect edges have no OWL
construct; they travel in the `gf:edgeExtension` annotation. The shapes give each
vertex type a `sh:NodeShape`: identity fields exactly once, other single-valued
fields at most once, and each edge's target class.

IRIs: `semantics.iri` when set; otherwise `--base-iri`, else
`metadata.base_iri`, else `urn:graflo:<schema name>:`, followed by the vertex name,
`<Vertex>_<field>` for a field, and the relation for an edge
(`<source>_<relation>_<target>` when several edges share a relation). This naming
is provisional and may change in a minor release.

### 3. Write the manifest as Apache Ossie

```bash
graflo schema-to-ossie manifest.yaml -o ossie/
```

Experimental: written against Ossie `0.3.0.dev0`, which is not released. The
directory holds `<name>.ontology.yaml` and, when a resource is bound to a SQL
table, `<name>.semantic_model.yaml` and `<name>.mapping.yaml`.

- A vertex becomes an `EntityType` identified by its identity fields.
- An edge without properties becomes a relationship of its source.
- An edge with properties or identities becomes an `EntityType` identified by
  its two ends and its identity attributes.

`losses.md` lists what Ossie cannot express, for example:

- transforms and resources that are not plain vertex and edge steps;
- resources bound to files, APIs or SPARQL;
- symmetric, undirected and indirect edges, filters, and secondary identities;
- the database profile.

## What you should see

`ontology.ttl` and `shapes.ttl` load in any RDF tool. The Ossie documents
validate against Ossie's JSON Schemas. `losses.md` says `nothing` when the
manifest maps completely.
