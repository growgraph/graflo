# My data has no obvious key. How do I find out what identifies a record?

Three organizations send you their product catalogs in one file, and a
supplier list in another. Neither file has an id column. `product_code` looks
like a key, but it is not: each organization numbers its own catalog, so
`P000` appears three times. Keyed by `product_code` alone, the 150 products
would become 50 vertices, and three different products would be written as
one.

GraFlo can propose the key from the data. It looks for the smallest set of
columns whose values no two rows share, and writes it into the manifest for
you to review. Then you ingest with it.

## What you need

- GraFlo installed (`pip install graflo`).
- No database is needed. The graph is written to a directory, as in
  [the file backend example (14)](../14-file-backend-export/README.md).

## The data

[`data/products.csv`](data/products.csv) has 150 rows, 50 product codes for
each of three organizations. The first rows:

| org | product_code | name | category | updated_at |
|---|---|---|---|---|
| acme | P000 | Widget 0 | parts | 2024-01-15 |
| globex | P000 | Widget 1 | tools | 2024-02-15 |
| initech | P000 | Widget 2 | supplies | 2024-03-15 |
| acme | P001 | Widget 3 | parts | 2024-04-15 |

[`data/suppliers.csv`](data/suppliers.csv) has 120 rows with a
`supplier_code` (`SUP-0000`, ...), a `name` and a `country`.

## Steps

### 1. Declare the vertices without an identity

[`manifest.yaml`](manifest.yaml) lists the properties of each vertex type and
no `identity`. With `identity_from_all_properties: true`, a vertex without an
identity is identified by all its properties together. The manifest loads, but
only rows that agree in every column would count as the same product: a
product whose `updated_at` changes would become a second vertex.

```yaml
vertex_config:
    identity_from_all_properties: true
    vertices:
    -   name: product
        properties:
        -   org
        -   product_code
        -   name
        -   category
        -   updated_at
    -   name: supplier
        properties:
        -   supplier_code
        -   name
        -   country
```

### 2. Let GraFlo propose the identities

```bash
cd examples/15-identity-inference
uv run python infer.py
```

[`infer.py`](infer.py) passes the rows of each file to
`apply_identity_inference_to_vertices`:

```python
samples = {name: read_rows(path) for name, path in SAMPLE_FILES.items()}
vertices, results = apply_identity_inference_to_vertices(
    list(vertex_config.vertices), samples
)
```

For each vertex type, inference ranks the columns, preferring those whose name
ends in `id`, `key` or `code`. If some column has a different value in every
row, the best-ranked such column wins. Otherwise inference adds columns in rank
order until no two rows share the combination.

The script writes the manifest with the proposed identities to
[`artifacts/manifest-inferred.yaml`](artifacts/manifest-inferred.yaml), and sets
`identity_from_all_properties: false` there, so that loading it fails if a
vertex has no identity:

```yaml
-   name: product
    properties:
    -   name: org
    -   name: product_code
    -   name: name
    -   name: category
    -   name: updated_at
    identity:
    - product_code
    - org
```

### 3. Ingest with the proposed identities

```bash
uv run python ingest.py
```

[`ingest.py`](ingest.py) loads both files through the inferred manifest into
`artifacts/csv-backend`, then counts the records and their distinct identities.

## What you should see

`infer.py` prints:

```text
product   composite  identity=['product_code', 'org'] confidence=1.0
supplier  unary      identity=['supplier_code'] confidence=1.0
Wrote artifacts/manifest-inferred.yaml
```

`supplier_code` is unique, so it is a one-column (`unary`) key. `name` is
unique among the suppliers too; `supplier_code` wins because of its name.
`product_code` is not unique, so inference adds the next-ranked column, `org`,
and the pair is unique (`composite`). `confidence` is the share of five random
samples, each 80% of the rows, on which the key stayed unique.

`ingest.py` prints:

```text
graflo_backend target does not support concurrent writers; forcing max_concurrent_db_ops=1.
product   150 records, 150 distinct identities (product_code, org)
supplier  120 records, 120 distinct identities (supplier_code)
```

Every row has its own identity: 150 products and 120 suppliers, which is what
a correct key gives on this data.

## What goes wrong

A key that is unique in the data is not always a key of the thing. In this
data `product_code` together with `name` is unique too, because no two
organizations happen to give the same code the same name. Move `org` below
`name` in the `product` properties of `manifest.yaml` and run `infer.py` again:
the proposal becomes `['product_code', 'name']`. Among columns that rank
equally, inference takes them in the order the manifest lists them. Only you
know that a product belongs to an organization, so review every composite key
before you use it.

Inference also refuses to guess from little data: with fewer than 100 rows for
a vertex type (`min_sample_size` of `IdentityInferenceConfig`), the strategy
is `no_viable_identity` and the vertex is left unchanged.

## What to read next

- [Link by an alternative identifier](../16-secondary-identities/README.md):
  a source that names records by another key than the identity.
- [Finding a key for your data](../../docs/guides/identity_inference.md): the
  same steps for your own data, and every tuning option.
- [Vertex identity](../../docs/concepts/schema/vertex_identity.md): what an
  identity does when records are written.
