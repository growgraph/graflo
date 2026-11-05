# How do I read a drawing of my manifest?

`graflo plot-manifest` draws a manifest as a set of figures: the vertex types and
the edges between them, each vertex type's fields, and what each resource reads
and produces. This guide draws them and explains every colour and shape. At the
end you have the figures, and you know what each one shows.

## What you need

- GraFlo with the `plot` extra (`pip install "graflo[plot]"`); see
  [Installation](../getting_started/installation.md).
- A manifest with a `schema` and an `ingestion_model`.

## Steps

### 1. Draw the figures

```bash
graflo plot-manifest -c manifest.yaml -o figures/ --output-format svg
```

`--output-format` is `svg`, `pdf` (the default), `png` or `dot`. Use `--only` to
draw only some figures; it is repeatable:

```bash
graflo plot-manifest -c manifest.yaml -o figures/ --only vc2vc --only vc2fields
```

### 2. Find each figure

File names start with the schema name; figures of the schema carry its version.

| Figure | File | What it shows |
|---|---|---|
| `vc2vc` | `<schema>_vc2vc-v<version>` | Vertex types and the edges between them, labelled with the relation |
| `vc2fields` | `<schema>_vc2fields-v<version>` | Each vertex type and its fields; the identity fields sit in a grey *definition* box |
| `source2vc` | `<schema>_source2vc-v<version>` | Every resource and the vertex types it touches |
| `source2vc_detailed` | `<schema>.resource2vc-<resource>-v<version>` | The same, one file per resource, each link labelled with the actors that make it |
| `resources` | `<schema>.resource-<resource>` | One resource's actor pipeline as a tree |

### 3. Read the colours

Every figure carries a dashed **legend** box with one sample per colour it uses.
A colour means the same thing in every figure:

| Colour | Shape | Meaning |
|---|---|---|
| green | ellipse | a vertex type; in a resource tree, a vertex actor |
| blue | box | a resource |
| red | trapezium | an identity field |
| violet | octagon | any other field |
| stone | ellipse | a transform |
| periwinkle | ellipse | an edge actor (resource trees only) |
| peach | ellipse | a vertex router actor (resource trees only) |
| white | ellipse | a descend actor (resource trees only) |

Every actor in a resource tree has a brown border. In `vc2vc` every vertex type is
green unless you color it by level (next step).

### 4. Color or group vertex types by level (optional)

```bash
graflo plot-manifest -c manifest.yaml -o figures/ --only vc2vc --color-vc-by-level
```

`--color-vc-by-level` fills each vertex type by its inferred **level**, and the
legend names each colour. A level counts how far a type sits from the types
nothing points to:

- vertex types that reach each other through edges (a cycle) share one level;
- **level 0** holds the types no edge from another level points to (the sources);
- every other type is one level past its deepest predecessor.

Level colours never reuse a colour from the table above, so a figure colored by
level cannot be mistaken for one colored by type. `--group-vc-by-level` instead
draws a labelled box around each level; the two flags can be combined.

## When a figure is too large to draw

Graphviz runs inside Python as WebAssembly, and its layout needs stack in
proportion to how deep the graph is. A very large schema, such as one with
thousands of fields in `vc2fields`, can need more than the WebAssembly stack
allows. The command then reports the figure:

```text
figure vc2fields not drawn: Graphviz ran out of stack laying out a graph of …
Error: not drawn: vc2fields. Leave a figure out with --only, …
```

It still draws the other figures and exits with status 1. To get the figure
anyway, write its DOT source and lay it out with a native Graphviz:

```bash
graflo plot-manifest -c manifest.yaml -o figures/ --only vc2fields --output-format dot
dot -Tsvg figures/<schema>_vc2fields-v<version>.dot -o vc2fields.svg
```

## What to read next

- [Creating a manifest](../getting_started/creating_manifest.md): the blocks these figures draw.
- [Vertex identity](../concepts/schema/vertex_identity.md): what makes a field an identity field.
