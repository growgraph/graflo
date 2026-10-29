# Comparing and deriving field values

A text rarely states a value exactly as the graph stores it: "Berlin" for "Berlin, pump hall 2", "Brookmere" for "Brookmere Waterworks", "PUMP-x1" for "Pump X1". A field's `match` block says how its values compare, and a `derived` field stores a form of another field's value, computed by GraFlo, so that lookups can find it. Both describe a value's structure; what the field means stays in your schema.

## How do I say how a field compares?

Declare `match` on the field:

```yaml
properties:
-   name: site
    match:
        normalizer: label_v1
        parts: {split: [",", "/"], order: general_first}
-   name: status
    match: {normalizer: label_v1, exclusive: true}
```

| Key | Says |
|---|---|
| `normalizer` | A registered normalizer, applied to the whole value and to each part. `label_v1` folds case, Unicode width, punctuation and spacing: `"PUMP-x1!"` and `"Pump X1"` are both `"pump x1"`. Without one, values compare after type coercion alone. |
| `parts.split` | `words`, or the separators between parts. Required with `parts`. |
| `parts.order` | Which end of the value is the general one: `general_first`, `specific_first` or `unordered`. Required with `parts`. |
| `exclusive` | Whether two different values are evidence against a match. Unset: true on a key or alternate-key field, false otherwise. |

Some shapes, as examples:

| Value | `match` |
|---|---|
| a serial number used as a key | nothing |
| a status | `{normalizer: label_v1, exclusive: true}` |
| an address, a category path, a version | `{normalizer: label_v1, parts: {split: [...], order: general_first}}` |
| a host name | `{normalizer: label_v1, parts: {split: ["."], order: specific_first}}` |
| a name whose short form is its first words | `{normalizer: label_v1, parts: {split: words, order: general_first}, exclusive: false}` |
| free text | `{normalizer: label_v1, parts: {split: words, order: unordered}}` |

## What does a comparison return?

`graflo.architecture.schema.compare.compare_value(field, stated, stored, keyed=...)` returns a level:

| Level | When |
|---|---|
| `exact` | Equal after the normalizer |
| `compatible` | One is a less specific form of the other: a leading run of parts (`general_first`), a trailing run (`specific_first`) or a subset of parts (`unordered`) |
| `differs` | Neither, on a field that is not `exclusive`; not evidence against |
| `conflict` | Neither, on an `exclusive` field |
| `unknown` | A value is missing on either side |

A list is compared member by member, and its best member's level wins. With `site` above, "Berlin" against "Berlin, pump hall 2" is `compatible`, and "Lyon" is `differs`.

## How do I store a normal form or the less specific forms of a value?

Declare a `derived` field that names its source:

```yaml
properties:
-   name: label
    match: {normalizer: label_v1}
-   name: normalized_label
    derived: {source: label, form: normal}
-   name: site
    match: {normalizer: label_v1, parts: {split: [","], order: general_first}}
-   name: site_keys
    type: LIST
    item_type: STRING
    derived: {source: site, form: keys}
```

- `form: normal` stores the source's normal form: "Berlin  Pump!" gives `berlin pump`.
- `form: keys` stores every run of the source's parts that keeps the general end: "Berlin, pump hall 2" gives `berlin` and `berlin pump hall 2`. It needs a `LIST` of `STRING` and ordered `parts` on its source.
- The normalizer and the split are read from the source's `match`, so the source must declare a `normalizer`. A derived field declares no `match`, is never `reserved` and never merges by `union`.

GraFlo sets derived fields before every vertex write and every digest, and overwrites a value a document states. A type sheet, element text and instance schema never show them: a model is never asked for a field GraFlo computes.

## What if a derived field is part of an identity?

A `form: normal` field may be a key or a `hash_identity_properties` field. One alias per normal form, whatever its spelling:

```yaml
-   name: Alias
    properties:
    -   {name: label, match: {normalizer: label_v1}}
    -   {name: normalized_label, derived: {source: label, form: normal}}
    identity: [id]
    hash_identity_properties: [normalized_label]
```

A record that states only `label` is identified, and its type sheet reads `hash(label)`. The digest names the normalizer, so a new normalizer version gives new ids, and changing one is a re-identification. A keys field is a list, and is never part of a key.

## Rules and limits

- A normalizer's name never changes meaning; a new behaviour is a new name.
- `label_v1` separates words by whitespace, so a script written without spaces gives one word per run of letters.
- Only vertex fields declare `match` or `derived`; an edge field that does is refused.
- An `identity_funnel` branch cannot read a derived field.
- Changing a field's `parts` changes no id. Stored keys are recomputed on each element's next write.

## What to read next

- [Finding the vertices a text names](../operations/finding_vertices_from_text.md): the lookup lanes that read derived fields.
- [Vertex identity](vertex_identity.md): keys, hashes and alternate identities.
