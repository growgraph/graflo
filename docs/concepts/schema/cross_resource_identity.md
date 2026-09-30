# Cross-resource identity discovery

A plant's maintenance system and its sensor feed describe the same machines.
One export calls the key column `serial_number`, the other `serial_no`, and
nobody wrote down that they are the same. This page explains what GraFlo looks
at when it proposes one identity for a vertex type that several
[resources](../glossary.md#resource) describe, what it can propose, and where
it stops. Read it before you accept such a proposal into a manifest; the steps
for your own data are in
[Finding a key for your data](../../guides/identity_inference.md).

The inference reads [samples](../glossary.md#sample) only. It needs no
language model and no database connection.

## Proposal, not decision

`CrossResourceIdentityInferencer` returns a `CrossResourceIdentityProposal`
and changes nothing. Applying a proposal to a vertex is a separate call,
`apply_proposal_to_vertex`, which you make after reviewing the evidence. It
refuses a proposal whose strategy is `no_viable_identity`.

```python
from suthing import FileHandle

from graflo import GraphEngine, GraphManifest
from graflo.db.cross_resource_identity import (
    apply_proposal_to_vertex,
    infer_from_source_sample,
)

manifest = GraphManifest.from_config(FileHandle.load("manifest.yaml"))
manifest.finish_init()
machine = manifest.require_schema().core_schema.vertex_config["machine"]

source = GraphEngine().sample_resources(
    ["exports/maintenance_assets.csv", "exports/sensor_devices.csv"],
    max_docs=500,
)
proposal = infer_from_source_sample(source, vertex_name="machine")
print(proposal.strategy, proposal.identity, proposal.warning)

if proposal.strategy != "no_viable_identity":
    machine = apply_proposal_to_vertex(machine, proposal)
```

For two such exports the proposal is `natural` with identity `[serial_no]`,
the alphabetically first of the two column names. Its `suggested_transforms`
rename `serial_number` to `serial_no` in the maintenance resource.
`apply_proposal_to_vertex` returns a new vertex with the proposed identity; the
manifest is unchanged until you put the vertex back into it.

Fuzzy signals decide which columns to compare; only exact equality decides
whether a field set is a key. Column-name similarity and value overlap pair up
columns across resources. Whether the paired field set identifies the records
is then tested on the values as they are: within each resource, no two records
may have the same values in those fields. Nothing fuzzy reaches the write path,
because a soft match
there would merge distinct machines without an error, and that is hard to undo.

## How a proposal is reached

1. **Eligibility.** In each resource, columns that cannot be key material are
   set aside: columns holding lists, bytes or nested objects, strings longer
   than 256 characters, and columns missing or null in more than half the
   records. Single-resource inference uses the same test.
2. **Declared foreign keys.** When a sample carries `foreign_keys` (sampling a
   PostgreSQL schema fills them from the real constraints), each column pair a
   foreign key names is kept whatever its scores, with a score of 1.
3. **Alignment.** Every other pair of eligible columns across two resources is
   scored twice. The name score is the better of word overlap and character
   similarity between the two column names. The value score is the Jaccard
   overlap of the two value sets, after trimming and lowercasing, and after
   keeping only digits when either name suggests a phone number. A pair is kept
   when its value score reaches `min_value_jaccard` and the mean of the two
   scores reaches `min_pair_score`. Two columns that share no values are never
   paired, however alike their names; two columns with identical values are
   paired even when their names differ entirely.
4. **Canonical names.** Pairs are closed into groups: `a <-> b` and `b <-> c`
   describe one column under three spellings. Each group takes the
   alphabetically first of its column names, so the result does not depend on
   the order of the resources. A group that holds two columns of the same
   resource would lose one of them when both are renamed to one name; it is
   left out and listed under `evidence["ambiguous_alignments"]`.
5. **Key search.** Among the columns every resource shares after renaming,
   GraFlo adds columns one at a time, most promising first, until the tuple is
   unique within every resource, then drops any column the tuple does not
   need. A tuple is tested as a whole: two columns may identify the records
   together while neither does alone. The key may have at most `max_key_width`
   columns. Columns are ranked as in single-resource inference: a name ending
   in `id`, `uuid`, `key`, `code` or `pk` ranks higher, and so do integer and
   UUID values.
6. **Fallbacks.** With no shared key, GraFlo searches each resource for its own
   key. If at least two different keys are found, each becomes one branch of an
   [identity funnel](../glossary.md#identity-funnel), named after its resource,
   with branches in alphabetical order of resource name. Otherwise the proposal
   is a flat hash over the first `max_key_width` shared columns in alphabetical
   order, with a warning. If no column pair aligns, or the resources share no
   column after renaming, the strategy is `no_viable_identity`.

Uniqueness is checked within each resource, never over the pooled records. Two
resources that describe the same machines hold the same serial numbers, so in
the pooled records every serial number would appear twice and the key would be
rejected. The proposal reports `uniqueness_by_resource` for each side, and
`shared_key_values`: how many key values, after trimming and lowercasing,
appear in every resource. The second number is the evidence that the resources
describe the same machines.

## What a proposal contains

| Field | Meaning |
|---|---|
| `strategy` | One of the strategies below |
| `identity`, `hash_identity_properties`, `identity_funnel` | The proposed identity, in the fields a vertex declares |
| `confidence` | The best alignment score, in `[0, 1]`; multiplied by 0.8 for a funnel and by 0.5 for a hash |
| `alignments` | Each column pair with its `name_score`, `value_jaccard` and whether it was `declared` |
| `resource_field_maps` | Per resource, `{source column: canonical name}` |
| `suggested_transforms` | For each resource whose columns need renaming, its name and a `transform` step with a `rename` map. Put the step at the start of that resource's `pipeline` |
| `warning` | Why a funnel or a hash was proposed instead of a key, or why nothing was |
| `evidence` | The numbers behind the choice: `resources`, `doc_counts`, `shared_fields`, and, depending on the strategy, `uniqueness_by_resource`, `shared_key_values`, `key_width`, `per_resource_keys`, `ambiguous_alignments` |

| `strategy` | Meaning |
|---|---|
| `natural` | One shared column identifies the records of every resource |
| `composite` | A tuple of shared columns identifies the records of every resource |
| `funnel` | No shared key; each resource identifies its records through its own branch. Records that only one resource describes do not merge with the others |
| `hash_fallback` | No key was proven; a hash over shared columns, which may give two machines the same `id` |
| `no_viable_identity` | Fewer than two resources with records, a resource below `min_sample_size`, or nothing aligned |

Single-resource inference calls a one-column key `unary`; this module calls it
`natural`, the name of the identity mode it produces.

## Configuration

`CrossResourceIdentityConfig`, passed as `config=` to
`infer_from_source_sample` or to `CrossResourceIdentityInferencer`:

| Option | Default | Meaning |
|---|---|---|
| `min_sample_size` | 100 | Records each resource must have. Below it, uniqueness is not evidence of a key |
| `max_sample_size` | none | Random subsample cap for large samples |
| `max_key_width` | 3 | A key needing more columns falls back to a funnel or a hash |
| `min_value_jaccard` | 0.1 | Floor on value overlap; a pair below it is never kept |
| `min_pair_score` | 0.5 | Floor on the mean of the name and value scores |
| `max_alignments` | 20 | How many of the best-scoring pairs are kept |
| `type_cost_weight`, `semantic_weight` | 0.2, 0.5 | How much value type and column name count when columns are ranked |
| `n_boots`, `subsample_ratio` | 5, 0.8 | Number and size of the resamples on which a key must stay unique |

## Input

The inferencer takes a `dict[str, list[dict]]`: flat records per resource.
`SourceSample.samples_by_resource`, which `GraphEngine.sample_resources`
produces (see [Sampling and profiling](sampling_and_profiling.md)), has that
shape. `infer_from_source_sample` passes every resource of the sample, plus the
foreign keys the sample declares. Sample only the resources that describe the
vertex type you are asking about; any other resource takes part in the
alignment too. Resource names must be unique within a sample, and
`SourceSample` rejects a duplicate.

Nested documents must be flattened first with `ResourceProfile.flat_docs`,
which turns paths into column names such as `contact.email`. `flat_docs` keeps
the first value per path and does not fan out lists, so one document becomes
one record. That identifies the document itself; it cannot identify the items
of a list inside it.

## Limits

- The inferencer proves a key within the sample only. A key that is unique over
  a few hundred records may not be unique over the full source.
  `min_sample_size` is the guard, and a primary key the source declares is
  better evidence than any inferred key.
- Trimming and lowercasing are used to compare columns, never applied to your
  data. If one export writes `SN-001` and the other ` sn-001 `, the columns
  align and `shared_key_values` counts the pair, but when the records are cast
  the two values differ and produce two vertices. Normalize the key in each
  resource with a transform step before you rely on the proposal.
- Values encoded differently (a zero-padded serial number and a plain one) do
  not overlap, so their columns do not align and the key is missed.
- A `funnel` proposal merges only records that complete the same branch. It
  cannot tell that a serial number in one source and an asset tag in another
  belong to the same machine. Its branches are ordered by resource name; check
  that the order suits you, because the first complete branch wins.

[The cross-resource identity example (18)](../../examples/cross-resource-identity/index.md)
runs two exports whose key columns have different names and prints the
alignments and evidence.

## What to read next

- [Finding a key for your data](../../guides/identity_inference.md): the steps
  for your own sources.
- [Vertex identity](vertex_identity.md): what a proposed funnel or hash does
  when records are cast.
- [Sampling and profiling](sampling_and_profiling.md): where the samples come
  from and what caps apply.
