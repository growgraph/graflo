# 23 — Edge inverses: audit, repair, realize

Two relation names often read one fact from its two ends: a person is
`employed_by` an institution, and the institution `employs` the person. Declaring
that pair is cheap and useful. *Storing* both readings is a separate decision —
and once you do store both, every source that writes one of them has to feed the
other, or the two relations quietly disagree.

`manifest.yaml` is a manifest assembled from two sources. `hr` reports both
readings, with a step for each. `registry` reports only `employed_by`. The schema
declares both edges, and nothing says that `registry` never writes `employs`.

```bash
cd examples/23-edge-inverses

# 1. how is each declared pair realized, and what is wrong?
uv run graflo inverses audit manifest.yaml

# 2. propagate what is merely missing; keep the ops to read
uv run graflo inverses repair manifest.yaml \
    --emit-ops artifacts/repair.ops.yaml -o artifacts/manifest_repaired.yaml

# 3. the result is clean
uv run graflo inverses audit artifacts/manifest_repaired.yaml

# 4. see what a `registry` row writes, before and after -- no database needed
uv run python mirror.py
```

## Declaring is not storing

Both pairs are declared in `edge_config.inverses`. That alone stores nothing and
is usually enough: the inverse name reads the forward edge from its target, so
`graph_neighbors(edge_types=["has_alumnus"])` works although no `has_alumnus`
edge exists. `alumnus_of` / `has_alumnus` is left exactly like that.

Storing the inverse is an optional extra step, one way per pair:

| Realization | What is stored | In this example |
|-------------|----------------|-----------------|
| **native** | The database maintains the reverse type (TigerGraph `WITH REVERSE_EDGE`). | — |
| **materialized** | The inverse is a declared edge, fed at ingestion. | `employed_by` / `employs`: both edges are declared. |

On most backends a reverse read costs nothing, so a stored inverse only
duplicates edges. Ask what your target needs:

```bash
uv run graflo inverses realize manifest.yaml --dry-run
```

On `neo4j` that plans nothing, and says why for each pair. Change
`db_flavor` to `tigergraph` and the same command plans a native inverse for
`alumnus_of` — there, reverse reachability is fixed when the edge type is created.

## What the audit finds

```text
alumnus_of <-> has_alumnus: declared
employed_by <-> employs: materialized (2/2 mirrored)
    fed in hr: step
    fed in registry: none

repairable (3):
  - [property_drift] inverse edges (...employed_by) and (...employs) declare different properties: [] vs ['since']
  - [undirected_not_symmetric] every edge of relation 'collaborates_with' is undirected but the relation is not declared symmetric
  - [unfed_inverse] resource 'registry' writes 'employed_by' but feeds nothing into its materialized inverse 'employs'; the two relations will disagree
      at registry:2
```

All three are **repairable**: one place states a fact and another omits it, so
propagating it cannot change what the manifest means. The other kind of finding
is a **conflict** — two places that disagree, such as `(person, institution,
employed_by)` next to `(person, institution, employs)`, where one of them was
modeled backwards. Only the author knows which, so `repair` lists conflicts and
never touches them, and `audit` exits 1 while any remain.

## What the repair does

```yaml
# artifacts/repair.ops.yaml
- op: declare_edge_inverses      # collaborates_with is undirected everywhere:
  symmetric: [collaborates_with] #   say so, once, at the relation level
- op: add_edge_properties        # `employs` states `since`; its mirror now does too
  additions: {employed_by: [since]}
- op: set_inverse_emission       # registry's employed_by step also writes employs
  steps: {registry: [{step: 2}]}
```

These are ordinary evolution ops. The file replays with `apply_evolution` and
no knowledge of this command, `invert_ops` undoes it exactly, and each repair
was kept only because — applied to a copy — its finding disappeared and no new
one appeared.

The third op sets `emit_inverse: true` on one step. Nothing is generated: the
inverse is mirrored at assembly, after the step's relation is resolved, so it
works the same whether the relation is fixed, read from a field, mapped, or
taken from a document key. `hr` is left alone — a source that reports both
readings feeds the inverse with a step of its own, which is just as good.

`mirror.py` shows the difference on one row:

```text
as assembled:
    (person)-[alumnus_of]->(institution)
    (person)-[employed_by]->(institution)
after repair:
    (institution)-[employs]->(person)
    (person)-[alumnus_of]->(institution)
    (person)-[employed_by]->(institution)
```

`alumnus_of` is still written once: its pair is only declared, so there is
nothing to mirror into.

## From Python

```python
from graflo.architecture.evolution import apply_evolution, plan_repair_inverses
from graflo.architecture.profile import audit_inverses

report = audit_inverses(manifest)  # pairs, feeding, findings
plan = plan_repair_inverses(manifest)  # .ops, .skipped, .remaining
manifest = apply_evolution(manifest, plan.ops)
```

`plan_realize_inverses`, `plan_switch_realization`, `plan_withdraw_realization`
and `plan_declare_symmetric` have the same shape. The same audit runs as a conformance profile:
`graflo check --profile inverses`.
