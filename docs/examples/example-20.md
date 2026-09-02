# Example 20: Version control for a world model — fork, conflict, resolve, merge

Two people start from the same schema. One decides the SSN identifies a person;
the other decides the email does. Both are right about their own half of the
business, and a registry that stores versions in a line has nowhere to put that:
the later write wins, silently, and the disagreement becomes a bug someone finds
months later in the data.

A **commit DAG** puts it somewhere. Both branches are recorded, both replay from
the base, and the history states plainly that it has two heads. Reconciling them
is a separate, explicit act that produces a third commit naming both parents —
and records *how* the disagreement was settled, so the same decision does not
have to be made twice.

No database. A commit history is a fact about the **contract**, not about any
deployment of it.

## Prerequisites

- Python 3.11+
- GraFlo package (run from the example directory with `uv run`)
- **No database.** Everything here is the contract plane.

## The shape

```
                  3605d3f2  key people by email    ← head
                 /
97382d5d ───────┤                                    b5680f7a  merge
 created_at      \                                  /
                  7bf6139a  key people by SSN  ─────┘  ← head
```

The shared commit matters: without a common ancestor there is nothing to merge
*against*, and combining two unrelated lineages is compose, not merge.

## Why each branch changes two things

Each branch re-keys the vertex **and** adds a property the other does not
contest. That is not padding. If the identity were the only difference, taking
one side would reproduce that side exactly, and `build_merge_commit` would
rightly refuse — a commit that moves nothing is a lie about history. With both,
the merge is a real merge: one side's decision wins the contested slot, and
*both* sides' uncontested work survives.

## Slots

Reconciliation happens per **slot** — the addressable location an op touches.
The two re-keys both land on `vertex/person/identity`, so they collide. The two
property additions land on different field slots, so they merge with no
question asked. That is the whole mechanism: conflicts are scoped to where the
disagreement actually is.

A conflict carries both sides' ops *and the ancestor's state*, because "what did
this look like before either change" is the question a resolver needs answered
and the one a two-way diff cannot express.

## Run it

No live graph database required.

```bash
cd examples/20-version-control
uv run python build_history.py              # → artifacts/commits/, two heads
uv run python build_history.py --show       # the log, without rewriting
uv run python merge_branches.py             # the conflict, resolved by taking left
uv run python merge_branches.py --take right
uv run python merge_branches.py --advance-left
```

The same history through the CLI:

```bash
uv run graflo log --store artifacts/commits
uv run graflo checkout --base manifest.yaml --store artifacts/commits <commit>
uv run graflo verify --base manifest.yaml --store artifacts/commits
```

## Tracked merges

`--advance-left` is the flag worth understanding. It moves the left branch on
(the vertex gains `nickname`) and then runs the *recorded* merge again:

```
replayed the recorded resolution:
  note: 1 recorded resolution(s) replayed: vertex/person/identity

merged (took left):
  identity  : ['ssn']
  properties: [..., 'ssn_verified', 'nickname', 'email_verified']
```

The identity decision is not asked again — it is replayed from the recipe, which
is content-addressed with its resolutions hashed in slot order. Only genuinely
new conflicts would surface. That is what keeps an overlay maintainable instead
of a fork someone re-litigates every release.

A recorded resolution whose slot *stops* conflicting is reported as unused
rather than force-applied: re-applying a stale decision to a slot nobody
contested is how a re-merge quietly reverts someone's work.

## What to look for

- **The fork is a recorded fact.** `build_history.py` prints two heads. Nothing
  was dropped and nothing was overwritten to get there.
- **Both sides' uncontested work survives.** The merged vertex carries
  `ssn_verified` *and* `email_verified`, whichever side won the identity.
- **The merge commit names both parents**, and materializes its ops against the
  *first* — which is what lets verified replay treat it like any other commit.
- **Taking the other side changes the hash**, not just the identity: content
  addressing is over the whole model.
- **The merged manifest is stamped** with `content_hash`, `canon`, both parents
  and the recipe hash, so it is self-describing outside any store.

## Files

| File | Purpose |
|------|---------|
| `manifest.yaml` | The base world model: a `person` keyed by `id`. |
| `build_history.py` | Records the shared commit and the two branches; prints the log. Importable and side-effect free. |
| `merge_branches.py` | Finds the ancestor, shows the conflict, resolves it, records the merge commit, and replays the recipe under `--advance-left`. |
| `artifacts/commits/` | The recorded history, one YAML per commit. |

## Related documentation

- [Version control](../concepts/schema/versioning.md) — content addressing, the commit DAG, slots, tracked merges
- [Manifest evolution](../concepts/schema/manifest_evolution.md) — the op vocabulary a commit records
- [Vertex identity](../concepts/schema/vertex_identity.md) — what re-keying a vertex actually changes
- [Example 19](example-19.md) — composing two *unrelated* manifests by declared equivalence, which is the operation to reach for when there is no common ancestor
- [Example 17](example-17.md) — identity funnels, for when the right answer is "whichever key this record actually carries" rather than one side winning
