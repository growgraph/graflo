# Apache Ossie JSON Schemas (vendored)

Copied unchanged from [apache/ossie](https://github.com/apache/ossie) at
revision `4a315fa3126bb16c770461fa18e88715acfb4968`, spec version `0.3.0.dev0`:

| File | Upstream path |
|---|---|
| `ossie-schema.json` | `core-spec/ossie-schema.json` (semantic model) |
| `ontology.json` | `ontology/ontology.json` |
| `mapping.json` | `ontology/mapping.json` |

Licensed under the Apache License 2.0 (`LICENSE`). `test/interop/test_ossie.py`
validates every exported document against them. When `OSSIE_VERSION` in
`graflo/interop/ossie.py` moves, copy all three again from one upstream
revision and update the revision above.
