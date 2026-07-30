# Runtime (moved)

`graflo.architecture.contract.runtime` was removed in the contract ↔ pipeline
inversion. Schema-bound execution now lives under
[`graflo.architecture.pipeline.runtime`](../pipeline/runtime.md).

- [`ResourceRuntime`](runtime/resource.md) → `graflo.architecture.pipeline.runtime.resource`
- [`EdgeDerivation`](runtime/edge_derivation.md) → `graflo.architecture.graph_types.edge_derivation`
