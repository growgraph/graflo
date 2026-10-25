# maintenance + sensors

The merge goes through.

## Machine: Asset ~ Device

- Declared by `vertex_equivalences[0]`.
- Keyed by a funnel: a record tries 2 branches in order and the first it completes is digested into `id`. Each member's own key becomes a lookup.
- `Asset.asset_id` is kept as `maintenance__asset_id`, a lookup in key space `maintenance`.
- `Device.device_id` is kept as `sensors__device_id`, a lookup in key space `sensors`.

![members](vertex-Machine.members.svg)

*Members. The types that become the merged type; a dashed edge marks a member a vocabulary joins rather than one the equivalence names.*

![attributes](vertex-Machine.attributes.svg)

*Attributes. Where each member attribute lands on the merged type: dark, a declared equivalence; blue, a vocabulary rename; thin grey, the name kept as it is; dotted, a mapping a later declaration overrode.*

![identity](vertex-Machine.identity.svg)

*Identity. The raw fields each branch reads, labelled with the transform or the tag; a record tries the branches in order, #1 first, and the first it completes keys it.*

![lookups](vertex-Machine.lookups.svg)

*Lookups. Each member's own key, kept on the merged type as a lookup in the key space the edge names.*

Findings:

1. **note** `lookup_demotion`: records of left:Asset key on the first of ['match_key', 'local_key'] they complete, so two with the same ['maintenance__asset_id'] that complete different ones become two 'Machine' vertices; ['maintenance__asset_id'] is now the lookup-only secondary 'maintenance' and no longer deduplicates them
2. **note** `lookup_demotion`: records of right:Device key on the first of ['match_key', 'local_key'] they complete, so two with the same ['sensors__device_id'] that complete different ones become two 'Machine' vertices; ['sensors__device_id'] is now the lookup-only secondary 'sensors' and no longer deduplicates them
