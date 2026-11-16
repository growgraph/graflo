"""Linking partial records to entities, over the key contract.

A producer that holds only some properties of the entities it describes -- an
extractor, a form, a partial feed -- states them as records
(``PartialRecord``) of a ``RecordGraph``. This package links those records to
entities:

- ``records`` -- the record, graph and resolution models;
- ``resolve`` -- ``resolve_records`` identifies a batch by keys alone, stages
  what it cannot identify with the reason, and ``write_record_graphs`` writes
  what resolved;
- ``candidates`` -- ``gather_candidates`` lists the entities a staged record
  may be, graded against what it states, and ``support`` says whether a
  candidate's features identify it.
- ``statements`` -- ``resolve`` turns statements (affirmed or denied, done,
  planned or hedged, with their stated and said times) into the facts that hold
  at a time, with history, conflicts and withdrawals; ``slot_rules`` reads which
  slots are single-valued, exclusive or stateful from a schema;
- ``assertions`` -- ``statement_container`` writes statements as the provenance
  aspect's ``Assertion`` vertices, one triple each, ``read_statements`` reads
  them back, about one entity or all, and ``graph_time`` is the newest time a
  record was written;
- ``reconcile`` -- ``reconcile`` keeps a graph's stated relations to what their
  statements resolve to: it deletes what does not hold and writes what does.

Every keyed decision is ``graflo.hq.identify.identify``; what this package
adds is batch state: records found by other records of the batch, staging,
minting, ownership and the write.

It is not exported from any ``graflo`` façade; import its modules directly
(``graflo.link.records``, ``graflo.link.resolve``, ``graflo.link.candidates``,
``graflo.link.statements``, ``graflo.link.assertions``, ``graflo.link.reconcile``).
"""

__all__ = [
    "assertions",
    "candidates",
    "reconcile",
    "records",
    "resolve",
    "statements",
]
