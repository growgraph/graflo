"""Linking partial records to entities: provisional scaffolding over the key contract.

A producer that holds only some properties of the entities it describes -- an
extractor, a form, a partial feed -- states them as records
(``PartialRecord``) of a ``RecordGraph``. This package links those records to
entities:

- ``records`` -- the record, graph and resolution models;
- ``resolve`` -- ``resolve_records`` identifies a batch by keys alone, stages
  what it cannot identify with the reason, and ``write_record_graphs`` writes
  what resolved;
- ``candidates`` -- ``gather_candidates`` lists the entities a staged record
  may be, for something else to choose among.

Every keyed decision is ``graflo.hq.identify.identify``; what this package
adds is batch state: records found by other records of the batch, staging,
minting, ownership and the write.

This package is scaffolding: its API may change in any release. It is not
exported from any ``graflo`` façade; import its modules directly
(``graflo.link.records``, ``graflo.link.resolve``, ``graflo.link.candidates``).
"""
