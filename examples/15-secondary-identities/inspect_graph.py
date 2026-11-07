"""Show the vertices and the issuedBy edges that ingest.py wrote.

Reads the file backend in ``artifacts/csv-backend``. Each edge endpoint is
printed by its identity (``sid``, ``iid``), although ``links.csv`` names it by
ISIN and LEI. Run it from this directory, after ``ingest.py``:

    uv run python inspect_graph.py
"""

from pathlib import Path

from graflo.architecture.backend import GraFloBackendReader

reader = GraFloBackendReader(Path("artifacts/csv-backend"))


def read_all(batches) -> list:
    """Collect every record of one type from its chunk files."""
    return [record for batch in batches for record in batch]


instruments = read_all(reader.iter_vertex_batches("instrument"))
issuers = read_all(reader.iter_vertex_batches("issuer"))
edges = read_all(reader.iter_edge_batches(("instrument", "issuer", "issuedBy")))

print(f"instruments: {len(instruments)}")
for doc in instruments:
    print(f"  {doc['sid']}  isin={doc['isin']}  {doc['name']}")
print(f"issuers: {len(issuers)}")
for doc in issuers:
    print(f"  {doc['iid']}  lei={doc['lei']}  {doc['name']}")
print(f"issuedBy edges: {len(edges)}")
# An edge record is [source endpoint, target endpoint, edge properties].
for source, target, properties in sorted(edges, key=lambda e: e[0]["sid"]):
    print(f"  {source['sid']} -> {target['iid']}  share={properties['share']}")
