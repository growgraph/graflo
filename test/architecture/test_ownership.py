"""Which vertex types a manifest's connectors own."""

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.ownership import connector_owned_types

SCHEMA = {
    "metadata": {"name": "support", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": name,
                    "properties": ["key", "email"],
                    "identity": ["key"],
                    "secondary_identities": [{"name": "by_email", "fields": ["email"]}],
                }
                for name in ("Ticket", "Comment", "Customer", "Agent", "Note")
            ]
        },
        "edge_config": {"edges": []},
    },
}


def _manifest(resources, bound) -> GraphManifest:
    return GraphManifest.from_config(
        {
            "schema": SCHEMA,
            "ingestion_model": {"resources": resources},
            "bindings": {
                "connectors": [{"name": "sync", "kind": "push"}],
                "resource_connector": [
                    {"resource": r, "connector": "sync"} for r in bound
                ],
            },
        }
    )


def test_types_a_bound_resource_writes_are_owned():
    manifest = _manifest(
        [
            {
                "name": "jira",
                "pipeline": [
                    {"vertex": "Ticket"},
                    {
                        "descend": {
                            "key": "comments",
                            "pipeline": [{"vertex": "Comment"}],
                        }
                    },
                    {"vertex": "Customer", "lookup_only": True},
                    {"vertex": "Agent", "find": "by_email"},
                ],
            },
            {"name": "notes", "pipeline": [{"vertex": "Note"}]},
        ],
        bound=["jira"],
    )
    assert connector_owned_types(manifest) == {"Ticket", "Comment"}


def test_a_manifest_without_bindings_owns_nothing():
    manifest = GraphManifest.from_config({"schema": SCHEMA})
    assert connector_owned_types(manifest) == frozenset()
