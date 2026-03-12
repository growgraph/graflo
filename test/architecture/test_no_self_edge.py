"""Test that top-level package 0xffff does not produce a spurious self-edge.

Uses schema from test/config/schema/debian-eco.yaml (same as examples/4-ingest-neo4j).
Verifies that package->package edges never have source==target when the package
does not depend on itself. Related to actor_util._iter_emitter_receiver_group_pairs.
"""

import pytest
from suthing import FileHandle

from graflo.architecture.onto import GraphContainer
from graflo.architecture.schema import IngestionModel, Schema


DOC_0XFFFF = {
    "name": "0xffff",
    "version": "0.9-1",
    "dependencies": {
        "depends": [
            {"name": "libc6", "version": ">= 2.14"},
            {"name": "libusb-0.1-4", "version": ">= 2:0.1.12"},
        ]
    },
    "description": "Open Free Fiasco Firmware Flasher",
    "maintainer": {
        "name": "Sebastian Reichel",
        "email": "sre@debian.org",
    },
}

BUGS_0AD = [
    {
        "originator": "Simon McVittie <smcv@debian.org>",
        "subject": "0ad: assertion failure if hyperthreading (SMT) is supported but disabled",
        "msgid": "<YkG+2KrwK0rqX4A0@momentum.pseudorandom.co.uk>",
        "package": "0ad",
        "severity": "normal",
        "owner": "",
        "summary": "",
        "location": "db-h",
        "source": "0ad",
        "pending": "pending",
        "forwarded": "",
        "found_versions": ["0ad/0.0.25b-1.1"],
        "fixed_versions": [],
        "date": "2022-03-28T14:00:01",
        "log_modified": "2022-05-27T08:18:03",
        "tags": [],
        "done": False,
        "done_by": None,
        "archived": False,
        "unarchived": False,
        "bug_num": 1008531,
        "mergedwith": [],
        "blockedby": [],
        "blocks": [],
        "affects": [],
    },
    {
        "originator": "Lennart Weller <lhw@ring0.de>",
        "subject": "0ad: Test 0ad with new version of nvidia-texture-tools",
        "msgid": "<20150804125612.30208.90988.reportbug@lhw.ring0.de>",
        "package": "0ad",
        "severity": "wishlist",
        "owner": "",
        "summary": "",
        "location": "db-h",
        "source": "0ad",
        "pending": "pending",
        "forwarded": "",
        "found_versions": [],
        "fixed_versions": [],
        "date": "2015-08-04T13:00:01",
        "log_modified": "2021-03-07T21:39:04",
        "tags": [],
        "done": False,
        "done_by": None,
        "archived": False,
        "unarchived": False,
        "bug_num": 794562,
        "mergedwith": [],
        "blockedby": [],
        "blocks": [],
        "affects": [],
    },
]


@pytest.fixture
def schema_debian_eco():
    schema_dict = FileHandle.load("test.config.schema", "debian-eco.yaml")
    schema = Schema.from_config(schema_dict)
    ingestion_model = IngestionModel.from_config(schema_dict)
    schema.bind_ingestion_model(ingestion_model)
    return schema


def test_0xffff_no_spurious_self_edge(schema_debian_eco):
    """Top-level package 0xffff must not produce a self-edge (0xffff -> 0xffff).

    0xffff depends on libc6 and libusb-0.1-4, not on itself. A spurious self-edge
    would indicate a bug in actor_util._iter_emitter_receiver_group_pairs when
    handling package->package edges with relation_from_key.
    """
    ingestion_model = schema_debian_eco.ingestion_model
    assert ingestion_model is not None
    resource = ingestion_model.fetch_resource("package")
    doc_result = resource(DOC_0XFFFF)
    graph = GraphContainer.from_docs_list([doc_result])

    # Collect all package->package edges (any relation: depends, pre-depends, etc.)
    package_edges = [
        (u, v)
        for (s, t, rel), edocs in graph.edges.items()
        if s == "package" and t == "package"
        for u, v, _ in edocs
    ]

    # 0xffff must not have a self-edge
    self_edges = [
        (u, v) for u, v in package_edges if u.get("name") == v.get("name") == "0xffff"
    ]
    assert len(self_edges) == 0, (
        f"Expected no self-edge for package 0xffff, got {len(self_edges)}: {self_edges}"
    )


def test_bugs_0ad_no_spurious_package_self_edge(schema_debian_eco):
    """Bugs for a single package (0ad) must not produce a package->package self-edge.

    The bug resource emits package and bug vertices. With infer_edges=True, the global
    edge_config includes package->package. When only one package (0ad) is in the vertex
    population, inferred package->package edges must not create a spurious 0ad->0ad
    self-edge.
    """
    ingestion_model = schema_debian_eco.ingestion_model
    assert ingestion_model is not None
    resource = ingestion_model.fetch_resource("bug")
    docs = [resource(bug) for bug in BUGS_0AD]
    graph = GraphContainer.from_docs_list(docs)

    # Collect all package->package edges (from inferred edges)
    package_edges = [
        (u, v)
        for (s, t, rel), edocs in graph.edges.items()
        if s == "package" and t == "package"
        for u, v, _ in edocs
    ]

    # 0ad must not have a self-edge
    self_edges = [
        (u, v) for u, v in package_edges if u.get("name") == v.get("name") == "0ad"
    ]
    assert len(self_edges) == 0, (
        f"Expected no self-edge for package 0ad from bugs resource, got {len(self_edges)}: {self_edges}"
    )
