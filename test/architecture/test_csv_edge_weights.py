"""Test CSV edge weights: 1 edge per CSV row.

Uses schema from test/config/schema/csv-edge-weights.yaml and
data from test/data/csv-edge-weights/relations.csv.
Verifies that each row produces exactly one edge.
"""

from pathlib import Path

import pandas as pd
import pytest
from suthing import FileHandle

from graflo.architecture.onto import GraphContainer
from graflo.architecture.schema import IngestionModel, Schema


def _load_csv_as_dicts(csv_path: Path) -> list[dict]:
    """Load CSV as list of dicts (one per row)."""
    df = pd.read_csv(csv_path)
    return df.to_dict(orient="records")


@pytest.fixture
def schema_csv_edge_weights():
    schema_dict = FileHandle.load("test.config.schema", "csv-edge-weights.yaml")
    schema = Schema.from_config(schema_dict)
    ingestion_model = IngestionModel.from_config(schema_dict)
    schema.bind_ingestion_model(ingestion_model)
    return schema


@pytest.fixture
def relations_data_csv_edge_weights(current_path):
    csv_path = Path(current_path) / "data" / "csv-edge-weights" / "relations.csv"
    return _load_csv_as_dicts(csv_path)


def test_csv_edge_weights_one_edge_per_row(
    schema_csv_edge_weights, relations_data_csv_edge_weights
):
    """Each row in relations.csv must produce exactly one edge."""
    ingestion_model = schema_csv_edge_weights.ingestion_model
    assert ingestion_model is not None
    resource = ingestion_model.fetch_resource("relations")
    docs = [resource(doc) for doc in relations_data_csv_edge_weights]
    graph = GraphContainer.from_docs_list(docs)

    # Count edges: (source, target, relation) -> list of (u, v, weight) tuples
    total_edges = sum(len(edocs) for edocs in graph.edges.values() if edocs)

    assert total_edges == len(relations_data_csv_edge_weights), (
        f"Expected {len(relations_data_csv_edge_weights)} edges (1 per row), got {total_edges}"
    )
