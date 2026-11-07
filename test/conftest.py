import io
import logging
import os
from os.path import dirname, join, realpath
from pathlib import Path

import pandas as pd
import pytest
import yaml
from hypothesis import settings
from suthing import FileHandle, diff

from graflo.architecture.contract.bindings import Bindings, FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.util import cast_graph_name_to_triple
from graflo.util.misc import sorted_dicts
from test.hypothesis_budget import register_profiles

logger = logging.getLogger(__name__)

# Loaded at import, before pytest_configure, so `--hypothesis-profile` still wins.
register_profiles()
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))


def pytest_addoption(parser):
    parser.addoption(
        "--reset",
        action="store_true",
        help="Rewrite the reference files under test/ref instead of comparing to them",
    )
    parser.addoption(
        "--run-performance",
        action="store_true",
        default=False,
        help="Run performance / stress tests marked @pytest.mark.performance",
    )
    parser.addoption(
        "--run-nebula",
        action="store_true",
        default=False,
        help="Run NebulaGraph integration tests marked @pytest.mark.nebula",
    )
    parser.addoption(
        "--run-kafka",
        action="store_true",
        default=False,
        help="Run Kafka integration tests marked @pytest.mark.kafka",
    )
    parser.addoption(
        "--run-tigergraph",
        action="store_true",
        default=False,
        help="Run live TigerGraph tests marked @pytest.mark.tigergraph",
    )


def pytest_collection_modifyitems(config, items):
    _skip_map = {
        "performance": ("--run-performance", "need --run-performance option to run"),
        "nebula": ("--run-nebula", "need --run-nebula option to run"),
        "kafka": ("--run-kafka", "need --run-kafka option to run"),
        # TigerGraph schema DDL costs 15-40s per graph, which made it ~96% of the
        # suite's wall-clock while every other backend finished in seconds. It is
        # gated like nebula rather than trimmed: the cost is inherent to GSQL, not
        # to how the tests are written.
        "tigergraph": ("--run-tigergraph", "need --run-tigergraph option to run"),
    }
    for marker_name, (option, reason) in _skip_map.items():
        if config.getoption(option):
            continue
        skip = pytest.mark.skip(reason=reason)
        for item in items:
            # Not `marker_name in item.keywords`: keywords also hold parametrize
            # ids, so an unmarked test with a parameter named `nebula` was skipped.
            if item.get_closest_marker(marker_name) is not None:
                item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def reset(request):
    return request.config.getoption("reset")


@pytest.fixture(scope="function")
def current_path():
    return dirname(realpath(__file__))


def fetch_schema_dict(mode):
    schema_dict = FileHandle.load_resource("test.config.schema", f"{mode}.yaml")
    return schema_dict


def fetch_manifest_obj(mode, *, dynamic_edge_feedback: bool = False) -> GraphManifest:
    manifest = GraphManifest.from_config(fetch_schema_dict(mode))
    manifest.finish_init(dynamic_edge_feedback=dynamic_edge_feedback)
    return manifest


def fetch_schema_obj(mode, *, dynamic_edge_feedback: bool = False):
    return fetch_manifest_obj(
        mode, dynamic_edge_feedback=dynamic_edge_feedback
    ).require_schema()


@pytest.fixture(scope="function")
def schema():
    return fetch_schema_dict


@pytest.fixture(scope="function")
def schema_obj():
    return fetch_schema_obj


def ingest_atomic(conn_conf, current_path, test_db_name, schema_o, mode, n_cores=1):
    path = Path(current_path) / f"data/{mode}"

    # Not `conn_conf.database = ...`: Nebula's space and PostgreSQL's schema
    # live on `schema_name`, and assigning `database` there is silently
    # ignored — so the ingest would land in whatever namespace the config
    # already pointed at, and the caller's assertions would read a different
    # one. See test/db/backends.set_target_namespace.
    from test.db.backends import set_target_namespace

    set_target_namespace(conn_conf, test_db_name)

    # Create Bindings for file-based resources
    # Map each resource to a FileConnector that matches files in the data directory
    bindings = Bindings()
    ingestion_model = fetch_manifest_obj(
        mode, dynamic_edge_feedback=True
    ).require_ingestion_model()
    ingestion_model.finish_init(schema_o.core_schema, dynamic_edge_feedback=True)
    for resource in ingestion_model.resources:
        resource_name = resource.name
        # Create a FileConnector that matches files for this resource
        # Use resource name as part of the filename connector (e.g., "resource_name.csv", "resource_name.json", etc.)
        file_connector = FileConnector(
            regex=f".*{resource_name}.*",
            sub_path=path,
        )
        bindings.add_connector(file_connector)
        bindings.bind_resource(resource_name, file_connector)

    # Determine DB flavor from connection config
    from graflo.hq.caster import IngestionParams
    from graflo.hq.graph_engine import GraphEngine

    db_type = conn_conf.connection_type

    # Use GraphEngine for the full workflow
    engine = GraphEngine(target_db_flavor=db_type)
    manifest = GraphManifest(
        graph_schema=schema_o,
        ingestion_model=ingestion_model,
        bindings=bindings,
    )
    manifest.finish_init()

    # Define schema first (with recreate_schema=True)
    engine.define_schema(
        manifest=manifest,
        target_db_config=conn_conf,
        recreate_schema=True,
    )

    # Then ingest data
    ingestion_params = IngestionParams(n_cores=n_cores, clear_data=False)
    engine.ingest(
        manifest=manifest,
        target_db_config=conn_conf,
        ingestion_params=ingestion_params,
    )


def verify(sample, current_path, mode, test_type, kind="sizes", reset=False):
    ext = "yaml"
    if kind == "sizes":
        sample_transformed = {
            cast_graph_name_to_triple(k): v for k, v in sample.items()
        }

        sample_transformed = {
            "->".join([f"{x}" for x in k]) if isinstance(k, tuple) else k: v
            for k, v in sample_transformed.items()
        }
    elif kind == "indexes":
        _ = sample.pop("_fishbowl", None)
        sample_transformed = sorted_dicts(sample)
    elif kind == "contents":
        sample_transformed = sorted_dicts(sample)
    else:
        raise ValueError(f"value {kind} not accepted")

    if reset:
        FileHandle.dump(
            sample_transformed,
            join(current_path, f"./ref/{test_type}/{mode}_{kind}.{ext}"),
        )

    else:
        sample_ref = FileHandle.load_resource(
            f"test.ref.{test_type}", f"{mode}_{kind}.{ext}"
        )
        differences = diff(sample_ref, sample_transformed)
        assert not differences, (
            f"mode {mode}: {len(differences)} difference(s) from"
            f" ref/{test_type}/{mode}_{kind}.{ext}:\n"
            + "\n".join(str(d) for d in differences)
        )


@pytest.fixture()
def sample_ticker() -> list[dict]:
    df0_str = """Date,Open,High,Low,Close,Volume,Dividends,Stock Splits,ticker
2014-04-15,17.899999618530273,17.920000076293945,15.149999618530273,15.350000381469727,3531700,0,0,AAPL
2014-04-16,15.350000381469727,16.09000015258789,15.210000038146973,15.619999885559082,266500,0,0,AAPL"""
    return pd.read_csv(
        io.StringIO(df0_str),
    ).to_dict(orient="records")


@pytest.fixture()
def resource_openalex_works():
    return yaml.safe_load("""
    -   vertex: work
    -   transform:
            call:
                module: graflo.util.transform
                foo: split_keep_part
                params:
                    sep: "/"
                    keep: -1
                input:
                -   id
                output:
                -   _key
    -   transform:
            call:
                module: graflo.util.transform
                foo: split_keep_part
                params:
                    sep: "/"
                    keep: [-2, -1]
                input:
                -   doi
                output:
                -   doi
    -   key: referenced_works
        apply:
        -   vertex: work
        -   transform:
                call:
                    module: graflo.util.transform
                    foo: split_keep_part
                    params:
                        sep: "/"
                        keep: -1
                    input:
                    -   id
                    output:
                    -   _key
    -   source: work
        target: work
    """)


@pytest.fixture()
def resource_deb():
    return yaml.safe_load("""
    -   name: package
        apply:
        -   vertex: package
        -   key: dependencies
            apply:
            -   key: breaks
                apply:
                -   vertex: package
            -   key: conflicts
                apply:
                -   vertex: package
            -   key: depends
                apply:
                -   vertex: package
            -   key: pre-depends
                apply:
                -   vertex: package
            -   key: suggests
                apply:
                -   vertex: package
            -   key: recommends
                apply:
                -   vertex: package
        -   source: maintainer
            target: package
            exclude_target: dependencies
        -   source: package
            target: package
            relation_from_key: true
        -   key: maintainer
            apply:
            -   vertex: maintainer
    """)


@pytest.fixture()
def resource_deb_compact():
    return yaml.safe_load("""
    -   name: package
        apply:
        -   vertex: package
        -   key: dependencies
            apply:
            -   any_key: true
                apply:
                -   vertex: package
        -   source: maintainer
            target: package
            exclude_target: dependencies
        -   source: package
            target: package
            relation_from_key: true
        -   key: maintainer
            apply:
            -   vertex: maintainer
    """)


@pytest.fixture()
def resource_deb_package_only():
    """Package-package edges only via relation_from_key (example 4 style). No maintainer."""
    return yaml.safe_load("""
    -   name: package
        apply:
        -   vertex: package
        -   key: dependencies
            apply:
            -   any_key: true
                apply:
                -   vertex: package
        -   source: package
            target: package
            relation_from_key: true
    """)


@pytest.fixture()
def resource_ticker():
    return yaml.safe_load("""
    name: ticker_data
    apply:
    -   transform:
            call:
                module: graflo.util.transform
                foo: round_str
                params:
                    ndigits: 3
                input:
                -   Open
                dress:
                    key: name
                    value: value
    -   transform:
            call:
                module: graflo.util.transform
                foo: round_str
                params:
                    ndigits: 3
                input:
                -   Close
                dress:
                    key: name
                    value: value
    -   transform:
            call:
                module: builtins
                foo: int
                input:
                -   Volume
                dress:
                    key: name
                    value: value
    -   transform:
            call:
                module: graflo.util.transform
                foo: parse_date_yahoo
                input:
                -   Date
                output:
                -   t_obs
    -   transform:
            rename:
                ticker: oftic
    -   vertex: ticker
    -   vertex: feature
    -   source: ticker
        target: feature
        properties:
        -   t_obs
        vertex_weights:
        -   name: feature
            fields:
            -   name
    """)


@pytest.fixture()
def data_deb():
    return FileHandle.load_resource("test.data.deb", "package.json")


@pytest.fixture()
def data_deb_relation_from_key():
    """Package data for relation_from_key edge test (example 4 style)."""
    return FileHandle.load_resource("test.data.deb", "package_relation_from_key.json")
