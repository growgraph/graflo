"""Data ingestion command-line interface for graph databases.

This module provides a CLI tool for ingesting data into graph databases. It supports
batch processing, parallel execution, and various data formats. The tool can handle
both initial database setup and incremental data ingestion.

The sources are the ones the manifest's bindings declare (or the bindings file
given with ``--resource-connector-config-path``); a file connector's relative
``sub_path`` is resolved against the working directory.
``--data-source-config-path`` lists the sources to read instead.

Example:
    $ uv run ingest \\
        --db-config-path config/db.yaml \\
        --schema-path config/manifest.yaml \\
        --batch-size 5000 \\
        --n-cores 4
"""

import logging
import pathlib

import click
from suthing import FileHandle

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.contract.manifest import GraphManifest
from graflo.connections.onto import DBConfig
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--db-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=True,
)
@click.option(
    "--schema-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=True,
)
@click.option(
    "--resource-connector-config-path",
    "resource_connector_config_path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--resource-pattern-config-path",
    "resource_connector_config_path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default=None,
    hidden=True,
)
@click.option(
    "--data-source-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default=None,
    help=(
        "Sources to read instead of the ones the bindings declare: a file with a "
        "`data_sources` list of file or SQL sources, each naming its resource."
    ),
)
@click.option("--limit-files", type=int, default=None)
@click.option(
    "--batch-size", type=int, default=IngestionParams.model_fields["batch_size"].default
)
@click.option("--n-cores", type=int, default=1)
@click.option("--fresh-start", type=bool, help="wipe existing database")
@click.option(
    "--init-only",
    default=False,
    is_flag=True,
    help="skip ingestion; only init the db",
)
@click.option(
    "--no-create-namespace",
    default=False,
    is_flag=True,
    help="Do not create graph/database/space; require pre-provisioned namespace",
)
@click.option(
    "--on-doc-error",
    type=click.Choice(["skip", "fail"]),
    default="skip",
    show_default=True,
    help="Per-source-document cast errors: skip (continue batch) or fail the whole batch.",
)
@click.option(
    "--doc-error-sink",
    type=click.Path(path_type=pathlib.Path),
    default=None,
    help="Append gzip-compressed JSONL row failure records (.jsonl.gz) to this path (optional).",
)
def ingest(
    db_config_path,
    schema_path,
    limit_files,
    batch_size,
    n_cores,
    fresh_start,
    init_only,
    resource_connector_config_path,
    data_source_config_path,
    on_doc_error,
    doc_error_sink,
    no_create_namespace,
):
    """Ingest data into a graph database.

    This command processes data files and ingests them into a graph database according
    to the provided schema. It supports various configuration options for controlling
    the ingestion process.

    Args:
        db_config_path: Path to database configuration file
        schema_path: Path to the manifest
        limit_files: Optional limit on number of files to process
        batch_size: Number of source items to group per batch (default: IngestionParams.batch_size)
        n_cores: Degree of parallelism for casting (default: 1)
        fresh_start: Whether to wipe existing database before ingestion
        init_only: Whether to only initialize the database without ingestion
        resource_connector_config_path: Optional path to resource connector configuration
        data_source_config_path: Optional path to a list of sources to read
            instead of the bound ones

    Example:
        $ uv run ingest \\
            --db-config-path config/db.yaml \\
            --schema-path config/manifest.yaml \\
            --batch-size 5000 \\
            --n-cores 4 \\
            --fresh-start
    """
    logging.basicConfig(level=logging.INFO)

    manifest = GraphManifest.from_config(FileHandle.load(schema_path))
    manifest.finish_init()
    # Both blocks are needed; say so before the database is touched.
    manifest.require_schema()
    manifest.require_ingestion_model()

    # Load config from file
    config_data = FileHandle.load(db_config_path)
    conn_conf = DBConfig.from_dict(config_data)
    if not conn_conf.can_be_target():
        raise click.UsageError(
            f"--db-config-path names a {conn_conf.connection_type.value} connection, "
            "which cannot be a target."
        )

    if resource_connector_config_path is not None:
        bindings = Bindings.from_dict(FileHandle.load(resource_connector_config_path))
    elif manifest.bindings is not None:
        bindings = manifest.bindings
    else:
        bindings = Bindings()

    engine = GraphEngine(target_db_flavor=conn_conf.connection_type)

    # Create ingestion params with CLI arguments
    ingestion_params = IngestionParams(
        n_cores=n_cores,
        batch_size=batch_size,
        init_only=init_only,
        limit_files=limit_files,
        on_doc_error=on_doc_error,
        doc_error_sink_path=doc_error_sink,
    )

    # Define schema first (if recreate_schema is requested)
    if fresh_start or init_only:
        engine.define_schema(
            manifest=manifest,
            target_db_config=conn_conf,
            recreate_schema=bool(fresh_start),
            create_namespace=not no_create_namespace,
        )
        if init_only:
            return

    registry: DataSourceRegistry | None = None
    if data_source_config_path is not None:
        data_source_config = FileHandle.load(data_source_config_path)
        registry = DataSourceRegistry()

        # Register data sources from config
        # Config format: {"data_sources": [{"source_type": "...", "resource_name": "...", ...}]}
        if "data_sources" in data_source_config:
            for ds_config in data_source_config["data_sources"]:
                ds_config_copy = ds_config.copy()
                resource_name = ds_config_copy.pop("resource_name")
                source_type = ds_config_copy.pop("source_type", None)
                if source_type is not None and str(source_type).lower() == "api":
                    raise click.UsageError(
                        "API data sources must be declared in manifest bindings "
                        "(APIConnector) and ingested via GraphEngine with a "
                        "ConnectionProvider; --data-source-config-path does not "
                        "support source_type: api."
                    )

                data_source = DataSourceFactory.create_data_source(
                    source_type=source_type, **ds_config_copy
                )
                registry.register(data_source, resource_name=resource_name)

    ingest_manifest = manifest.model_copy(update={"bindings": bindings})
    ingest_manifest.finish_init()
    engine.ingest(
        manifest=ingest_manifest,
        target_db_config=conn_conf,
        ingestion_params=ingestion_params,
        data_source_registry=registry,
    )


if __name__ == "__main__":
    ingest()
