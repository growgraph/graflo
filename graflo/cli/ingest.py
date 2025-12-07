"""Data ingestion command-line interface for graph databases.

This module provides a CLI tool for ingesting data into graph databases. It supports
batch processing, parallel execution, and various data formats. The tool can handle
both initial database setup and incremental data ingestion.

Key Features:
    - Configurable batch processing
    - Multi-core and multi-threaded execution
    - Support for custom resource patterns
    - Database initialization and cleanup options
    - Flexible file discovery and processing

Example:
    $ uv run ingest \\
        --db-config-path config/db.yaml \\
        --schema-path config/schema.yaml \\
        --source-path data/ \\
        --batch-size 5000 \\
        --n-cores 4
"""

import logging.config
import pathlib
from os.path import dirname, join, realpath

import click
from suthing import FileHandle

from graflo import Caster, DataSourceRegistry, Patterns, Schema
from graflo.db.connection.onto import DBConfig
from graflo.data_source import DataSourceFactory

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
    "--source-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=False,
    help="Path to source data directory (required if not using --data-source-config-path)",
)
@click.option(
    "--resource-pattern-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--data-source-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default=None,
    help="Path to data source configuration file (supports API, SQL, file sources)",
)
@click.option("--limit-files", type=int, default=None)
@click.option("--batch-size", type=int, default=5000)
@click.option("--n-cores", type=int, default=1)
@click.option(
    "--n-threads",
    type=int,
    default=1,
)
@click.option("--fresh-start", type=bool, help="wipe existing database")
@click.option(
    "--init-only",
    default=False,
    is_flag=True,
    help="skip ingestion; only init the db",
)
def ingest(
    db_config_path,
    schema_path,
    source_path,
    limit_files,
    batch_size,
    n_cores,
    n_threads,
    fresh_start,
    init_only,
    resource_pattern_config_path,
    data_source_config_path,
):
    """Ingest data into a graph database.

    This command processes data files and ingests them into a graph database according
    to the provided schema. It supports various configuration options for controlling
    the ingestion process.

    Args:
        db_config_path: Path to database configuration file
        schema_path: Path to schema configuration file
        source_path: Path to source data directory
        limit_files: Optional limit on number of files to process
        batch_size: Number of items to process in each batch (default: 5000)
        n_cores: Number of CPU cores to use for parallel processing (default: 1)
        n_threads: Number of threads per core for parallel processing (default: 1)
        fresh_start: Whether to wipe existing database before ingestion
        init_only: Whether to only initialize the database without ingestion
        resource_pattern_config_path: Optional path to resource pattern configuration

    Example:
        $ uv run ingest \\
            --db-config-path config/db.yaml \\
            --schema-path config/schema.yaml \\
            --source-path data/ \\
            --batch-size 5000 \\
            --n-cores 4 \\
            --fresh-start
    """
    cdir = dirname(realpath(__file__))

    logging.config.fileConfig(
        join(cdir, "../logging.conf"), disable_existing_loggers=False
    )

    logging.basicConfig(level=logging.INFO)

    schema = Schema.from_dict(FileHandle.load(schema_path))

    # Load config from file
    config_data = FileHandle.load(db_config_path)
    conn_conf = DBConfig.from_dict(config_data)

    if resource_pattern_config_path is not None:
        patterns = Patterns.from_dict(FileHandle.load(resource_pattern_config_path))
    else:
        patterns = Patterns()

    schema.fetch_resource()

    caster = Caster(
        schema,
        n_cores=n_cores,
        n_threads=n_threads,
    )

    # Validate that either source_path or data_source_config_path is provided
    if data_source_config_path is None and source_path is None:
        raise click.UsageError(
            "Either --source-path or --data-source-config-path must be provided"
        )

    # Check if data source config is provided (for API, SQL, etc.)
    if data_source_config_path is not None:
        # Load data source configuration
        data_source_config = FileHandle.load(data_source_config_path)
        registry = DataSourceRegistry()

        # Register data sources from config
        # Config format: {"data_sources": [{"source_type": "...", "resource_name": "...", ...}]}
        if "data_sources" in data_source_config:
            for ds_config in data_source_config["data_sources"]:
                ds_config_copy = ds_config.copy()
                resource_name = ds_config_copy.pop("resource_name")
                source_type = ds_config_copy.pop("source_type", None)

                # Create data source using factory
                data_source = DataSourceFactory.create_data_source(
                    source_type=source_type, **ds_config_copy
                )
                registry.register(data_source, resource_name=resource_name)

        # Use data source registry for ingestion
        caster.ingest_data_sources(
            data_source_registry=registry,
            conn_conf=conn_conf,
            clean_start=fresh_start,
            batch_size=batch_size,
            init_only=init_only,
        )
    else:
        # Fall back to file-based ingestion
        caster.ingest(
            output_config=conn_conf,
            patterns=patterns,
            limit_files=limit_files,
            clean_start=fresh_start,
            batch_size=batch_size,
            init_only=init_only,
        )


if __name__ == "__main__":
    ingest()
