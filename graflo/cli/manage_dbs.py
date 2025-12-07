"""Database management utilities for ArangoDB.

This module provides command-line tools for managing ArangoDB databases, including
backup and restore operations. It supports both local and Docker-based operations.

Key Features:
    - Database backup and restore
    - Docker and local execution modes
    - Configurable connection settings
    - Batch processing of multiple databases

Example:
    $ uv run manage_dbs \\
        --db-config-path config/db.yaml \\
        --db mydb1 mydb2 \\
        --store-directory-path /backups \\
        --use-docker
"""

import logging
import pathlib
import subprocess
import sys
from datetime import date

import click
from suthing import FileHandle, Timer

from graflo.db.connection.onto import ArangoConfig, DBConfig

logger = logging.getLogger(__name__)


def act_db(
    conf: ArangoConfig,
    db_name: str,
    output_path: pathlib.Path,
    restore: bool,
    docker_version: str,
    use_docker: bool,
):
    """Execute database backup or restore operation.

    This function performs either a backup (arangodump) or restore (arangorestore)
    operation on an ArangoDB database. It can use either the local arangodump/arangorestore
    tools or run them in a Docker container.

    Args:
        conf: Database connection configuration
        db_name: Name of the database to backup/restore
        output_path: Path where backup will be stored or restored from
        restore: Whether to restore (True) or backup (False)
        docker_version: Version of ArangoDB Docker image to use
        use_docker: Whether to use Docker for the operation

    Returns:
        None

    Raises:
        subprocess.CalledProcessError: If the backup/restore operation fails
    """
    host = f"tcp://{conf.hostname}:{conf.port}"
    db_folder = output_path / db_name

    cmd = "arangorestore" if restore else "arangodump"
    if use_docker:
        ru = (
            f"docker run --rm --network=host -v {db_folder}:/dump"
            f" arangodb/arangodb:{docker_version} {cmd}"
        )
        output = "--output-directory /dump"
    else:
        ru = f"{cmd}"
        output = f"--output-directory {db_folder}"

    dir_spec = "input" if restore else "output"

    query = f"""{ru} --server.endpoint {host} --server.username {conf.username} --server.password "{conf.password}" --{dir_spec}-directory {output} --server.database "{db_name}" """

    restore_suffix = "--create-database true --force-same-database true"
    if restore:
        query += restore_suffix
    else:
        query += "--overwrite true"

    flag = subprocess.run(query, shell=True)
    logger.info(f"returned {flag}")


@click.command()
@click.option(
    "--db-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=False,
    default=None,
)
@click.option("--db-host", type=str)
@click.option("--db-password", type=str)
@click.option("--db-user", type=str, default="root")
@click.option(
    "--db",
    type=str,
    multiple=True,
    required=True,
    help="filesystem path where to dump db snapshot",
)
@click.option(
    "--store-directory-path",
    type=click.Path(path_type=pathlib.Path),
    required=True,
    help="filesystem path where to dump db snapshot",
)
@click.option("--docker-version", type=str, default="3.12.1")
@click.option("--restore", type=bool, default=False, is_flag=True)
@click.option("--use-docker", type=bool, default=True)
def manage_dbs(
    db_config_path,
    db_host,
    db_password,
    db_user,
    db,
    store_directory_path,
    restore,
    docker_version,
    use_docker=True,
):
    """Manage ArangoDB database backups and restores.

    This command provides functionality to backup and restore ArangoDB databases.
    It supports both local execution and Docker-based operations. The command can
    process multiple databases in sequence and provides timing information for
    each operation.

    Args:
        db_config_path: Path to database configuration file (optional)
        db_host: Database host address (if not using config file)
        db_password: Database password (if not using config file)
        db_user: Database username (default: root)
        db: List of database names to process
        store_directory_path: Path where backups will be stored/restored
        restore: Whether to restore (True) or backup (False)
        docker_version: Version of ArangoDB Docker image (default: 3.12.1)
        use_docker: Whether to use Docker for operations (default: True)

    Example:
        $ uv run manage_dbs \\
            --db-config-path config/db.yaml \\
            --db mydb1 mydb2 \\
            --store-directory-path /backups \\
            --use-docker
    """
    if db_config_path is None:
        # Construct URI from host
        uri = db_host if db_host and "://" in db_host else f"http://{db_host}"
        db_conf = ArangoConfig(uri=uri, username=db_user, password=db_password)
    else:
        conn_conf = FileHandle.load(fpath=db_config_path)
        db_conf_raw = DBConfig.from_dict(conn_conf)
        # Type checker can't infer the specific type, but we know it's ArangoConfig from the config
        if not isinstance(db_conf_raw, ArangoConfig):
            raise ValueError(f"Expected ArangoConfig, got {type(db_conf_raw)}")
        db_conf: ArangoConfig = db_conf_raw

    action = "restoring" if restore else "dumping"
    if restore:
        out_path = store_directory_path
    else:
        out_path = (
            store_directory_path.expanduser().resolve() / date.today().isoformat()
        )

        if not out_path.exists():
            out_path.mkdir(exist_ok=True)

    with Timer() as t_all:
        for dbname in db:
            with Timer() as t_dump:
                try:
                    act_db(
                        db_conf,
                        dbname,
                        out_path,
                        restore=restore,
                        docker_version=docker_version,
                        use_docker=use_docker,
                    )
                except Exception as e:
                    logging.error(e)
            logging.info(
                f"{action} {dbname} took  {t_dump.mins} mins {t_dump.secs:.2f} sec"
            )
    logging.info(f"all {action} took  {t_all.mins} mins {t_all.secs:.2f} sec")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    manage_dbs()
