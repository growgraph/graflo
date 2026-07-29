"""XML to JSON conversion tool for data preprocessing.

This module provides a command-line tool for converting XML files to JSON format,
with support for different data sources and chunking options. It's particularly
useful for preprocessing scientific literature data from sources like Web of Science
and PubMed.

Key Features:
    - Support for Web of Science and PubMed XML formats
    - Configurable chunking for large files
    - Batch processing of multiple files
    - Customizable output format

Example:
    $ uv run xml2json \\
        --source-path data/wos.xml \\
        --chunk-size 1000 \\
        --mode wos_csv
"""

import logging
import pathlib
import sys

import click

from graflo.data_source.chunker import convert, force_list_wos, tag_wos

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "-s",
    "--source-path",
    type=click.Path(path_type=pathlib.Path),
    required=True,
)
@click.option("-c", "--chunk-size", type=int, default=1000)
@click.option("-m", "--max-chunks", type=int, default=None)
@click.option("--mode", type=str)
def do(source_path, chunk_size, max_chunks, mode):
    """Convert XML files to JSON format.

    This command processes XML files and converts them to JSON format, with support
    for different data sources and chunking options.

    Args:
        source_path: Path to source XML file or directory
        chunk_size: Number of records per output file (default: 1000)
        max_chunks: Maximum number of chunks to process (default: None)
        mode: Data source mode ('wos_csv' or 'pubmed')

    Example:
        $ uv run xml2json \\
            --source-path data/wos.xml \\
            --chunk-size 1000 \\
            --mode wos_csv
    """
    if mode == "wos_csv":
        pattern = r"xmlns=\".*[^\"]\"(?=>)"
        force_list = force_list_wos
        tag = tag_wos
    elif mode == "pubmed":
        pattern = None
        force_list = None
        tag = "PubmedArticle"
    else:
        raise ValueError(f"Unknown mode {mode}")

    if source_path.is_dir():
        files = [
            fp for fp in source_path.iterdir() if not fp.is_dir() and "xml" in fp.name
        ]
    else:
        files = [source_path] if ".xml." in source_path.name else []
    for fp in files:
        target_root = str(fp.parent / fp.name.split(".")[0])

        convert(
            fp,
            target_root=target_root,
            chunk_size=chunk_size,
            max_chunks=max_chunks,
            pattern=pattern,
            force_list=force_list,
            root_tag=tag,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    do()
