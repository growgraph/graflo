"""Data chunking utilities for efficient file processing.

This module provides utilities for processing large files by breaking them into
manageable chunks. It supports various file formats (JSON, JSONL, CSV) and provides
both file-based and in-memory chunking capabilities.

Key Components:
    - AbstractChunker: Base class for chunking implementations
    - FileChunker: File-based chunking with encoding support
    - TableChunker: CSV/TSV file chunking
    - JsonlChunker: JSON Lines file chunking
    - JsonChunker: JSON file chunking
    - TrivialChunker: In-memory list chunking
    - ChunkerDataFrame: Pandas DataFrame chunking
    - ChunkerFactory: Factory for creating appropriate chunkers

Example:
    >>> chunker = ChunkerFactory.create_chunker(
    ...     resource="data.json",
    ...     type=ChunkerType.JSON,
    ...     batch_size=1000
    ... )
    >>> for batch in chunker:
    ...     process_batch(batch)
"""

import abc
import csv
import gc
import gzip
import json
import logging
import pathlib
import re
from contextlib import contextmanager
from pathlib import Path
from shutil import copyfileobj
from typing import Any, Callable, TextIO, TypeVar
from xml.etree import ElementTree as et

import ijson
import pandas as pd
import xmltodict

from graflo.architecture.onto import BaseEnum, EncodingType

AbstractChunkerType = TypeVar("AbstractChunkerType", bound="AbstractChunker")

logger = logging.getLogger(__name__)


class ChunkerType(BaseEnum):
    """Types of chunkers supported by the system.

    JSON: For JSON files
    JSONL: For JSON Lines files
    TABLE: For CSV/TSV files
    TRIVIAL: For in-memory lists
    """

    JSON = "json"
    JSONL = "jsonl"
    TABLE = "table"
    TRIVIAL = "trivial"


class AbstractChunker(abc.ABC):
    """Abstract base class for chunking implementations.

    This class defines the interface for all chunkers, providing common
    functionality for batch processing and iteration.

    Args:
        batch_size: Number of items per batch (default: 10)
        limit: Maximum number of items to process (default: None)

    Attributes:
        units_processed: Number of items processed
        batch_size: Size of each batch
        limit: Maximum number of items to process
        cnt: Current count of processed items
        iteration_tried: Whether iteration has been attempted
    """

    def __init__(self, batch_size=10, limit=None):
        self.units_processed = 0
        self.batch_size = batch_size
        self.limit: int | None = limit
        self.cnt = 0
        self.iteration_tried = False

    def _limit_reached(self):
        """Check if the processing limit has been reached.

        Returns:
            bool: True if limit is reached, False otherwise
        """
        return self.limit is not None and self.cnt >= self.limit

    def __iter__(self):
        """Initialize iteration if not already done.

        Returns:
            self: Iterator instance
        """
        if not self.iteration_tried:
            self._prepare_iteration()
        return self

    def __next__(self):
        """Get the next batch of items.

        Returns:
            list: Next batch of items

        Raises:
            StopIteration: When no more items are available or limit is reached
        """
        batch = self._next_item()
        self.cnt += len(batch)
        if not batch or self._limit_reached():
            raise StopIteration
        return batch

    @abc.abstractmethod
    def _next_item(self):
        """Get the next item or batch of items.

        This method must be implemented by subclasses.

        Returns:
            Any: Next item or batch of items
        """
        pass

    def _prepare_iteration(self):
        """Prepare for iteration.

        This method is called before the first iteration attempt.
        """
        self.iteration_tried = True


class FileChunker(AbstractChunker):
    """Base class for file-based chunking.

    This class provides functionality for reading and chunking files,
    with support for different encodings and compression.

    Args:
        filename: Path to the file to process
        encoding: File encoding (default: UTF_8)
        mode: File mode ('t' for text, 'b' for binary)
        **kwargs: Additional arguments for AbstractChunker

    Attributes:
        filename: Path to the file
        file_obj: File object for reading
        encoding: File encoding
        mode: File mode
    """

    def __init__(
        self,
        filename,
        encoding: EncodingType = EncodingType.UTF_8,
        mode="t",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.filename: Path = filename
        self.file_obj: TextIO | gzip.GzipFile | None = None
        self.encoding: EncodingType | None = encoding
        self.mode = mode
        if self.mode == "b":
            self.encoding = None

    def _next_item(self):
        """Get the next line from the file.

        Returns:
            str: Next line from the file

        Raises:
            StopIteration: When end of file is reached
            RuntimeError: If file is not opened (should not happen in normal flow)
        """
        # file_obj is guaranteed to be open after _prepare_iteration() is called
        if self.file_obj is None:
            raise RuntimeError("File should be opened before calling _next_item()")
        return next(self.file_obj)

    def _prepare_iteration(self):
        """Open the file for reading.

        Handles both regular and gzipped files.
        """
        super()._prepare_iteration()
        if ".gz" in self.filename.suffixes:
            self.file_obj = gzip.open(
                self.filename.absolute().as_posix(),
                f"r{self.mode}",
                encoding=self.encoding,
            )
        else:
            self.file_obj = open(
                self.filename.absolute().as_posix(),
                f"r{self.mode}",
                encoding=self.encoding,
            )

    def __next__(self):
        """Get the next batch of lines.

        Returns:
            list[str]: Next batch of lines

        Raises:
            StopIteration: When end of file is reached or limit is reached
            RuntimeError: If file is not opened (should not happen in normal flow)
        """
        batch = []

        if self._limit_reached():
            if self.file_obj is not None:
                self.file_obj.close()
            raise StopIteration
        while len(batch) < self.batch_size and not self._limit_reached():
            try:
                batch += [self._next_item()]
                self.cnt += 1
            except StopIteration:
                if batch:
                    return batch
                if self.file_obj is not None:
                    self.file_obj.close()
                raise StopIteration

        return batch


class TableChunker(FileChunker):
    """Chunker for CSV/TSV files.

    This class extends FileChunker to handle tabular data, converting
    each row into a dictionary with column headers as keys.

    Args:
        **kwargs: Arguments for FileChunker, including:
            sep: Field separator (default: ',')
    """

    def __init__(self, **kwargs):
        self.sep = kwargs.pop("sep", ",")
        super().__init__(**kwargs)
        self.header: list[str]

    def _prepare_iteration(self):
        """Read the header row and prepare for iteration."""
        super()._prepare_iteration()
        # After super()._prepare_iteration(), file_obj is guaranteed to be open
        if self.file_obj is None:
            raise RuntimeError("File should be opened by parent _prepare_iteration()")
        header = next(self.file_obj)
        self.header = header.rstrip("\n").split(self.sep)

    def __next__(self):
        """Get the next batch of rows as dictionaries.

        Returns:
            list[dict]: Next batch of rows as dictionaries
        """
        lines = super().__next__()
        lines2 = [
            next(csv.reader([line.rstrip()], skipinitialspace=True)) for line in lines
        ]
        dressed = [dict(zip(self.header, row)) for row in lines2]
        return dressed


class JsonlChunker(FileChunker):
    """Chunker for JSON Lines files.

    This class extends FileChunker to handle JSON Lines format,
    parsing each line as a JSON object.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __next__(self):
        """Get the next batch of JSON objects.

        Returns:
            list[dict]: Next batch of parsed JSON objects
        """
        lines = super().__next__()
        lines2 = [json.loads(line) for line in lines]
        return lines2


class JsonChunker(FileChunker):
    """Chunker for JSON files.

    This class extends FileChunker to handle JSON files using
    streaming JSON parsing for memory efficiency.
    """

    def __init__(self, **kwargs):
        super().__init__(mode="b", **kwargs)
        self.parser: Any

    def _prepare_iteration(self):
        """Initialize the JSON parser for streaming."""
        super()._prepare_iteration()
        # After super()._prepare_iteration(), file_obj is guaranteed to be open
        if self.file_obj is None:
            raise RuntimeError("File should be opened by parent _prepare_iteration()")
        self.parser = ijson.items(self.file_obj, "item")

    def _next_item(self):
        """Get the next JSON object.

        Returns:
            dict: Next parsed JSON object

        Raises:
            StopIteration: When end of file is reached
        """
        return next(self.parser)


class TrivialChunker(AbstractChunker):
    """Chunker for in-memory lists.

    This class provides chunking functionality for lists of dictionaries
    that are already in memory.

    Args:
        array: List of dictionaries to chunk
        **kwargs: Additional arguments for AbstractChunker
    """

    def __init__(self, array: list[dict], **kwargs):
        super().__init__(**kwargs)
        self.array = array

    def _next_item(self):
        """Get the next batch of items from the array.

        Returns:
            list[dict]: Next batch of items
        """
        return self.array[self.cnt : self.cnt + self.batch_size]

    def __next__(self):
        """Get the next batch of items.

        Returns:
            list[dict]: Next batch of items

        Raises:
            StopIteration: When no more items are available or limit is reached
        """
        batch = self._next_item()
        self.cnt += len(batch)
        if not batch or self._limit_reached():
            raise StopIteration
        return batch


class ChunkerDataFrame(AbstractChunker):
    """Chunker for Pandas DataFrames.

    This class provides chunking functionality for Pandas DataFrames,
    converting each chunk into a list of dictionaries.

    Args:
        df: DataFrame to chunk
        **kwargs: Additional arguments for AbstractChunker
    """

    def __init__(self, df: pd.DataFrame, **kwargs):
        super().__init__(**kwargs)
        self.df = df
        self.columns = df.columns

    def _next_item(self):
        """Get the next batch of rows as dictionaries.

        Returns:
            list[dict]: Next batch of rows as dictionaries
        """
        cid = self.cnt
        pre_batch = self.df.iloc[cid : cid + self.batch_size].values.tolist()
        batch = [{k: v for k, v in zip(self.columns, item)} for item in pre_batch]
        return batch


class ChunkerFactory:
    """Factory for creating appropriate chunkers.

    This class provides a factory method for creating chunkers based on
    the type of resource and configuration provided.

    Example:
        >>> chunker = ChunkerFactory.create_chunker(
        ...     resource="data.json",
        ...     type=ChunkerType.JSON,
        ...     batch_size=1000
        ... )
    """

    @classmethod
    def _guess_chunker_type(cls, filename: Path) -> ChunkerType:
        """Guess the appropriate chunker type based on file extension.

        This method examines the file extension to determine the most appropriate
        chunker type. It supports common file extensions for JSON, JSONL, and CSV/TSV files,
        including compressed versions (e.g., .json.gz, .csv.gz).

        Args:
            filename: Path to the file to analyze

        Returns:
            ChunkerType: Guessed chunker type based on file extension

        Raises:
            ValueError: If file extension is not recognized
        """
        # Get all suffixes and remove compression extensions
        suffixes = filename.suffixes
        base_suffix = [y for y in suffixes if y.lower() not in (".gz", ".zip")][
            -1
        ].lower()

        if base_suffix == ".json":
            return ChunkerType.JSON
        elif base_suffix == ".jsonl":
            return ChunkerType.JSONL
        elif base_suffix in (".csv", ".tsv", ".txt"):
            return ChunkerType.TABLE
        else:
            raise ValueError(
                f"Could not guess chunker type for file extension: {base_suffix}"
            )

    @classmethod
    def create_chunker(cls, **kwargs) -> AbstractChunker:
        """Create an appropriate chunker for the given resource.

        Args:
            **kwargs: Configuration for the chunker, including:
                resource: Path to file, list, or DataFrame
                type: Type of chunker to create (optional, will be guessed if None)
                batch_size: Size of each batch
                limit: Maximum number of items to process

        Returns:
            AbstractChunker: Appropriate chunker instance

        Raises:
            ValueError: If resource type is not supported or chunker type cannot be guessed
        """
        resource: Path | list[dict] | pd.DataFrame | None = kwargs.pop("resource", None)
        chunker_type = kwargs.pop("type", None)

        if isinstance(resource, list):
            return TrivialChunker(array=resource, **kwargs)
        elif isinstance(resource, pd.DataFrame):
            return ChunkerDataFrame(df=resource, **kwargs)
        elif isinstance(resource, Path):
            if chunker_type is None:
                chunker_type = cls._guess_chunker_type(resource)
            if chunker_type == ChunkerType.JSON:
                return JsonChunker(filename=resource, **kwargs)
            elif chunker_type == ChunkerType.JSONL:
                return JsonlChunker(filename=resource, **kwargs)
            elif chunker_type == ChunkerType.TABLE:
                return TableChunker(filename=resource, **kwargs)
            else:
                raise ValueError(f"Unknown chunker type: {chunker_type}")
        else:
            raise ValueError(f"Unsupported resource type: {type(resource)}")


class ChunkFlusherMono:
    """Monolithic chunk flusher for writing data to files.

    This class provides functionality for writing chunks of data to files,
    with support for file naming and size limits.

    Args:
        target_prefix: Prefix for output files
        chunksize: Maximum number of items per file
        maxchunks: Maximum number of chunks to write
        suffix: File suffix (default: '.json')
    """

    def __init__(self, target_prefix, chunksize, maxchunks=None, suffix=None):
        self.target_prefix = target_prefix
        self.acc = []
        self.chunk_count = 0
        self.chunksize = chunksize
        self.maxchunks = maxchunks
        self.iprocessed = 0
        self.suffix = "good" if suffix is None else suffix
        logger.info(f" in flush_chunk {self.chunksize}")

    def flush_chunk(self):
        """Write the current chunk to a file."""
        logger.info(
            f" in flush_chunk: : {len(self.acc)}; chunk count : {self.chunk_count}"
        )
        if len(self.acc) > 0:
            filename = f"{self.target_prefix}#{self.suffix}#{self.chunk_count}.json.gz"
            with gzip.GzipFile(filename, "w") as fout:
                fout.write(json.dumps(self.acc, indent=4).encode("utf-8"))
                logger.info(f" flushed {filename}")
                self.chunk_count += 1
                self.iprocessed += len(self.acc)
                self.acc = []

    def push(self, item):
        """Add an item to the current chunk.

        Args:
            item: Item to add to the chunk
        """
        self.acc.append(item)
        if len(self.acc) >= self.chunksize:
            self.flush_chunk()
            gc.collect()

    def stop(self):
        """Flush any remaining items and close."""
        return self.maxchunks is not None and (self.chunk_count >= self.maxchunks)

    def items_processed(self):
        """Get the total number of items processed.

        Returns:
            int: Number of items processed
        """
        return self.iprocessed


class FPSmart:
    """Smart file pointer for pattern-based file processing.

    This class provides a file-like interface with pattern-based
    transformation of the data being read.

    Args:
        fp: File pointer to wrap
        pattern: Regular expression pattern to match
        substitute: String to substitute for matches
        count: Maximum number of substitutions (0 for unlimited)
    """

    def __init__(self, fp, pattern, substitute="", count=0):
        self.fp = fp
        self.pattern = pattern
        self.p = re.compile(self.pattern)
        self.count = count
        self.sub = substitute

    def read(self, n):
        """Read and transform data from the file.

        Args:
            n: Number of bytes to read

        Returns:
            str: Transformed data
        """
        s = self.fp.read(n).decode()
        return self.transform(s).encode()

    def transform(self, s):
        """Transform the data using the pattern.

        Args:
            s: Data to transform

        Returns:
            str: Transformed data
        """
        self.p.search(s)
        r = self.p.sub(self.sub, s, count=self.count)
        return r

    def close(self):
        """Close the underlying file pointer."""
        self.fp.close()


tag_wos = "REC"
pattern_wos = r"xmlns=\".*[^\"]\"(?=>)"
force_list_wos = (
    "abstract",
    "address_name",
    "book_note",
    "conf_date",
    "conf_info",
    "conf_location",
    "conf_title",
    "conference",
    "contributor",
    "doctype",
    "grant",
    "grant_id",
    "heading",
    "identifier",
    "keyword",
    "language",
    "name",
    "organization",
    "p",
    "publisher",
    "reference",
    "rw_author",
    "sponsor",
    "subheading",
    "subject",
    "suborganization",
    "title",
    "edition",
    "zip",
)


@contextmanager
def nullcontext(enter_result=None):
    """Context manager that does nothing.

    Args:
        enter_result: Value to return when entering the context

    Yields:
        The enter_result value
    """
    yield enter_result


def gunzip_file(fname_in, fname_out):
    """Decompress a gzipped file.

    Args:
        fname_in: Path to input gzipped file
        fname_out: Path to output decompressed file
    """
    with gzip.open(fname_in, "rb") as f_in:
        with open(fname_out, "wb") as f_out:
            copyfileobj(f_in, f_out)


def parse_simple(fp, good_cf, force_list=None, root_tag=None):
    """Parse XML file with simple structure.

    Args:
        fp: File pointer to parse
        good_cf: Function to check if an element is valid
        force_list: List of tags that should always be lists
        root_tag: Root tag to start parsing from

    Returns:
        dict: Parsed XML data
    """
    events = ("start", "end")
    tree = et.iterparse(fp, events)
    context = iter(tree)
    event, root = next(context)
    for event, pub in context:
        if event == "end" and (pub.tag == root_tag if root_tag is not None else True):
            item = et.tostring(pub, encoding="utf8", method="xml").decode("utf")
            obj = xmltodict.parse(
                item,
                force_cdata=True,
                force_list=force_list,
            )
            good_cf.push(obj)
            root.clear()
            if good_cf.stop():
                break


def convert(
    source: pathlib.Path,
    target_root: str,
    chunk_size: int = 10000,
    max_chunks=None,
    pattern: str | None = None,
    force_list=None,
    root_tag=None,
):
    """Convert XML file to JSON chunks.

    This function processes an XML file and converts it to a series of JSON files,
    with support for pattern-based transformation and chunking.

    Args:
        source: Path to source XML file
        target_root: Root path for output files
        chunk_size: Number of items per output file (default: 10000)
        max_chunks: Maximum number of chunks to create (default: None)
        pattern: Regular expression pattern for transformation
        force_list: List of tags that should always be lists
        root_tag: Root tag to start parsing from

    Example:
        >>> convert(
        ...     source="data.xml",
        ...     target_root="output",
        ...     chunk_size=1000,
        ...     pattern=r'xmlns="[^"]*"',
        ...     root_tag="PubmedArticle"
        ... )
    """
    logger.info(f" chunksize : {chunk_size} | maxchunks {max_chunks} ")

    good_cf = ChunkFlusherMono(target_root, chunk_size, max_chunks)
    bad_cf = ChunkFlusherMono(target_root, chunk_size, max_chunks, suffix="bad")

    if source.suffix == ".gz":
        open_foo: Callable = gzip.open
    elif source.suffix == ".xml":
        open_foo = open
    else:
        raise ValueError("Unknown file type")
    # pylint: disable-next=assignment
    fp: gzip.GzipFile | FPSmart | None

    with (
        open_foo(source, "rb")
        if isinstance(  # type: ignore
            source, pathlib.Path
        )
        else nullcontext() as fp
    ):
        if pattern is not None:
            fp = FPSmart(fp, pattern)
        else:
            fp = fp
        parse_simple(fp, good_cf, force_list, root_tag)

        good_cf.flush_chunk()

        logger.info(f" {good_cf.items_processed()} good records")
        bad_cf.flush_chunk()
        logger.info(f"{bad_cf.items_processed()} bad records")
