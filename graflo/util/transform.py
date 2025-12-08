"""Data transformation utilities for graph operations.

This module provides utility functions for transforming and standardizing data
in various formats, particularly for graph database operations. It includes
functions for date parsing, string standardization, and data cleaning.

Key Functions:
    - standardize: Standardize string keys and names
    - parse_date_*: Various date parsing functions for different formats
    - cast_ibes_analyst: Parse and standardize analyst names
    - clear_first_level_nones: Clean dictionaries by removing None values
    - parse_multi_item: Parse complex multi-item strings
    - pick_unique_dict: Remove duplicate dictionaries

Example:
    >>> name = standardize("John. Doe, Smith")
    >>> date = parse_date_standard("2023-01-01")
    >>> analyst = cast_ibes_analyst("ADKINS/NARRA")
"""

import logging
import re
import time
from collections import defaultdict
from datetime import datetime

ORDINAL_SUFFIX = ["st", "nd", "rd", "th"]

logger = logging.getLogger(__name__)


def standardize(k):
    """Standardizes a string key by removing periods and splitting.

    Handles comma and space-separated strings, normalizing their format.

    Args:
        k (str): Input string to be standardized.

    Returns:
        str: Cleaned and standardized string.

    Example:
        >>> standardize("John. Doe, Smith")
        'John,Doe,Smith'
        >>> standardize("John Doe Smith")
        'John,Doe,Smith'
    """
    k = k.translate(str.maketrans({".": ""}))
    # try to split by ", "
    k = k.split(", ")
    if len(k) < 2:
        k = k[0].split(" ")
    else:
        k[1] = k[1].translate(str.maketrans({" ": ""}))
    return ",".join(k)


def parse_date_standard(input_str):
    """Parse a date string in YYYY-MM-DD format.

    Args:
        input_str (str): Date string in YYYY-MM-DD format.

    Returns:
        tuple: (year, month, day) as integers.

    Example:
        >>> parse_date_standard("2023-01-01")
        (2023, 1, 1)
    """
    dt = datetime.strptime(input_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def parse_date_conf(input_str):
    """Parse a date string in YYYYMMDD format.

    Args:
        input_str (str): Date string in YYYYMMDD format.

    Returns:
        tuple: (year, month, day) as integers.

    Example:
        >>> parse_date_conf("20230101")
        (2023, 1, 1)
    """
    dt = datetime.strptime(input_str, "%Y%m%d")
    return dt.year, dt.month, dt.day


def parse_date_ibes(date0, time0):
    """Converts IBES date and time to ISO 8601 format datetime.

    Args:
        date0 (str/int): Date in YYYYMMDD format.
        time0 (str): Time in HH:MM:SS format.

    Returns:
        str: Datetime in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).

    Example:
        >>> parse_date_ibes(20160126, "9:35:52")
        '2016-01-26T09:35:52Z'
    """
    date0 = str(date0)
    year, month, day = date0[:4], date0[4:6], date0[6:]
    full_datetime = f"{year}-{month}-{day}T{time0}Z"

    return full_datetime


def parse_date_yahoo(date0):
    """Convert Yahoo Finance date to ISO 8601 format.

    Args:
        date0 (str): Date in YYYY-MM-DD format.

    Returns:
        str: Datetime in ISO 8601 format with noon time.

    Example:
        >>> parse_date_yahoo("2023-01-01")
        '2023-01-01T12:00:00Z'
    """
    full_datetime = f"{date0}T12:00:00Z"
    return full_datetime


def round_str(x, **kwargs):
    """Round a string number to specified precision.

    Args:
        x (str): String representation of a number.
        **kwargs: Additional arguments for round() function.

    Returns:
        float: Rounded number.

    Example:
        >>> round_str("3.14159", ndigits=2)
        3.14
    """
    return round(float(x), **kwargs)


def parse_date_standard_to_epoch(input_str):
    """Convert standard date string to Unix epoch timestamp.

    Args:
        input_str (str): Date string in YYYY-MM-DD format.

    Returns:
        float: Unix epoch timestamp.

    Example:
        >>> parse_date_standard_to_epoch("2023-01-01")
        1672531200.0
    """
    dt = datetime.strptime(input_str, "%Y-%m-%d").timetuple()
    timestamp = time.mktime(dt)
    return timestamp


def cast_ibes_analyst(s):
    """Splits and normalizes analyst name strings.

    Handles various name formats like 'ADKINS/NARRA' or 'ARFSTROM      J'.

    Args:
        s (str): Analyst name string.

    Returns:
        tuple: (last_name, first_initial)

    Examples:
        >>> cast_ibes_analyst('ADKINS/NARRA')
        ('ADKINS', 'N')
        >>> cast_ibes_analyst('ARFSTROM      J')
        ('ARFSTROM', 'J')
    """
    if " " in s or "\t" in s:
        r = s.split()[:2]
        if len(r) < 2:
            return r[0], ""
        else:
            return r[0], r[1][:1]
    else:
        r = s.split("/")
        if s.startswith("/"):
            r = r[1:3]
        else:
            r = r[:2]
        if len(r) < 2:
            return r[0], ""
        else:
            return r[0], r[1][:1]


def parse_date_reference(input_str):
    """Extract year from a date reference string.

    Args:
        input_str (str): Date reference string.

    Returns:
        int: Year from the date reference.

    Example:
        >>> parse_date_reference("1923, May 10")
        1923
    """
    return _parse_date_reference(input_str)["year"]


def _parse_date_reference(input_str):
    """Parse complex, human-written date references.

    Handles various date formats like:
    - "1923, May 10"
    - "1923, July"
    - "1921, Sept"
    - "1935-36"
    - "1926, December 24th"

    Args:
        input_str (str): Date string in various formats.

    Returns:
        dict: Parsed date information with keys 'year', optional 'month', 'day'.

    Example:
        >>> _parse_date_reference("1923, May 10")
        {'year': 1923, 'month': 5, 'day': 10}
    """
    if "," in input_str:
        if len(input_str.split(" ")) == 3:
            if input_str[-2:] in ORDINAL_SUFFIX:
                input_str = input_str[:-2]
            try:
                dt = datetime.strptime(input_str, "%Y, %B %d")
                return {"year": dt.year, "month": dt.month, "day": dt.day}
            except:
                try:
                    aux = input_str.split(" ")
                    input_str = " ".join([aux[0]] + [aux[1][:3]] + [aux[2]])
                    dt = datetime.strptime(input_str, "%Y, %b %d")
                    return {"year": dt.year, "month": dt.month, "day": dt.day}
                except:
                    return {"year": input_str}
        else:
            try:
                dt = datetime.strptime(input_str, "%Y, %B")
                return {"year": dt.year, "month": dt.month}
            except:
                try:
                    aux = input_str.split(" ")
                    input_str = " ".join([aux[0]] + [aux[1][:3]])
                    dt = datetime.strptime(input_str, "%Y, %b")
                    return {"year": dt.year, "month": dt.month}
                except:
                    return {"year": input_str}
    else:
        try:
            dt = datetime.strptime(input_str[:4], "%Y")
            return {"year": dt.year}
        except:
            return {"year": input_str}


def try_int(x):
    """Attempt to convert a value to integer.

    Args:
        x: Value to convert.

    Returns:
        int or original value: Integer if conversion successful, original value otherwise.

    Example:
        >>> try_int("123")
        123
        >>> try_int("abc")
        'abc'
    """
    try:
        x = int(x)
        return x
    except:
        return x


def clear_first_level_nones(docs, keys_keep_nones: list | None = None):
    """Removes None values from dictionaries, with optional key exceptions.

    Args:
        docs (list): List of dictionaries to clean.
        keys_keep_nones (list, optional): Keys to keep even if their value is None.

    Returns:
        list: Cleaned list of dictionaries.

    Example:
        >>> docs = [{"a": 1, "b": None}, {"a": None, "b": 2}]
        >>> clear_first_level_nones(docs, keys_keep_nones=["a"])
        [{"a": 1}, {"a": None, "b": 2}]
    """
    if keys_keep_nones is not None:
        docs = [
            {k: v for k, v in tdict.items() if v or k in keys_keep_nones}
            for tdict in docs
        ]
    return docs


def parse_multi_item(s, mapper: dict, direct: list):
    """Parses complex multi-item strings into structured data.

    Supports parsing strings with quoted or bracketed items.

    Args:
        s (str): Input string to parse.
        mapper (dict): Mapping of input keys to output keys.
        direct (list): Direct keys to extract.

    Returns:
        defaultdict: Parsed items with lists as values.

    Example:
        >>> s = '[name: John, age: 30] [name: Jane, age: 25]'
        >>> mapper = {"name": "full_name"}
        >>> direct = ["age"]
        >>> parse_multi_item(s, mapper, direct)
        defaultdict(list, {'full_name': ['John', 'Jane'], 'age': ['30', '25']})
    """
    if "'" in s:
        items_str = re.findall(r"\"(.*?)\"", s) + re.findall(r"\'(.*?)\'", s)
    else:
        # remove brackets
        items_str = re.findall(r"\[([^]]+)", s)[0].split()
    r: defaultdict[str, list] = defaultdict(list)
    for item in items_str:
        doc0 = [ss.strip().split(":") for ss in item.split(",")]
        if all([len(x) == 2 for x in doc0]):
            doc0_dict = dict(doc0)
            for n_init, n_final in mapper.items():
                try:
                    r[n_final] += [doc0_dict[n_init]]
                except KeyError:
                    r[n_final] += [None]

            for n_final in direct:
                try:
                    r[n_final] += [doc0_dict[n_final]]
                except KeyError:
                    r[n_final] += [None]
        else:
            for key, value in zip(direct, doc0):
                r[key] += [value]

    return r


def pick_unique_dict(docs):
    """Removes duplicate dictionaries from a list.

    Uses a hash-based approach to identify unique dictionaries, which is more
    efficient than JSON serialization and preserves original object types.

    Args:
        docs (list): List of dictionaries.

    Returns:
        list: List of unique dictionaries (preserving original objects).

    Example:
        >>> docs = [{"a": 1}, {"a": 1}, {"b": 2}]
        >>> pick_unique_dict(docs)
        [{"a": 1}, {"b": 2}]
    """
    from datetime import date, datetime, time
    from decimal import Decimal

    def make_hashable(obj):
        """Convert an object to a hashable representation.

        Handles nested structures, datetime objects, and Decimal types.

        Args:
            obj: Object to make hashable

        Returns:
            Hashable representation of the object
        """
        if isinstance(obj, dict):
            # Sort items by key for consistent hashing
            return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
        elif isinstance(obj, (list, tuple)):
            return tuple(make_hashable(item) for item in obj)
        elif isinstance(obj, (datetime, date, time)):
            # Convert to ISO format string for hashing
            return ("__datetime__", obj.isoformat())
        elif isinstance(obj, Decimal):
            # Convert to string representation to preserve precision
            return ("__decimal__", str(obj))
        elif isinstance(obj, set):
            # Convert set to sorted tuple for consistent hashing
            return tuple(sorted(make_hashable(item) for item in obj))
        else:
            # Primitive types (int, float, str, bool, None) are already hashable
            return obj

    # Use a dict to preserve insertion order and original objects
    seen = {}
    for doc in docs:
        # Create hashable representation
        hashable_repr = make_hashable(doc)
        # Use hashable representation as key, original doc as value
        if hashable_repr not in seen:
            seen[hashable_repr] = doc

    # Return list of unique documents (preserving original objects)
    return list(seen.values())


def split_keep_part(s: str, sep="/", keep=-1) -> str:
    """Split a string and keep specified parts.

    Args:
        s (str): String to split.
        sep (str): Separator to split on.
        keep (int or list): Index or indices to keep.

    Returns:
        str: Joined string of kept parts.

    Example:
        >>> split_keep_part("a/b/c", keep=0)
        'a'
        >>> split_keep_part("a/b/c", keep=[0, 2])
        'a/c'
    """
    if isinstance(keep, list):
        items = s.split(sep)
        return sep.join(items[k] for k in keep)
    else:
        return s.split(sep)[keep]
