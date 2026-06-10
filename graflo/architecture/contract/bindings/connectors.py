"""Resource connector types (file, SQL table, SPARQL)."""

from __future__ import annotations

import abc
import hashlib
import json
import pathlib
import re
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import (
    AliasChoices,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import BaseEnum

from .column_time_filter import ColumnTimeFilter

# SQL identifier for TableConnector.base_alias validation (matches SelectSpec).
_BASE_TABLE_ALIAS_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\Z")

if TYPE_CHECKING:
    from graflo.db import PostgresConfig
    from graflo.data_source.api import APIConfig
    from graflo.connection_models import ApiAuth
    from graflo.filter.onto import FilterExpression
else:
    try:
        from graflo.db import PostgresConfig
    except ImportError:
        PostgresConfig = Any


class BoundSourceKind(BaseEnum):
    """Physical source modality for a bound connector (how rows are retrieved).

    This describes the connector-backed access pattern, not the abstract
    ingestion resource. File format (CSV, JSON, etc.) is chosen by the loader
    from file extensions.

    Attributes:
        FILE: File-based connector (directory + pattern or paths).
        SQL_TABLE: SQL table / database-backed connector.
        SPARQL: SPARQL / RDF connector (endpoint or local RDF via rdflib).
        API: REST API connector (path + pagination on a runtime base URL).
    """

    FILE = "file"
    SQL_TABLE = "sql_table"
    SPARQL = "sparql"
    API = "api"


class ConnectorUpdate(ConfigBaseModel):
    """Patch payload for an existing connector (applied after manifest load).

    Only ``connector`` is a fixed field; any other keys are captured as extras and
    merged into the resolved connector via ``model_validate`` (so validators,
    including hash recomputation, run). New connector types and fields do not
    require changes to this model. Not part of the stored ``GraphManifest``;
    load from a separate file or build in code, then call
    ``Bindings.apply_connector_update``.

    Attributes:
        connector: Connector ``name`` or ``hash`` (same resolution as bindings).
    """

    model_config = ConfigDict(extra="allow")

    connector: str = Field(
        ...,
        description="Connector reference: name or hash of the connector to patch.",
    )

    def as_patch(self) -> dict[str, Any]:
        """Return extra keys as a patch mapping (excludes ``connector``)."""
        return dict(self.model_extra or {})


class ResourceConnector(ConfigBaseModel, abc.ABC):
    """Abstract base class for resource connectors (files or tables).

    Provides common API for connector matching and resource identification.
    All concrete connector types inherit from this class.

    Connectors only describe source-side matching/query behavior. Resource-to-
    connector linkage is handled by ``Bindings``.
    """

    name: str | None = Field(
        default=None,
        description="Optional connector name used by top-level resource_connector mapping.",
    )
    resource_name: str | None = Field(
        default=None,
        description="Optional direct resource binding declared on the connector itself.",
    )
    hash: str = Field(
        default="",
        exclude=True,
        description="Deterministic internal connector id derived from defining fields.",
    )
    row_annotations: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Constant fields merged into every fetched row as defaults (response "
            "fields take priority). Only implemented for :class:`APIConnector`; "
            "other connector types reject non-empty values."
        ),
    )

    def _hash_payload(self) -> dict[str, Any]:
        payload = self.model_dump(
            mode="json",
            by_alias=True,
            exclude={"hash", "name", "resource_name"},
        )
        payload["_connector_type"] = type(self).__name__
        return payload

    @model_validator(mode="after")
    def _compute_hash(self) -> Self:
        canonical = json.dumps(
            self._hash_payload(), sort_keys=True, separators=(",", ":")
        )
        object.__setattr__(
            self,
            "hash",
            hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )
        return self

    @abc.abstractmethod
    def matches(self, resource_identifier: str) -> bool:
        """Check if connector matches a resource identifier.

        Args:
            resource_identifier: Identifier to match (filename or table name)

        Returns:
            bool: True if connector matches
        """
        pass

    @abc.abstractmethod
    def bound_source_kind(self) -> BoundSourceKind:
        """Return the physical source kind for this connector."""
        pass


class FileConnector(ResourceConnector):
    """Connector for matching files.

    Attributes:
        regex: Regular expression pattern for matching filenames
        sub_path: Path to search for matching files (default: "./")
        time_filter: Optional structured filter on a date/time column (shared with
            :class:`TableConnector`), using :class:`~graflo.architecture.contract.bindings.column_time_filter.ColumnTimeFilter`.
    """

    regex: str | None = None
    sub_path: pathlib.Path = Field(default_factory=lambda: pathlib.Path("./"))
    time_filter: ColumnTimeFilter | None = None

    @model_validator(mode="after")
    def _validate_file_connector(self) -> Self:
        """Ensure sub_path is a Path."""
        if not isinstance(self.sub_path, pathlib.Path):
            object.__setattr__(self, "sub_path", pathlib.Path(self.sub_path))
        if self.row_annotations:
            raise ValueError("row_annotations is not implemented for FileConnector")
        return self

    @property
    def date_field(self) -> str | None:
        """Column used for time filtering, if any (compat alias for ``time_filter.column``)."""
        return self.time_filter.column if self.time_filter else None

    def matches(self, resource_identifier: str) -> bool:
        """Check if connector matches a filename.

        Args:
            resource_identifier: Filename to match

        Returns:
            bool: True if connector matches
        """
        if self.regex is None:
            return False
        return bool(re.match(self.regex, resource_identifier))

    def bound_source_kind(self) -> BoundSourceKind:
        """File connector always uses ``BoundSourceKind.FILE``."""
        return BoundSourceKind.FILE


class JoinClause(ConfigBaseModel):
    """Specification for a SQL JOIN operation.

    Used by TableConnector to describe multi-table queries. Each JoinClause
    adds one JOIN to the generated SQL. The base row uses ``TableConnector.base_alias``
    (default ``base``), not a hard-coded name.

    Attributes:
        table: Table name to join (e.g. "all_classes").
        schema_name: Optional schema override for the joined table.
        alias: SQL alias for the joined table (e.g. "s", "t"). Required when
            the same table is joined more than once.
        on_self: Column on the base (left) table used in the ON condition.
        on_other: Column on the joined (right) table used in the ON condition.
        join_type: Type of join -- LEFT, INNER, etc. Defaults to LEFT.
        select_fields: Explicit list of columns to SELECT from this join.
            When None every column of the joined table is included (aliased
            with the join alias prefix).
    """

    table: str = Field(..., description="Table name to join.")
    schema_name: str | None = Field(
        default=None, description="Schema override for the joined table."
    )
    alias: str | None = Field(
        default=None, description="SQL alias for the joined table."
    )
    on_self: str = Field(
        ..., description="Column on the base table for the ON condition."
    )
    on_other: str = Field(
        ..., description="Column on the joined table for the ON condition."
    )
    join_type: str = Field(default="LEFT", description="JOIN type (LEFT, INNER, etc.).")
    select_fields: list[str] | None = Field(
        default=None,
        description="Columns to SELECT from this join (None = all columns).",
    )


class TableConnector(ResourceConnector):
    """Connector for matching database tables.

    Supports simple single-table queries as well as multi-table JOINs and
    pushdown filters via ``FilterExpression``.

    Attributes:
        table_name: Exact table name or regex pattern
        schema_name: Schema name (optional, defaults to public)
        database: Database name (optional)
        time_filter: Optional structured filter on a date/time column, rendered
            via :class:`~graflo.filter.onto.FilterExpression` in SQL.
        filters: General-purpose pushdown filters rendered as SQL WHERE fragments.
        joins: Multi-table JOIN specifications (auto-generated or explicit).
        base_alias: SQL alias for the base table when ``joins`` is non-empty.
        select_columns: Explicit SELECT column list. None means ``*`` for the
            base table (plus aliased columns from joins).
    """

    table_name: str = Field(
        default="", validation_alias=AliasChoices("table_name", "table")
    )
    schema_name: str | None = Field(
        default=None, validation_alias=AliasChoices("schema_name", "schema")
    )
    database: str | None = None
    time_filter: ColumnTimeFilter | None = None
    filters: list[Any] = Field(
        default_factory=list,
        description="Pushdown FilterExpression filters (rendered to SQL WHERE).",
    )
    joins: list[JoinClause] = Field(
        default_factory=list,
        description="JOIN clauses for multi-table queries.",
    )
    base_alias: str = Field(
        default="base",
        description="SQL alias for the base table row when joins are present.",
    )
    select_columns: list[str] | None = Field(
        default=None,
        description="Explicit SELECT columns. None = SELECT * (plus join aliases).",
    )
    view: Any = Field(
        default=None,
        description="SelectSpec or dict for declarative view (alternative to table+joins+filters).",
    )

    @field_validator("filters", mode="before")
    @classmethod
    def _coerce_filters(cls, v: Any) -> list[Any]:
        from graflo.filter.onto import parse_filter_expression

        if v is None:
            return []
        if not isinstance(v, list):
            raise ValueError("filters must be a list")
        result: list[Any] = []
        for i, item in enumerate(v):
            try:
                result.append(parse_filter_expression(item))
            except (ValueError, ValidationError) as e:
                raise ValueError(f"filters[{i}]: {e}") from e
        return result

    @field_validator("view", mode="before")
    @classmethod
    def _coerce_view(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, dict):
            from graflo.filter.select import SelectSpec

            return SelectSpec.from_dict(v)
        return v

    @model_validator(mode="after")
    def _validate_table_connector(self) -> Self:
        """Validate table_name and join wiring."""
        if not self.table_name:
            raise ValueError("table_name is required for TableConnector")
        if not _BASE_TABLE_ALIAS_IDENT.match(self.base_alias):
            raise ValueError(
                "base_alias must be a simple SQL identifier "
                "(ASCII letter, digit, underscore)"
            )
        join_aliases = {jc.alias or jc.table for jc in self.joins}
        if self.base_alias in join_aliases:
            raise ValueError(
                f"base_alias {self.base_alias!r} conflicts with a join alias "
                f"{sorted(join_aliases)}"
            )
        if self.row_annotations:
            raise ValueError("row_annotations is not implemented for TableConnector")
        return self

    @property
    def date_field(self) -> str | None:
        """Column used for time filtering, if any (compat alias for ``time_filter.column``)."""
        return self.time_filter.column if self.time_filter else None

    def matches(self, resource_identifier: str) -> bool:
        """Check if connector matches a table name.

        Args:
            resource_identifier: Table name to match (format: schema.table or just table)

        Returns:
            bool: True if connector matches
        """
        if not self.table_name:
            return False

        # Compile regex expression
        if self.table_name.startswith("^") or self.table_name.endswith("$"):
            # Already a regex expression
            compiled_regex = re.compile(self.table_name)
        else:
            # Exact match expression
            compiled_regex = re.compile(f"^{re.escape(self.table_name)}$")

        # Check if resource_identifier matches
        if compiled_regex.match(resource_identifier):
            return True

        # If schema_name is specified, also check schema.table format
        if self.schema_name:
            full_name = f"{self.schema_name}.{resource_identifier}"
            if compiled_regex.match(full_name):
                return True

        return False

    def bound_source_kind(self) -> BoundSourceKind:
        return BoundSourceKind.SQL_TABLE

    def build_where_clause(self, base_alias: str | None = None) -> str:
        """Build SQL WHERE clause from time filter **and** general filters.

        Returns:
            WHERE clause string (without the WHERE keyword) or empty string if no filters
        """
        from graflo.onto import ExpressionFlavor

        conditions: list[str] = []

        if self.time_filter is not None:
            expr = self.time_filter.as_filter_expression()
            if expr is not None:
                filt_expr = self._coerce_filter_expression(expr, base_alias)
                if filt_expr is not None:
                    rendered = filt_expr(kind=ExpressionFlavor.SQL)
                    if rendered:
                        conditions.append(str(rendered))

        # General-purpose FilterExpression filters
        for filt in self.filters:
            filt_expr = self._coerce_filter_expression(filt, base_alias)
            if filt_expr is not None:
                rendered = filt_expr(kind=ExpressionFlavor.SQL)
                if rendered:
                    conditions.append(str(rendered))

        if conditions:
            return " AND ".join(conditions)
        return ""

    def build_query(self, effective_schema: str | None = None) -> str:
        """Build a complete SQL SELECT query.

        When ``view`` is set, delegates to ``view.build_sql()``. Otherwise
        incorporates the base table, any JoinClauses, explicit select_columns,
        time_filter, and FilterExpression filters.

        Args:
            effective_schema: Schema to use if ``self.schema_name`` is None.

        Returns:
            Complete SQL query string.
        """
        schema = self.schema_name or effective_schema or "public"
        if self.view is not None:
            from graflo.filter.select import SelectSpec

            if isinstance(self.view, SelectSpec):
                query = self.view.build_sql(schema=schema, base_table=self.table_name)
                where = self.build_where_clause(base_alias=self.view.base_alias)
                if where:
                    return self._append_where_condition(query, where)
                return query
        base_alias = self.base_alias if self.joins else None
        base_ref = f'"{schema}"."{self.table_name}"'
        if base_alias:
            base_ref_aliased = f"{base_ref} {base_alias}"
        else:
            base_ref_aliased = base_ref

        # --- SELECT ---
        select_parts: list[str] = []
        if self.select_columns is not None:
            select_parts = list(self.select_columns)
        elif self.joins:
            select_parts.append(f"{base_alias}.*")
            for jc in self.joins:
                alias = jc.alias or jc.table
                jc_schema = jc.schema_name or schema
                if jc.select_fields is not None:
                    for col in jc.select_fields:
                        select_parts.append(f'{alias}."{col}" AS "{alias}__{col}"')
                else:
                    select_parts.append(f"{alias}.*")
        else:
            select_parts.append("*")

        select_clause = ", ".join(select_parts)

        # --- FROM + JOINs ---
        from_clause = base_ref_aliased
        for jc in self.joins:
            jc_schema = jc.schema_name or schema
            alias = jc.alias or jc.table
            join_ref = f'"{jc_schema}"."{jc.table}"'
            left_col = (
                f'{base_alias}."{jc.on_self}"' if base_alias else f'"{jc.on_self}"'
            )
            right_col = f'{alias}."{jc.on_other}"'
            from_clause += (
                f" {jc.join_type} JOIN {join_ref} {alias} ON {left_col} = {right_col}"
            )

        query = f"SELECT {select_clause} FROM {from_clause}"

        # --- WHERE ---
        where = self.build_where_clause(base_alias=base_alias)
        if where:
            query += f" WHERE {where}"

        return query

    @staticmethod
    def _append_where_condition(query: str, condition: str) -> str:
        """Append a SQL condition to *query* preserving an existing WHERE clause."""
        if re.search(r"\bWHERE\b", query, flags=re.IGNORECASE):
            return f"{query} AND {condition}"
        return f"{query} WHERE {condition}"

    @staticmethod
    def _qualified_column_ref(column: str, base_alias: str | None) -> str:
        if base_alias:
            return f'{base_alias}."{column}"'
        return f'"{column}"'

    @classmethod
    def _qualify_filter_payload(
        cls, payload: dict[str, Any], base_alias: str | None
    ) -> dict[str, Any]:
        qualified = dict(payload)
        if base_alias is None:
            return qualified
        if qualified.get("kind") == "leaf":
            field = qualified.get("field")
            if isinstance(field, str) and "." not in field:
                qualified["field"] = f"{base_alias}.{field}"
            return qualified
        deps = qualified.get("deps")
        if isinstance(deps, list):
            qualified["deps"] = [
                cls._qualify_filter_payload(dep, base_alias)
                if isinstance(dep, dict)
                else dep
                for dep in deps
            ]
        return qualified

    @classmethod
    def _coerce_filter_expression(
        cls, raw_filter: Any, base_alias: str | None
    ) -> FilterExpression | None:
        from graflo.filter.onto import parse_filter_expression

        if raw_filter is None:
            return None
        expr = parse_filter_expression(raw_filter)
        if base_alias is None:
            return expr
        payload = expr.model_dump(mode="python")
        return parse_filter_expression(cls._qualify_filter_payload(payload, base_alias))


class SparqlConnector(ResourceConnector):
    """Connector for matching SPARQL / RDF data sources.

    Each ``SparqlConnector`` targets instances of a single ``rdf:Class``.
    It can be backed either by a remote SPARQL endpoint (Fuseki, Blazegraph, ...)
    or by a local RDF file parsed with *rdflib*.

    Attributes:
        rdf_class: Full URI of the ``rdf:Class`` whose instances this connector
            fetches (e.g. ``"http://example.org/Person"``).
        endpoint_url: SPARQL query endpoint URL.  When set, instances are
            fetched via HTTP.  When ``None`` the connector is for local file mode.
        graph_uri: Named-graph URI to restrict the query to (optional).
        sparql_query: Custom SPARQL ``SELECT`` query override.  When provided
            the auto-generated per-class query is skipped.
        rdf_file: Path to a local RDF file (``.ttl``, ``.rdf``, ``.n3``,
            ``.jsonld``).  Mutually exclusive with *endpoint_url*.
    """

    rdf_class: str = Field(
        ..., description="URI of the rdf:Class to fetch instances of"
    )
    endpoint_url: str | None = Field(
        default=None, description="SPARQL query endpoint URL"
    )
    graph_uri: str | None = Field(
        default=None, description="Named graph URI (optional)"
    )
    sparql_query: str | None = Field(
        default=None, description="Custom SPARQL query override"
    )
    rdf_file: pathlib.Path | None = Field(
        default=None, description="Path to a local RDF file"
    )

    @model_validator(mode="after")
    def _reject_row_annotations(self) -> Self:
        # TODO: implement row_annotations for SparqlConnector row payloads.
        if self.row_annotations:
            raise ValueError("row_annotations is not implemented for SparqlConnector")
        return self

    def matches(self, resource_identifier: str) -> bool:
        """Match by the local name (fragment) of the rdf:Class URI.

        Args:
            resource_identifier: Identifier to match against

        Returns:
            True when *resource_identifier* equals the class local name
        """
        local_name = self.rdf_class.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
        return resource_identifier == local_name

    def bound_source_kind(self) -> BoundSourceKind:
        """Return ``BoundSourceKind.SPARQL``."""
        return BoundSourceKind.SPARQL

    def build_select_query(self) -> str:
        """Build a SPARQL SELECT query for instances of ``rdf_class``.

        If *sparql_query* is set it is returned as-is.  Otherwise a simple
        per-class query is generated::

            SELECT ?s ?p ?o WHERE {
              ?s a <rdf_class> .
              ?s ?p ?o .
            }

        Returns:
            SPARQL query string
        """
        if self.sparql_query:
            return self.sparql_query

        graph_open = f"GRAPH <{self.graph_uri}> {{" if self.graph_uri else ""
        graph_close = "}" if self.graph_uri else ""

        return (
            "SELECT ?s ?p ?o WHERE { "
            f"{graph_open} "
            f"?s a <{self.rdf_class}> . "
            f"?s ?p ?o . "
            f"{graph_close} "
            "}"
        )


class PaginationConfig(ConfigBaseModel):
    """Configuration for API pagination (contract-level, secret-free).

    Supports offset, cursor, and page-based strategies.
    """

    strategy: Literal["offset", "page", "cursor"] = "offset"
    offset_param: str = "offset"
    limit_param: str = Field(
        default="limit",
        description=(
            "Query parameter name for page size (offset strategy only). "
            "The value sent is ``page_size``, not a total item cap."
        ),
    )
    cursor_param: str = "cursor"
    page_param: str = "page"
    per_page_param: str = Field(
        default="per_page",
        description=(
            "Query parameter name for page size (page strategy only). "
            "The value sent is ``page_size``, not a total item cap."
        ),
    )
    initial_offset: int = 0
    initial_page: int = 1
    initial_cursor: str | None = None
    page_size: int = Field(
        default=100,
        description=(
            "Records requested per HTTP page. Sent as the value of "
            "``limit_param`` (offset) or ``per_page_param`` (page)."
        ),
    )
    cursor_path: str | None = None
    has_more_path: str | None = None
    data_path: str | None = None


class APIConnector(ResourceConnector):
    """Connector for REST API endpoints.

    Declares the non-secret access pattern (path, method, pagination). Runtime
    ``base_url`` and credentials are supplied via ``connector_connection`` ->
    ``conn_proxy`` -> :class:`~graflo.hq.connection_provider.ApiGeneralizedConnConfig`.

    Attributes:
        path: Relative endpoint path (e.g. ``/api/users``).
        method: HTTP method (default ``GET``).
        params: Static query parameters.
        pagination: Pagination strategy and response path configuration.
        row_annotations: Constant fields merged into every fetched row (doc wins).
        headers: Non-secret HTTP headers.
        timeout: Request timeout in seconds.
        retries: Number of retry attempts.
        retry_backoff_factor: Backoff factor for retries.
        retry_status_forcelist: HTTP status codes to retry on.
        verify: Verify SSL certificates.
    """

    path: str = Field(..., description="Relative API endpoint path")
    method: str = "GET"
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: PaginationConfig | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float | None = None
    retries: int = 0
    retry_backoff_factor: float = 0.1
    retry_status_forcelist: list[int] = Field(
        default_factory=lambda: [500, 502, 503, 504]
    )
    verify: bool = True

    @staticmethod
    def _join_url(base_url: str, path: str) -> str:
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    def matches(self, resource_identifier: str) -> bool:
        """Match resource name, connector name, or path tail."""
        if self.name is not None and resource_identifier == self.name:
            return True
        if self.resource_name is not None and resource_identifier == self.resource_name:
            return True
        path_tail = self.path.rstrip("/").rsplit("/", 1)[-1]
        return resource_identifier in {self.path, path_tail}

    def bound_source_kind(self) -> BoundSourceKind:
        return BoundSourceKind.API

    def build_api_config(
        self,
        *,
        base_url: str,
        auth: "ApiAuth | None" = None,
        default_headers: dict[str, str] | None = None,
        page_size_override: int | None = None,
    ) -> "APIConfig":
        """Merge contract fields with runtime connection config into ``APIConfig``."""
        from graflo.data_source.api import APIConfig

        headers = dict(default_headers or {})
        headers.update(self.headers)

        pagination = self.pagination
        if pagination is not None and page_size_override is not None:
            pagination = pagination.model_copy(update={"page_size": page_size_override})

        return APIConfig(
            url=self._join_url(base_url, self.path),
            method=self.method,
            headers=headers,
            auth=auth,
            params=dict(self.params),
            timeout=self.timeout,
            retries=self.retries,
            retry_backoff_factor=self.retry_backoff_factor,
            retry_status_forcelist=list(self.retry_status_forcelist),
            verify=self.verify,
            pagination=pagination,
            row_annotations=dict(self.row_annotations),
        )
