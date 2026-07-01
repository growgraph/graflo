"""TigerGraph connection implementation for graph database operations.

This module implements the Connection interface for TigerGraph. Implementation
is split across focused submodules; :class:`TigerGraphConnection` is the public
facade that wires them together.
"""

from __future__ import annotations

import contextlib
import logging
import re
from typing import TYPE_CHECKING, Any

from graflo.architecture.contract.bindings import Bindings
from graflo.architecture.graph_types import GraphContainer, Index
from graflo.architecture.schema import Schema
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.vertex import VertexConfig
from graflo.db.conn import Connection
from graflo.db.connection import TigergraphBulkLoadConfig, TigergraphConfig
from graflo.db.tigergraph import compat  # noqa: F401  # patch requests exceptions on import
from graflo.db.tigergraph.auth import TigerGraphAuth
from graflo.db.tigergraph.document_utils import clean_document, extract_id
from graflo.db.tigergraph.bulk_session import (
    bulk_load_append,
    bulk_load_begin,
    bulk_load_finalize,
)
from graflo.db.tigergraph.data_ops import TigerGraphDataOps
from graflo.db.tigergraph.graph_admin import GraphAdmin
from graflo.db.tigergraph.gsql_client import TigerGraphGsqlClient
from graflo.db.tigergraph.name_validation import (
    load_tigergraph_name_rules as _load_tigergraph_name_rules,
    validate_tigergraph_schema_name as _validate_tigergraph_schema_name,
)
from graflo.db.tigergraph.rest_client import TigerGraphRestClient
from graflo.db.tigergraph.schema_ddl import SchemaDdlBuilder
from graflo.db.tigergraph.token_cache import (
    TokenCacheKey,
    _CachedToken,
    _TigerGraphTokenCache,
    make_token_cache_key as _make_token_cache_key,
    parse_tg_expiration as _parse_tg_expiration,
    reset_tigergraph_token_cache,
)
from graflo.onto import DBType

if TYPE_CHECKING:
    from graflo.hq.connection_provider import ConnectionProvider

logger = logging.getLogger(__name__)


def _wrap_tg_exception(func):
    """Decorator kept for backward compatibility with existing method wrappers."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


class TigerGraphConnection(Connection):
    """TigerGraph database connection — facade over auth, REST, GSQL, DDL, and data ops."""

    flavor = DBType.TIGERGRAPH

    def __init__(self, config: TigergraphConfig):
        super().__init__()
        self.config = config
        self.ssl_verify = getattr(config, "ssl_verify", True)

        if self.config.database is None and self.config.schema_name is not None:
            self.config.database = self.config.schema_name
        elif self.config.schema_name is None and self.config.database is not None:
            self.config.schema_name = self.config.database

        configured_graph = self._configured_graph_name()
        self.graphname: str = (
            configured_graph if configured_graph is not None else "DefaultGraph"
        )
        self._installed_clear_data_queries: dict[str, set[str]] = {}

        gs_port: int | str | None = config.gs_port
        if gs_port is None:
            uri_port = config.port
            if uri_port:
                try:
                    gs_port = int(uri_port)
                    logger.debug(f"Using port {gs_port} from URI for GSQL endpoint")
                except (ValueError, TypeError):
                    pass

        if gs_port is None:
            raise ValueError(
                "gs_port or URI with port must be set in TigergraphConfig. "
                "Standard ports: 14240 (GSQL), 9000 (REST++)."
            )
        self.gsql_url = f"{config.url_without_port}:{gs_port}"

        self.tg_version: str | None = None
        self._use_restpp_prefix = False

        gsql_client = TigerGraphGsqlClient(self)
        object.__setattr__(self, "_auth_impl", TigerGraphAuth(self))
        object.__setattr__(self, "_rest_impl", TigerGraphRestClient(self))
        object.__setattr__(self, "_gsql_impl", gsql_client)

        if hasattr(config, "version") and config.version:
            version_str = config.version
            logger.info(f"Using manually configured TigerGraph version: {version_str}")
        else:
            try:
                version_str = gsql_client._get_version()
            except Exception as e:
                logger.warning(
                    f"Failed to detect TigerGraph version: {e}. "
                    f"Defaulting to 4.2.2+ behavior (no /restpp prefix)"
                )
                version_str = None

        if version_str:
            version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_str)
            if version_match:
                major = int(version_match.group(1))
                minor = int(version_match.group(2))
                patch = int(version_match.group(3))
                self.tg_version = f"{major}.{minor}.{patch}"
                self._use_restpp_prefix = True
                logger.info(
                    f"TigerGraph version {self.tg_version} detected, "
                    f"using /restpp prefix for REST API"
                )
            else:
                logger.warning(
                    f"Could not extract version number from '{version_str}'. "
                    f"Defaulting to using /restpp prefix for REST API"
                )
                self._use_restpp_prefix = True

        base_url = f"{config.url_without_port}:{gs_port}"
        self.restpp_url = f"{base_url}/restpp"

        self.api_token: str | None = None
        self._token_cache_key: TokenCacheKey | None = None
        if config.secret:
            secret = config.secret
            self._token_cache_key = _make_token_cache_key(
                self.gsql_url, self.graphname, secret
            )
            try:
                token, cache_hit = _TigerGraphTokenCache.instance().get_or_fetch(
                    self._token_cache_key,
                    lambda: self._get_token_from_secret(secret, self.graphname),
                )
                self.api_token = token
                if cache_hit:
                    logger.debug(
                        "Reused cached API token for graph '%s'",
                        self.graphname,
                    )
                else:
                    logger.info(
                        "Successfully obtained API token for graph '%s'",
                        self.graphname,
                    )
            except Exception as e:
                logger.warning(f"Failed to get authentication token: {e}")
                logger.warning("Falling back to username/password authentication")
                logger.warning(
                    "Note: For best results, provide both username/password AND secret. "
                    "Username/password is used for GSQL operations, secret generates token for REST API."
                )

    @property
    def _auth(self) -> TigerGraphAuth:
        auth = getattr(self, "_auth_impl", None)
        if auth is None:
            auth = TigerGraphAuth(self)
            object.__setattr__(self, "_auth_impl", auth)
        return auth

    @_auth.setter
    def _auth(self, value: TigerGraphAuth) -> None:
        object.__setattr__(self, "_auth_impl", value)

    @property
    def _rest(self) -> TigerGraphRestClient:
        client = getattr(self, "_rest_impl", None)
        if client is None:
            client = TigerGraphRestClient(self)
            object.__setattr__(self, "_rest_impl", client)
        return client

    @property
    def _gsql(self) -> TigerGraphGsqlClient:
        client = getattr(self, "_gsql_impl", None)
        if client is None:
            client = TigerGraphGsqlClient(self)
            object.__setattr__(self, "_gsql_impl", client)
        return client

    @property
    def _ddl(self) -> SchemaDdlBuilder:
        builder = getattr(self, "_ddl_impl", None)
        if builder is None:
            builder = SchemaDdlBuilder(self)
            object.__setattr__(self, "_ddl_impl", builder)
        return builder

    @property
    def _admin(self) -> GraphAdmin:
        admin = getattr(self, "_admin_impl", None)
        if admin is None:
            admin = GraphAdmin(self)
            object.__setattr__(self, "_admin_impl", admin)
        return admin

    @property
    def _data(self) -> TigerGraphDataOps:
        ops = getattr(self, "_data_impl", None)
        if ops is None:
            ops = TigerGraphDataOps(self)
            object.__setattr__(self, "_data_impl", ops)
        return ops

    def _configured_graph_name(self) -> str | None:
        return self.config.database or self.config.schema_name

    def _require_configured_graph_name(self) -> str:
        graph_name = self._configured_graph_name()
        if not graph_name:
            raise ValueError(
                "Graph name must be configured via config.database or config.schema_name"
            )
        return graph_name

    def _get_auth_headers(self, use_basic_auth: bool = False) -> dict[str, str]:
        return self._auth._get_auth_headers(use_basic_auth=use_basic_auth)

    def _get_token_from_secret(
        self, secret: str, graph_name: str | None = None, lifetime: int = 3600 * 24 * 30
    ) -> tuple[str, str | None]:
        return self._auth._get_token_from_secret(secret, graph_name, lifetime=lifetime)

    def _get_version(self) -> str | None:
        return self._gsql._get_version()

    def _execute_gsql(self, gsql_command: str) -> str:
        return self._gsql._execute_gsql(gsql_command)

    def _get_vertex_types(self, graph_name: str | None = None) -> list[str]:
        return self._gsql._get_vertex_types(graph_name)

    def _get_edge_types(
        self, graph_name: str | None = None
    ) -> dict[str, list[tuple[str, str]]]:
        return self._gsql._get_edge_types(graph_name)

    def _get_installed_queries(self, graph_name: str | None = None) -> list[str]:
        return self._gsql._get_installed_queries(graph_name)

    def _drop_installed_queries_for_graph(self, graph_name: str) -> None:
        return self._gsql._drop_installed_queries_for_graph(graph_name)

    def _drop_global_schema_types(
        self, schema: Schema, surviving_graphs: list[str]
    ) -> None:
        return self._gsql._drop_global_schema_types(schema, surviving_graphs)

    def _drop_jobs_for_graph(self, graph_name: str) -> None:
        return self._gsql._drop_jobs_for_graph(graph_name)

    def _run_installed_query(
        self, query_name: str, graph_name: str | None = None, **kwargs: Any
    ) -> dict[str, Any] | list[dict]:
        return self._gsql._run_installed_query(query_name, graph_name, **kwargs)

    def _clear_data_via_installed_query(
        self, graph_name: str, vertex_types: tuple[str, ...]
    ) -> None:
        return self._gsql._clear_data_via_installed_query(graph_name, vertex_types)

    def _call_restpp_api(
        self,
        endpoint: str,
        method: str = "GET",
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        *,
        use_basic_auth: bool = False,
    ) -> dict[str, Any] | list[dict]:
        return self._rest._call_restpp_api(
            endpoint,
            method=method,
            data=data,
            params=params,
            use_basic_auth=use_basic_auth,
        )

    def _upsert_vertex(
        self,
        vertex_type: str,
        vertex_id: str,
        attributes: dict[str, Any],
        graph_name: str | None = None,
    ) -> dict[str, Any] | list[dict]:
        return self._rest._upsert_vertex(vertex_type, vertex_id, attributes, graph_name)

    def _upsert_edge(
        self,
        source_type: str,
        source_id: str,
        edge_type: str,
        target_type: str,
        target_id: str,
        attributes: dict[str, Any] | None = None,
        graph_name: str | None = None,
    ) -> dict[str, Any] | list[dict]:
        return self._rest._upsert_edge(
            source_type,
            source_id,
            edge_type,
            target_type,
            target_id,
            attributes,
            graph_name,
        )

    def _get_edges(
        self,
        source_type: str,
        source_id: str,
        edge_type: str | None = None,
        graph_name: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._rest._get_edges(source_type, source_id, edge_type, graph_name)

    def _get_vertices_by_id(
        self, vertex_type: str, vertex_id: str, graph_name: str | None = None
    ) -> dict[str, dict[str, Any]]:
        return self._rest._get_vertices_by_id(vertex_type, vertex_id, graph_name)

    def _get_vertex_count(self, vertex_type: str, graph_name: str | None = None) -> int:
        return self._rest._get_vertex_count(vertex_type, graph_name)

    def _delete_vertices(
        self, vertex_type: str, where: str | None = None, graph_name: str | None = None
    ) -> dict[str, Any] | list[dict]:
        return self._rest._delete_vertices(vertex_type, where, graph_name)

    def _get_all_graph_names(self) -> list[str]:
        return self._admin._get_all_graph_names()

    def _get_graph_type_names(self, graph_name: str) -> tuple[set[str], set[str]]:
        return self._admin._get_graph_type_names(graph_name)

    def _snapshot_all_queries(self) -> dict[str, list[str]]:
        return self._admin._snapshot_all_queries()

    def _define_schema_local(self, schema: Schema) -> None:
        return self._ddl._define_schema_local(schema)

    def _get_vertex_add_statement(
        self, vertex, vertex_config, *, db_profile=None
    ) -> str:
        return self._ddl._get_vertex_add_statement(
            vertex, vertex_config, db_profile=db_profile
        )

    def _get_edge_add_statement(
        self, edge, *, relation_name, source_vertex, target_vertex, db_profile=None
    ) -> str:
        return self._ddl._get_edge_add_statement(
            edge,
            relation_name=relation_name,
            source_vertex=source_vertex,
            target_vertex=target_vertex,
            db_profile=db_profile,
        )

    def _get_edge_group_create_statement(self, *args, **kwargs) -> str:
        return self._ddl._get_edge_group_create_statement(*args, **kwargs)

    def _gsql_vertex_field_def(self, *args, **kwargs) -> str:
        return self._ddl._gsql_vertex_field_def(*args, **kwargs)

    def _parse_show_graph_output(self, result_str: str) -> list[str]:
        from graflo.db.tigergraph.gsql_parsers import parse_show_graph_output

        return parse_show_graph_output(result_str)

    @staticmethod
    def _clean_document(doc: dict[str, Any]) -> dict[str, Any]:
        return clean_document(doc)

    @staticmethod
    def _extract_id(
        doc: dict[str, Any] | None,
        match_keys: list[str] | tuple[str, ...],
    ) -> str | None:
        return extract_id(doc, match_keys)

    def _validate_tigergraph_vertex_properties(self, vertex) -> None:
        return self._ddl._validate_tigergraph_vertex_properties(vertex)

    def _validate_tigergraph_edge_property_names(self, edge, edge_config_db) -> None:
        return self._ddl._validate_tigergraph_edge_property_names(edge, edge_config_db)

    def _add_index(self, obj_name, index: Index, is_vertex_index=True):
        return self._admin._add_index(obj_name, index, is_vertex_index=is_vertex_index)

    @contextlib.contextmanager
    def _ensure_graph_context(self, graph_name: str | None = None):
        graph_name = graph_name or self._configured_graph_name()
        if not graph_name:
            raise ValueError(
                "Graph name must be provided via graph_name parameter "
                "or config.database/config.schema_name"
            )
        old_graphname = self.graphname
        self.graphname = graph_name
        try:
            yield graph_name
        finally:
            self.graphname = old_graphname

    def graph_exists(self, name: str) -> bool:
        return self._admin.graph_exists(name)

    def create_database(self, name: str):
        return self._admin.create_database(name)

    def delete_database(self, name: str):
        return self._admin.delete_database(name)

    def execute(self, query, **kwargs):
        if isinstance(query, str):
            return self._execute_gsql(query)
        raise TypeError(f"Unsupported query type: {type(query)}")

    def close(self):
        pass

    def bulk_load_begin(
        self, schema: Schema, bulk_cfg: TigergraphBulkLoadConfig
    ) -> str:
        return bulk_load_begin(self, schema, bulk_cfg)

    def bulk_load_append(
        self, session_id: str, gc: GraphContainer, schema: Schema
    ) -> None:
        bulk_load_append(self, session_id, gc, schema)

    def bulk_load_finalize(
        self,
        session_id: str,
        schema: Schema,
        *,
        bindings: Bindings | None = None,
        connection_provider: ConnectionProvider | None = None,
    ) -> str:
        return bulk_load_finalize(
            self,
            session_id,
            schema,
            bindings=bindings,
            connection_provider=connection_provider,
        )

    def init_db(
        self,
        schema: Schema,
        recreate_schema: bool = False,
        *,
        create_namespace: bool = True,
    ) -> None:
        return self._admin.init_db(
            schema, recreate_schema, create_namespace=create_namespace
        )

    def ensure_target_namespace(self, schema: Schema, *, create: bool) -> None:
        return self._admin.ensure_target_namespace(schema, create=create)

    def apply_target_schema(
        self,
        schema: Schema,
        *,
        recreate: bool,
        create_namespace: bool = True,
    ) -> None:
        return self._admin.apply_target_schema(
            schema, recreate=recreate, create_namespace=create_namespace
        )

    def define_schema(self, schema: Schema):
        return self._admin.define_schema(schema)

    def define_vertex_classes(self, schema: Schema) -> None:
        return self._admin.define_vertex_classes(schema.core_schema.vertex_config)

    def define_edge_classes(self, edges: list[Edge]):
        return self._admin.define_edge_classes(edges)

    def define_vertex_indexes(
        self, vertex_config: VertexConfig, schema: Schema | None = None
    ):
        return self._admin.define_vertex_indexes(vertex_config, schema=schema)

    def define_edge_indexes(self, edges: list[Edge], schema: Schema | None = None):
        return self._admin.define_edge_indexes(edges, schema=schema)

    def delete_graph_structure(self, *args, **kwargs):
        return self._admin.delete_graph_structure(*args, **kwargs)

    def clear_data(self, schema: Schema) -> None:
        return self._admin.clear_data(schema)

    def define_indexes(self, schema: Schema):
        return self._admin.define_indexes(schema)

    def fetch_indexes(self, vertex_type: str | None = None):
        return self._admin.fetch_indexes(vertex_type)

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        return self._data.upsert_docs_batch(docs, class_name, match_keys, **kwargs)

    def insert_edges_batch(self, *args, **kwargs):
        return self._data.insert_edges_batch(*args, **kwargs)

    def insert_return_batch(self, docs, class_name):
        return self._data.insert_return_batch(docs, class_name)

    def fetch_docs(self, *args, **kwargs):
        return self._data.fetch_docs(*args, **kwargs)

    def fetch_edges(self, *args, **kwargs):
        return self._data.fetch_edges(*args, **kwargs)

    def fetch_present_documents(self, *args, **kwargs):
        return self._data.fetch_present_documents(*args, **kwargs)

    def aggregate(self, *args, **kwargs):
        return self._data.aggregate(*args, **kwargs)

    def keep_absent_documents(self, *args, **kwargs):
        return self._data.keep_absent_documents(*args, **kwargs)


__all__ = [
    "TigerGraphConnection",
    "TokenCacheKey",
    "_CachedToken",
    "_TigerGraphTokenCache",
    "_load_tigergraph_name_rules",
    "_make_token_cache_key",
    "_parse_tg_expiration",
    "_validate_tigergraph_schema_name",
    "reset_tigergraph_token_cache",
]
