"""Named connectors and resource-to-connector wiring."""

from __future__ import annotations

from typing import Any, Self

from pydantic import Field, PrivateAttr, field_validator, model_validator

from graflo.architecture.base import ConfigBaseModel
from .connectors import (
    FileConnector,
    ResourceConnector,
    SparqlConnector,
    TableConnector,
)


class ResourceConnectorBinding(ConfigBaseModel):
    """Top-level resource -> connector-name mapping entry."""

    resource: str
    connector: str


class ConnectorConnectionBinding(ConfigBaseModel):
    """Connector -> runtime connection-proxy mapping entry.

    This is a non-secret contract block: manifests store proxy names only.
    At runtime, the :class:`~graflo.hq.connection_provider.ConnectionProvider`
    resolves each ``conn_proxy`` to a concrete generalized config holding
    credentials/secrets.
    """

    connector: str
    conn_proxy: str


class StagingProxyBinding(ConfigBaseModel):
    """Named staging profile -> runtime connection-proxy (e.g. S3 credentials).

    Used by TigerGraph bulk ingest to resolve ``S3GeneralizedConnConfig`` without
    putting secrets in the manifest.
    """

    name: str
    conn_proxy: str


class Bindings(ConfigBaseModel):
    """Named resource connectors with explicit resource linkage."""

    connectors: list[FileConnector | TableConnector | SparqlConnector] = Field(
        default_factory=list
    )
    # Accept dict entries at init-time (see validators below).
    # Internally and at runtime, Graflo uses typed lists derived from these.
    resource_connector: list[ResourceConnectorBinding | dict[str, str]] = Field(
        default_factory=list
    )
    # Connector -> runtime endpoint config indirection (proxy by name).
    connector_connection: list[ConnectorConnectionBinding | dict[str, str]] = Field(
        default_factory=list
    )
    _resource_connector_typed: list[ResourceConnectorBinding] = PrivateAttr(
        default_factory=list
    )
    _connector_connection_typed: list[ConnectorConnectionBinding] = PrivateAttr(
        default_factory=list
    )
    _connectors_index: dict[str, ResourceConnector] = PrivateAttr(default_factory=dict)
    _connectors_name_index: dict[str, str] = PrivateAttr(default_factory=dict)
    _resource_to_connector_hashes: dict[str, list[str]] = PrivateAttr(
        default_factory=dict
    )
    _connector_to_conn_proxy: dict[str, str] = PrivateAttr(default_factory=dict)
    staging_proxy: list[StagingProxyBinding | dict[str, str]] = Field(
        default_factory=list,
        description="Optional named staging endpoints (S3) -> conn_proxy wiring.",
    )
    _staging_proxy_typed: list[StagingProxyBinding] = PrivateAttr(default_factory=list)
    _staging_name_to_conn_proxy: dict[str, str] = PrivateAttr(default_factory=dict)

    @property
    def connector_connection_bindings(
        self,
    ) -> list[ConnectorConnectionBinding]:
        # Expose typed entries for downstream components (type-checker friendly).
        return self._connector_connection_typed

    @field_validator("staging_proxy", mode="before")
    @classmethod
    def _coerce_staging_proxy_entries(
        cls, v: Any
    ) -> list[StagingProxyBinding | dict[str, str]]:
        if v is None:
            return []
        if not isinstance(v, list):
            raise ValueError(
                "staging_proxy must be a list of {name, conn_proxy} entries"
            )
        coerced: list[StagingProxyBinding | dict[str, str]] = []
        for i, item in enumerate(v):
            if isinstance(item, StagingProxyBinding):
                coerced.append(item)
                continue
            if isinstance(item, dict):
                missing = [k for k in ("name", "conn_proxy") if k not in item]
                if missing:
                    raise ValueError(
                        f"Invalid staging_proxy entry at index {i}: missing {missing}."
                    )
                coerced.append(StagingProxyBinding.model_validate(item))
                continue
            raise ValueError(
                f"Invalid staging_proxy entry at index {i}: got {type(item).__name__}."
            )
        return coerced

    def _rebuild_staging_proxy_index(self) -> None:
        self._staging_name_to_conn_proxy = {}
        for m in self._staging_proxy_typed:
            existing = self._staging_name_to_conn_proxy.get(m.name)
            if existing is not None and existing != m.conn_proxy:
                raise ValueError(
                    f"Duplicate staging_proxy name '{m.name}' with conflicting conn_proxy."
                )
            self._staging_name_to_conn_proxy[m.name] = m.conn_proxy

    def get_staging_conn_proxy(self, name: str) -> str | None:
        """Return ``conn_proxy`` for a staging profile name, if declared."""
        return self._staging_name_to_conn_proxy.get(name)

    def _rebuild_indexes(self) -> None:
        self._connectors_index = {}
        self._connectors_name_index = {}
        for connector in self.connectors:
            existing = self._connectors_index.get(connector.hash)
            if existing is not None:
                raise ValueError(
                    "Connector hash collision detected for connectors "
                    f"'{type(existing).__name__}' and '{type(connector).__name__}' "
                    f"(hash='{connector.hash}')."
                )
            self._connectors_index[connector.hash] = connector

            if connector.name:
                existing_hash = self._connectors_name_index.get(connector.name)
                if existing_hash is not None and existing_hash != connector.hash:
                    raise ValueError(
                        "Connector names must be unique when provided. "
                        f"Duplicate connector name '{connector.name}'."
                    )
                self._connectors_name_index[connector.name] = connector.hash

    def _append_resource_connector_hash(
        self, resource_name: str, connector_hash: str
    ) -> None:
        """Append *connector_hash* for *resource_name* if not already present (order kept)."""
        bucket = self._resource_to_connector_hashes.setdefault(resource_name, [])
        if connector_hash not in bucket:
            bucket.append(connector_hash)

    @field_validator("resource_connector", mode="before")
    @classmethod
    def _coerce_resource_connector_entries(
        cls, v: Any
    ) -> list[ResourceConnectorBinding]:
        if v is None:
            return []
        if not isinstance(v, list):
            raise ValueError(
                "resource_connector must be a list of {resource, connector} entries"
            )

        coerced: list[ResourceConnectorBinding] = []
        for i, item in enumerate(v):
            if isinstance(item, ResourceConnectorBinding):
                coerced.append(item)
                continue

            if isinstance(item, dict):
                missing = [k for k in ("resource", "connector") if k not in item]
                if missing:
                    raise ValueError(
                        f"Invalid resource_connector entry at index {i}: missing required keys {missing}. "
                        "Expected keys: ['resource', 'connector']."
                    )

                try:
                    coerced.append(ResourceConnectorBinding.model_validate(item))
                except Exception as e:  # noqa: BLE001
                    # Keep the message concise and contextual; nested pydantic
                    # errors can be noisy for config authors.
                    raise ValueError(
                        f"Invalid resource_connector entry at index {i}: {item!r}."
                    ) from e
                continue

            raise ValueError(
                f"Invalid resource_connector entry at index {i}: expected dict or "
                f"ResourceConnectorBinding, got {type(item).__name__}."
            )

        return coerced

    @field_validator("connector_connection", mode="before")
    @classmethod
    def _coerce_connector_connection_entries(
        cls, v: Any
    ) -> list[ConnectorConnectionBinding]:
        if v is None:
            return []
        if not isinstance(v, list):
            raise ValueError(
                "connector_connection must be a list of {connector, conn_proxy} entries"
            )

        coerced: list[ConnectorConnectionBinding] = []
        for i, item in enumerate(v):
            if isinstance(item, ConnectorConnectionBinding):
                coerced.append(item)
                continue

            if isinstance(item, dict):
                missing = [k for k in ("connector", "conn_proxy") if k not in item]
                if missing:
                    raise ValueError(
                        f"Invalid connector_connection entry at index {i}: missing required keys {missing}. "
                        "Expected keys: ['connector', 'conn_proxy']."
                    )
                try:
                    coerced.append(ConnectorConnectionBinding.model_validate(item))
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"Invalid connector_connection entry at index {i}: {item!r}."
                    ) from e
                continue

            raise ValueError(
                f"Invalid connector_connection entry at index {i}: expected dict or "
                f"ConnectorConnectionBinding, got {type(item).__name__}."
            )

        return coerced

    @staticmethod
    def default_connector_name(connector: ResourceConnector) -> str:
        if connector.name:
            return connector.name
        if isinstance(connector, FileConnector):
            return connector.regex or str(connector.sub_path)
        if isinstance(connector, TableConnector):
            return connector.table_name
        if isinstance(connector, SparqlConnector):
            return connector.rdf_class
        raise TypeError(f"Unsupported connector type: {type(connector)!r}")

    @model_validator(mode="after")
    def _populate_resource_connector(self) -> Self:
        self._rebuild_indexes()
        self._resource_to_connector_hashes = {}

        # Create typed views so internal code never has to handle dicts.
        self._resource_connector_typed = [
            ResourceConnectorBinding.model_validate(m) if isinstance(m, dict) else m
            for m in self.resource_connector
        ]
        self._connector_connection_typed = [
            ConnectorConnectionBinding.model_validate(m) if isinstance(m, dict) else m
            for m in self.connector_connection
        ]
        self._staging_proxy_typed = [
            StagingProxyBinding.model_validate(m) if isinstance(m, dict) else m
            for m in self.staging_proxy
        ]
        self._rebuild_staging_proxy_index()

        for connector in self.connectors:
            if connector.resource_name is None:
                continue
            self._append_resource_connector_hash(
                connector.resource_name, connector.hash
            )

        for mapping in self._resource_connector_typed:
            connector_hash = self._connectors_name_index.get(mapping.connector)
            if connector_hash is None:
                if mapping.connector in self._connectors_index:
                    connector_hash = mapping.connector
                else:
                    raise ValueError(
                        f"resource_connector references unknown connector '{mapping.connector}' "
                        f"for resource '{mapping.resource}'."
                    )
            self._append_resource_connector_hash(mapping.resource, connector_hash)
        self._rebuild_connector_to_conn_proxy()
        return self

    def _resolve_connector_ref_to_hash(self, connector_ref: str) -> str:
        """Resolve a connector reference to its canonical connector hash.

        Allowed references:
        - ``connector.hash`` (canonical internal id), or
        - ``connector.name`` (when a name is provided / auto-filled).

        Ingestion resource names are not valid connector references (a resource
        may map to multiple connectors).
        """
        if connector_ref in self._connectors_index:
            return connector_ref
        resolved_hash = self._connectors_name_index.get(connector_ref)
        if resolved_hash is None:
            raise ValueError(f"Unknown connector reference '{connector_ref}'")
        return resolved_hash

    def _rebuild_connector_to_conn_proxy(self) -> None:
        self._connector_to_conn_proxy = {}
        for mapping in self._connector_connection_typed:
            connector_hash = self._resolve_connector_ref_to_hash(mapping.connector)
            existing = self._connector_to_conn_proxy.get(connector_hash)
            if existing is not None and existing != mapping.conn_proxy:
                raise ValueError(
                    "Conflicting conn_proxy mapping for connector "
                    f"'{connector_hash}' (existing='{existing}', new='{mapping.conn_proxy}')."
                )
            self._connector_to_conn_proxy[connector_hash] = mapping.conn_proxy

    def get_conn_proxy_for_connector(
        self, connector: TableConnector | FileConnector | SparqlConnector
    ) -> str | None:
        """Return the mapped runtime proxy name for a given connector."""
        return self._connector_to_conn_proxy.get(connector.hash)

    def bind_connector_to_conn_proxy(
        self,
        connector: TableConnector | FileConnector | SparqlConnector,
        conn_proxy: str,
    ) -> None:
        """Bind a connector to a non-secret runtime proxy name.

        Uses ``connector.name`` when available, falling back to ``connector.hash``.
        """
        # Ensure indexes include the connector and that a default name is set.
        if connector.hash not in self._connectors_index:
            self.add_connector(connector)
        # Pick a contract reference string that's stable and user-friendly.
        connector_ref = connector.name or connector.hash

        # Ensure uniqueness by connector.hash (not by ref-string).
        connector_hash = connector.hash
        existing_idx: int | None = None
        for i, m in enumerate(self._connector_connection_typed):
            try:
                if self._resolve_connector_ref_to_hash(m.connector) == connector_hash:
                    existing_idx = i
                    break
            except ValueError:
                continue

        if existing_idx is None:
            self._connector_connection_typed.append(
                ConnectorConnectionBinding(
                    connector=connector_ref, conn_proxy=conn_proxy
                )
            )
        else:
            self._connector_connection_typed[existing_idx] = ConnectorConnectionBinding(
                connector=connector_ref, conn_proxy=conn_proxy
            )
        # Keep the public contract field in sync for serialization / downstream.
        self.connector_connection = list(self._connector_connection_typed)

        self._rebuild_connector_to_conn_proxy()

    @classmethod
    def from_dict(cls, data: dict[str, Any] | list[Any]) -> Self:
        if isinstance(data, list):
            raise ValueError(
                "Bindings.from_dict expects a mapping with 'connectors' and optional "
                "'resource_connector'. List-style connector payloads are not supported."
            )
        legacy_keys = {
            "postgres_connections",
            "table_connectors",
            "file_connectors",
            "sparql_connectors",
        }
        found_legacy = sorted(k for k in legacy_keys if k in data)
        if found_legacy:
            raise ValueError(
                "Legacy Bindings init keys are not supported. "
                f"Unsupported keys: {', '.join(found_legacy)}."
            )
        return cls.model_validate(data)

    def add_connector(
        self,
        connector: TableConnector | FileConnector | SparqlConnector,
    ) -> None:
        if connector.name is None:
            object.__setattr__(
                connector, "name", self.default_connector_name(connector)
            )
        existing_name_hash = None
        if connector.name:
            existing_name_hash = self._connectors_name_index.get(connector.name)
        if (
            connector.name
            and existing_name_hash is not None
            and existing_name_hash != connector.hash
        ):
            raise ValueError(
                "Connector names must be unique when provided. "
                f"Duplicate connector name '{connector.name}'."
            )

        if connector.hash in self._connectors_index:
            old_connector = self._connectors_index[connector.hash]
            for idx, existing in enumerate(self.connectors):
                if existing is old_connector:
                    self.connectors[idx] = connector
                    break
        else:
            self.connectors.append(connector)
        self._rebuild_indexes()
        if connector.resource_name is not None:
            self._append_resource_connector_hash(
                connector.resource_name, connector.hash
            )

    def bind_resource(
        self,
        resource_name: str,
        connector: TableConnector | FileConnector | SparqlConnector,
    ) -> None:
        if connector.hash not in self._connectors_index:
            raise KeyError(f"Connector not found for hash='{connector.hash}'")
        self._append_resource_connector_hash(resource_name, connector.hash)
        connector_name = connector.name or self.default_connector_name(connector)
        self._resource_connector_typed.append(
            ResourceConnectorBinding(
                resource=resource_name,
                connector=connector_name,
            )
        )
        # Keep the public contract field in sync for serialization / downstream.
        self.resource_connector = list(self._resource_connector_typed)

    def get_connectors_for_resource(
        self, resource_name: str
    ) -> list[TableConnector | FileConnector | SparqlConnector]:
        """Return connectors bound to *resource_name*, in binding order (unique by hash)."""
        result: list[TableConnector | FileConnector | SparqlConnector] = []
        for h in self._resource_to_connector_hashes.get(resource_name, []):
            c = self._connectors_index.get(h)
            if isinstance(c, (TableConnector, FileConnector, SparqlConnector)):
                result.append(c)
        return result
