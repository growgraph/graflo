"""Data source registry for mapping data sources to resources.

This module provides a registry for mapping data sources to resource names.
Many data sources can map to the same resource, allowing flexible data
ingestion from multiple sources.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from graflo.onto import BaseDataclass

if TYPE_CHECKING:
    from graflo.data_source.base import AbstractDataSource


@dataclasses.dataclass
class DataSourceRegistry(BaseDataclass):
    """Registry for mapping data sources to resource names.

    This class maintains a mapping from resource names to lists of data sources.
    Many data sources can map to the same resource, allowing data to be ingested
    from multiple sources and combined.

    Attributes:
        sources: Dictionary mapping resource names to lists of data sources
    """

    sources: dict[str, list[AbstractDataSource]] = dataclasses.field(
        default_factory=dict
    )

    def register(self, data_source: AbstractDataSource, resource_name: str) -> None:
        """Register a data source for a resource.

        Args:
            data_source: Data source to register
            resource_name: Name of the resource to map to
        """
        if resource_name not in self.sources:
            self.sources[resource_name] = []
        self.sources[resource_name].append(data_source)
        data_source.resource_name = resource_name

    def get_data_sources(self, resource_name: str) -> list[AbstractDataSource]:
        """Get all data sources for a resource.

        Args:
            resource_name: Name of the resource

        Returns:
            List of data sources for the resource (empty list if none found)
        """
        return self.sources.get(resource_name, [])

    def get_all_data_sources(self) -> list[AbstractDataSource]:
        """Get all registered data sources.

        Returns:
            List of all registered data sources
        """
        all_sources = []
        for sources_list in self.sources.values():
            all_sources.extend(sources_list)
        return all_sources

    def has_resource(self, resource_name: str) -> bool:
        """Check if a resource has any data sources.

        Args:
            resource_name: Name of the resource

        Returns:
            True if the resource has data sources, False otherwise
        """
        return resource_name in self.sources and len(self.sources[resource_name]) > 0

    def clear(self) -> None:
        """Clear all registered data sources."""
        self.sources.clear()
