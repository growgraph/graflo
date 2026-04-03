"""Logical graph model (vertices and edges)."""

from __future__ import annotations

from pydantic import Field as PydanticField, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


class CoreSchema(ConfigBaseModel):
    """Logical graph model (A): vertices and edges."""

    vertex_config: VertexConfig = PydanticField(
        ...,
        description="Configuration for vertex collections (vertices, identities, properties).",
    )
    edge_config: EdgeConfig = PydanticField(
        ...,
        description="Configuration for edge collections (edges, weights).",
    )

    @model_validator(mode="after")
    def _init_graph(self) -> CoreSchema:
        self.finish_init()
        return self

    def finish_init(self) -> None:
        self.vertex_config.finish_init()
        self._validate_edge_vertices_defined()
        self.edge_config.finish_init(self.vertex_config)

    def _validate_edge_vertices_defined(self) -> None:
        """Ensure all edge endpoints reference defined vertex names."""
        declared_vertices = self.vertex_config.vertex_set
        edge_vertices = self.edge_config.vertices
        undefined_vertices = edge_vertices - declared_vertices
        if undefined_vertices:
            undefined_vertices_list = sorted(undefined_vertices)
            declared_vertices_list = sorted(declared_vertices)
            raise ValueError(
                "edge_config references undefined vertices: "
                f"{undefined_vertices_list}. "
                f"Declared vertices: {declared_vertices_list}"
            )

    def remove_disconnected_vertices(self) -> set[str]:
        """Remove disconnected vertices and return removed names."""
        connected = self.edge_config.vertices
        disconnected = self.vertex_config.vertex_set - connected
        if disconnected:
            self.vertex_config.remove_vertices(disconnected)
        return disconnected
