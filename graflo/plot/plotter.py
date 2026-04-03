"""Graph visualization utilities for schema and data structures.

This module provides utilities for visualizing graph database schemas, relationships,
and data structures using NetworkX and Graphviz. It includes functionality for
plotting vertex collections, resources, and their relationships.

Key Components:
    - SchemaPlotter: Main class for schema visualization
    - AuxNodeType: Enum for different node types in visualizations
    - Color and shape mappings for different node types
    - Tree assembly and graph generation utilities

Example:
    >>> plotter = ManifestPlotter("config.json", "output/")
    >>> plotter.plot_vc2fields()  # Plot vertex collections and their fields
    >>> plotter.plot_resources()  # Plot resource relationships
    >>> plotter.plot_vc2vc()      # Plot vertex collection relationships
"""

import logging
import os
from itertools import product
from pathlib import Path

import networkx as nx
from suthing import FileHandle

from graflo.architecture import GraphManifest
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import Edge
from graflo.architecture.pipeline.runtime.actor import (
    ActorWrapper,
    DescendActor,
    EdgeActor,
    EdgeRouterActor,
    TransformActor,
    VertexActor,
    VertexRouterActor,
)
from graflo.onto import BaseEnum

logger = logging.getLogger(__name__)

partition_color_palette = [
    "#BEDFC8",
    "#B7D1DF",
    "#DDD0E5",
    "#FFE5B4",
    "#EBA59E",
    "#E4E1D7",
    "#C7CEEA",
    "#D5E8D4",
]


class AuxNodeType(BaseEnum):
    """Node types for graph visualization.

    This enum defines the different types of nodes that can appear in the
    visualization graphs, each with specific visual properties.

    Attributes:
        FIELD: Regular field node
        FIELD_DEFINITION: Field definition node
        INDEX: Index field node
        RESOURCE: Resource node
        TRANSFORM: Transform node
        VERTEX: Vertex node
        VERTEX_BLANK: Empty vertex node
    """

    FIELD = "field"
    FIELD_DEFINITION = "field_definition"
    INDEX = "field"
    RESOURCE = "resource"
    TRANSFORM = "transform"
    VERTEX = "vertex"
    VERTEX_BLANK = "vertex_blank"


# Color palette for node fill colors
fillcolor_palette = {
    "violet": "#DDD0E5",
    "green": "#BEDFC8",
    "blue": "#B7D1DF",
    "red": "#EBA59E",
    "peach": "#FFE5B4",
}

# Mapping of node types to shapes
NODE_SHAPE_BY_TYPE = {
    AuxNodeType.RESOURCE: "box",
    AuxNodeType.VERTEX_BLANK: "box",
    AuxNodeType.FIELD_DEFINITION: "trapezium",
    AuxNodeType.TRANSFORM: "oval",
    AuxNodeType.VERTEX: "ellipse",
    AuxNodeType.INDEX: "polygon",
    AuxNodeType.FIELD: "octagon",
}

# Mapping of node types to colors
NODE_COLOR_BY_TYPE = {
    AuxNodeType.RESOURCE: fillcolor_palette["blue"],
    AuxNodeType.FIELD_DEFINITION: fillcolor_palette["red"],
    AuxNodeType.VERTEX_BLANK: "white",
    AuxNodeType.VERTEX: fillcolor_palette["green"],
    AuxNodeType.INDEX: "orange",
    AuxNodeType.TRANSFORM: "grey",
    AuxNodeType.FIELD: fillcolor_palette["violet"],
}

# Mapping of actor classes to colors
map_class2color = {
    DescendActor: fillcolor_palette["green"],
    VertexActor: "orange",
    EdgeActor: fillcolor_palette["violet"],
    TransformActor: fillcolor_palette["blue"],
}

# Edge style mapping
EDGE_STYLE_BY_SOURCE_TYPE = {AuxNodeType.VERTEX: "solid"}

# Backward-compatible aliases.
map_type2shape = NODE_SHAPE_BY_TYPE
map_type2color = NODE_COLOR_BY_TYPE
edge_status = EDGE_STYLE_BY_SOURCE_TYPE


def get_auxnode_id(ntype: AuxNodeType, label=False, vfield=False, **kwargs):
    """Generate a unique identifier for an auxiliary node.

    Args:
        ntype: Type of the auxiliary node
        label: Whether to generate a label instead of an ID
        vfield: Whether this is a vertex field
        **kwargs: Additional parameters for node identification

    Returns:
        str: Node identifier or label

    Example:
        >>> get_auxnode_id(AuxNodeType.VERTEX, vertex="user", label=True)
        'user'
    """
    vertex = kwargs.pop("vertex", None)
    resource = kwargs.pop("resource", None)
    vertex_shortcut = kwargs.pop("vertex_sh", None)
    resource_shortcut = kwargs.pop("resource_sh", None)
    s = "***"
    if ntype == AuxNodeType.RESOURCE:
        resource_type = kwargs.pop("resource_type")
        if label:
            s = f"{resource}"
        else:
            s = f"{ntype}:{resource_type}:{resource}"
    elif ntype == AuxNodeType.VERTEX:
        if label:
            s = f"{vertex}"
        else:
            s = f"{ntype}:{vertex}"
    elif ntype == AuxNodeType.FIELD:
        field = kwargs.pop("field", None)
        if vfield:
            if label:
                s = f"({vertex_shortcut[vertex]}){field}"
            else:
                s = f"{ntype}:{vertex}:{field}"
        else:
            if label:
                s = f"<{resource_shortcut[resource]}>{field}"
            else:
                s = f"{ntype}:{resource}:{field}"
    elif ntype == AuxNodeType.TRANSFORM:
        inputs = kwargs.pop("inputs")
        outputs = kwargs.pop("outputs")
        t_spec = inputs + outputs
        t_key = "-".join(t_spec)
        t_label = "-".join([x[0] for x in t_spec])

        if label:
            s = f"[t]{t_label}"
        else:
            s = f"transform:{t_key}"
    return s


def shortest_unique_prefix_map(strings):
    """Create a dictionary of string prefixes for shortening labels.

    Args:
        strings: List of strings to process

    Returns:
        dict: Mapping of shortened prefixes to original prefixes

    Example:
        >>> lto_dict(["user", "user_profile", "user_settings"])
        {'user': 'user', 'user_p': 'user_', 'user_s': 'user_'}
    """
    strings = list(set(strings))
    d = {"": strings}
    while any([len(v) > 1 for v in d.values()]):
        keys = list(d.keys())
        for k in keys:
            item = d.pop(k)
            if len(item) < 2:
                d[k] = item
            else:
                for s in item:
                    if s:
                        if k + s[0] in d:
                            d[k + s[0]].append(s[1:])
                        else:
                            d[k + s[0]] = [s[1:]]
                    else:
                        d[k] = [s]
    r = {}
    for k, v in d.items():
        if v:
            r[k + v[0]] = k
        else:
            r[k] = k
    return r


def lto_dict(strings):
    """Backward-compatible alias for shortest_unique_prefix_map."""
    return shortest_unique_prefix_map(strings)


def assemble_tree(
    aw: ActorWrapper,
    fig_path: Path | str | None = None,
    output_format: str = "pdf",
    output_dpi: int | None = None,
):
    """Assemble a tree visualization from an actor wrapper.

    Args:
        aw: Actor wrapper containing the tree structure
        fig_path: Optional path to save the visualization

    Returns:
        nx.MultiDiGraph | None: The assembled graph if fig_path is None

    Example:
        >>> graph = assemble_tree(actor_wrapper)
        >>> assemble_tree(actor_wrapper, "output/tree.pdf")
    """
    _, _, _, edges = aw.fetch_actors(0, [])
    logger.info(f"{len(edges)}")
    nodes = {}
    g = nx.MultiDiGraph()
    for ha, hb, pa, pb in edges:
        nodes[ha] = pa
        nodes[hb] = pb

    for n, props in nodes.items():
        nodes[n]["fillcolor"] = map_class2color[props["class"]]
        nodes[n]["style"] = "filled"
        nodes[n]["color"] = "brown"

    edges = [(ha, hb) for ha, hb, _, _ in edges]
    g.add_edges_from(edges)
    g.add_nodes_from(nodes.items())

    if fig_path is not None:
        ag = nx.nx_agraph.to_agraph(g)
        if output_format == "png" and output_dpi is not None:
            ag.graph_attr["dpi"] = str(output_dpi)
        ag.draw(
            fig_path,
            output_format,
            prog="dot",
        )
        return None
    else:
        return g


class ManifestPlotter:
    """Main class for schema visualization.

    This class provides methods to visualize different aspects of a graph database
    schema, including vertex collections, resources, and their relationships.

    Attributes:
        fig_path: Path to save visualizations
        config: Schema configuration
        schema: Schema instance
        name: Schema name
        prefix: Prefix for output files
    """

    def __init__(
        self,
        config_filename: str | os.PathLike[str] | None = None,
        fig_path: str | os.PathLike[str] = ".",
        output_format: str = "pdf",
        output_dpi: int | None = None,
        graph_manifest: GraphManifest | None = None,
    ):
        """Initialize the schema plotter.

        Args:
            config_filename: Optional path to schema configuration file
            fig_path: Path to save visualizations
            graph_manifest: Optional pre-built graph manifest
        """
        self.fig_path = fig_path
        self.output_format = output_format.lower()
        self.output_dpi = output_dpi

        manifest: GraphManifest
        if graph_manifest is not None:
            manifest = graph_manifest
            self.config = None
        elif config_filename is not None:
            self.config = FileHandle.load(fpath=config_filename)
            manifest = GraphManifest.from_config(self.config)
        else:
            raise ValueError(
                "ManifestPlotter requires either `config_filename` or `graph_manifest`."
            )

        manifest.finish_init()
        self.schema = manifest.require_schema()
        self.ingestion_model = manifest.require_ingestion_model()

        self.name = self.schema.metadata.name
        self.prefix = self.name

    def _figure_path(self, stem: str) -> str:
        return os.path.join(self.fig_path, f"{stem}.{self.output_format}")

    def _versioned_stem(self, stem: str) -> str:
        metadata = getattr(self.schema, "metadata", None)
        version = getattr(metadata, "version", None)
        if version is None:
            return stem
        return f"{stem}-v{version}"

    def _draw(self, ag, stem: str, prog: str = "dot") -> None:
        if self.output_format == "png" and self.output_dpi is not None:
            ag.graph_attr["dpi"] = str(self.output_dpi)
        ag.draw(
            self._figure_path(stem),
            self.output_format,
            prog=prog,
        )

    def _discover_edges_from_resources(
        self,
    ) -> tuple[dict[EdgeId, Edge], dict[EdgeId, str], dict[EdgeId, bool]]:
        """Discover edges from resources by walking through ActorWrappers.

        This method finds all EdgeActors in resources and extracts their edges,
        which may include edges with dynamic relations (EdgeActor derivation) that
        aren't fully represented in edge_config.

        Returns:
            discovered_edges: map edge_id → Edge
            relation_source_by_edge_id: document field for per-row relation (plot hint).
            relation_from_key_by_edge_id: True when an edge step derives relation from keys.
        """
        discovered_edges: dict[EdgeId, Edge] = {}
        relation_source_by_edge_id: dict[EdgeId, str] = {}
        relation_from_key_by_edge_id: dict[EdgeId, bool] = {}

        for resource in self.ingestion_model.resources:
            # Collect all actors from the resource's ActorWrapper
            actors = resource.root.collect_actors()

            for actor in actors:
                if isinstance(actor, EdgeActor):
                    edge = actor.edge
                    edge_id = edge.edge_id
                    # Store the edge, preferring already discovered edges from edge_config
                    # but allowing resource edges to supplement
                    if edge_id not in discovered_edges:
                        discovered_edges[edge_id] = edge
                    if actor.relation_field is not None:
                        relation_source_by_edge_id[edge_id] = actor.relation_field
                    if actor.derivation.relation_from_key:
                        relation_from_key_by_edge_id[edge_id] = True

        return (
            discovered_edges,
            relation_source_by_edge_id,
            relation_from_key_by_edge_id,
        )

    @staticmethod
    def _edge_label(
        edge: Edge,
        *,
        relation_source_field: str | None = None,
        relation_from_key: bool = False,
    ) -> str | None:
        """Build the human-readable edge label for plotting."""
        if edge.relation is not None:
            return edge.relation
        if relation_source_field is not None:
            return f"[{relation_source_field}]"
        if relation_from_key:
            return "[key]"
        return None

    @staticmethod
    def _merge_edges(primary_edges: dict, supplemental_edges: dict) -> dict:
        """Merge edge dictionaries without replacing existing primary edges."""
        merged = dict(primary_edges)
        for edge_id, edge in supplemental_edges.items():
            merged.setdefault(edge_id, edge)
        return merged

    @staticmethod
    def _filter_edges_with_known_vertices(
        edges: dict,
        known_vertices: set[str],
    ) -> tuple[dict, dict]:
        """Split edges by whether both endpoints are known vertices."""
        valid_edges = {}
        invalid_edges = {}
        for edge_id, edge in edges.items():
            source, target, _ = edge_id
            if source in known_vertices and target in known_vertices:
                valid_edges[edge_id] = edge
            else:
                invalid_edges[edge_id] = edge
        return valid_edges, invalid_edges

    @staticmethod
    def _style_nodes(
        graph: nx.MultiDiGraph | nx.DiGraph,
        *,
        force_labels: bool = False,
        partition_by_vertex: dict[str, str] | None = None,
        partition_color_map: dict[str, str] | None = None,
    ) -> None:
        """Apply common visual styles to graph nodes."""
        for node_id in graph.nodes():
            props = graph.nodes()[node_id]
            node_type = props.get("type")
            if node_type is None:
                logger.warning(
                    "Skipping node style; missing 'type' for node %s", node_id
                )
                continue

            shape = NODE_SHAPE_BY_TYPE.get(node_type)
            color = NODE_COLOR_BY_TYPE.get(node_type)
            if shape is None or color is None:
                logger.warning(
                    "Skipping node style; unsupported node type %s for node %s",
                    node_type,
                    node_id,
                )
                continue

            updates: dict[str, object] = {
                "shape": shape,
                "color": color,
                "style": "filled",
            }
            if force_labels and "label" in props:
                updates["forcelabel"] = True

            if partition_by_vertex and partition_color_map:
                if isinstance(node_id, str) and node_id.startswith(
                    f"{AuxNodeType.VERTEX}:"
                ):
                    vertex_name = node_id.split(":", maxsplit=1)[1]
                    group = partition_by_vertex.get(vertex_name)
                    if group is not None and group in partition_color_map:
                        updates["fillcolor"] = partition_color_map[group]

            for key, value in updates.items():
                graph.nodes[node_id][key] = value

    @staticmethod
    def _style_edges(
        graph: nx.MultiDiGraph | nx.DiGraph,
        *,
        default_style: str = "solid",
        source_type_style_map: dict[AuxNodeType, str] | None = None,
    ) -> None:
        """Apply common visual styles to graph edges."""
        if graph.is_multigraph():
            edge_iterator = graph.edges(keys=True)
        else:
            edge_iterator = ((source, target, None) for source, target in graph.edges())

        for source, target, key in edge_iterator:
            if graph.is_multigraph():
                edge_data = graph.edges[source, target, key]
            else:
                edge_data = graph.edges[source, target]
            updates: dict[str, object] = {
                "arrowhead": "vee",
                "style": default_style,
            }
            if source_type_style_map is not None:
                source_props = graph.nodes[source]
                source_type = source_props.get("type")
                if source_type in source_type_style_map:
                    updates["style"] = source_type_style_map[source_type]
            if "label" in edge_data:
                updates["label"] = edge_data["label"]
            for key_name, value in updates.items():
                if graph.is_multigraph():
                    graph.edges[source, target, key][key_name] = value
                else:
                    graph.edges[source, target][key_name] = value

    @staticmethod
    def _add_partition_subgraphs(
        ag,
        graph: nx.MultiDiGraph | nx.DiGraph,
        partition_by_vertex: dict[str, str],
    ) -> None:
        """Add Graphviz subgraphs grouping vertices by partition labels."""
        partition_to_nodes: dict[str, list[str]] = {}
        for node_id in graph.nodes():
            if not isinstance(node_id, str) or not node_id.startswith(
                f"{AuxNodeType.VERTEX}:"
            ):
                continue
            vertex_name = node_id.split(":", maxsplit=1)[1]
            group = partition_by_vertex.get(vertex_name)
            if group is None:
                continue
            partition_to_nodes.setdefault(group, []).append(node_id)

        for group in sorted(partition_to_nodes):
            ag.add_subgraph(
                partition_to_nodes[group],
                name=f"cluster_partition_{group}",
                rank="same",
                label=str(group),
            )

    def _add_resource_vertex_links(
        self,
        graph: nx.MultiDiGraph,
        resource,
    ) -> None:
        """Add a resource node and its vertex links to a graph."""
        resource_id = get_auxnode_id(
            AuxNodeType.RESOURCE,
            resource=resource.name,
            resource_type="resource",
        )
        graph.add_node(
            resource_id,
            type=AuxNodeType.RESOURCE,
            label=resource.name,
        )

        vertex_reasons = self._extract_resource_vertex_reasons(resource)
        for vertex_name, reasons in sorted(vertex_reasons.items()):
            vertex_id = get_auxnode_id(AuxNodeType.VERTEX, vertex=vertex_name)
            graph.add_node(
                vertex_id,
                type=AuxNodeType.VERTEX,
                label=vertex_name,
            )
            graph.add_edge(resource_id, vertex_id, label=", ".join(sorted(reasons)))

    @staticmethod
    def _node_id_to_vertex(node_id: str) -> str:
        """Extract vertex name from an AuxNodeType.VERTEX node ID."""
        return node_id.split(":", maxsplit=1)[1]

    def _build_partition_color_map(
        self,
        partition_values: set[str],
        partition_colors: dict[str, str] | None,
    ) -> dict[str, str]:
        """Build a deterministic color map for partition labels."""
        color_map = {}
        if partition_colors is not None:
            color_map.update(partition_colors)

        auto_keys = sorted(partition_values - set(color_map.keys()))
        for idx, key in enumerate(auto_keys):
            color_map[key] = partition_color_palette[idx % len(partition_color_palette)]
        return color_map

    def _infer_vertex_levels(self, edges: list[tuple[str, str]]) -> dict[str, int]:
        """Infer robust DAG-like levels by collapsing SCCs then layering the DAG."""
        graph = nx.DiGraph()
        graph.add_nodes_from(self.schema.core_schema.vertex_config.vertex_set)
        graph.add_edges_from(edges)

        condensation = nx.condensation(graph)
        component_level: dict[int, int] = {}
        for component in nx.topological_sort(condensation):
            predecessors = list(condensation.predecessors(component))
            if not predecessors:
                component_level[component] = 0
                continue
            component_level[component] = (
                max(component_level[pred] for pred in predecessors) + 1
            )

        levels = {}
        member_map = condensation.graph["mapping"]
        for vertex in graph.nodes:
            levels[vertex] = component_level[member_map[vertex]]
        return levels

    def plot_vc2fields(self):
        """Plot vertex collections and their fields.

        Creates a visualization showing the relationship between vertex collections
        and their fields, including index fields. The visualization is saved as
        a PDF file.
        """
        g = nx.DiGraph()
        nodes = []
        edges = []
        vconf = self.schema.core_schema.vertex_config
        vertex_prefix_dict = shortest_unique_prefix_map(
            [v for v in self.schema.core_schema.vertex_config.vertex_set]
        )

        kwargs = {"vfield": True, "vertex_sh": vertex_prefix_dict}
        for k in vconf.vertex_set:
            index_fields = vconf.identity_fields(k)
            fields = vconf.fields_names(k)
            kwargs["vertex"] = k
            nodes_collection = [
                (
                    get_auxnode_id(AuxNodeType.VERTEX, **kwargs),
                    {
                        "type": AuxNodeType.VERTEX,
                        "label": get_auxnode_id(
                            AuxNodeType.VERTEX, label=True, **kwargs
                        ),
                    },
                )
            ]
            nodes_fields = [
                (
                    get_auxnode_id(AuxNodeType.FIELD, field=item, **kwargs),
                    {
                        "type": (
                            AuxNodeType.FIELD_DEFINITION
                            if item in index_fields
                            else AuxNodeType.FIELD
                        ),
                        "label": get_auxnode_id(
                            AuxNodeType.FIELD, field=item, label=True, **kwargs
                        ),
                    },
                )
                for item in fields
            ]
            nodes += nodes_collection
            nodes += nodes_fields
            edges += [(x[0], y[0]) for x, y in product(nodes_collection, nodes_fields)]

        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        self._style_nodes(g, force_labels=True)
        self._style_edges(g)

        ag = nx.nx_agraph.to_agraph(g)

        for k in vconf.vertex_set:
            level_index = [
                get_auxnode_id(
                    AuxNodeType.FIELD,
                    vertex=k,
                    field=item,
                    vfield=True,
                    vertex_sh=vertex_prefix_dict,
                )
                for item in vconf.identity_fields(k)
            ]
            index_subgraph = ag.add_subgraph(level_index, name=f"cluster_{k}:def")
            index_subgraph.node_attr["style"] = "filled"
            index_subgraph.node_attr["label"] = "definition"

        ag = ag.unflatten("-l 5 -f -c 3")
        self._draw(ag, self._versioned_stem(f"{self.prefix}_vc2fields"))

    def plot_resources(self):
        """Plot resource relationships.

        Creates visualizations for each resource in the schema, showing their
        internal structure and relationships. Each resource is saved as a
        separate PDF file.
        """
        resource_prefix_dict = shortest_unique_prefix_map(
            [resource.name for resource in self.ingestion_model.resources]
        )
        vertex_prefix_dict = shortest_unique_prefix_map(
            [v for v in self.schema.core_schema.vertex_config.vertex_set]
        )
        kwargs = {"vertex_sh": vertex_prefix_dict, "resource_sh": resource_prefix_dict}

        for resource in self.ingestion_model.resources:
            kwargs["resource"] = resource.name
            assemble_tree(
                resource.root,
                self._figure_path(
                    f"{self.schema.metadata.name}.resource-{resource.name}"
                ),
                output_format=self.output_format,
                output_dpi=self.output_dpi,
            )

    def plot_source2vc(self):
        """Plot source to vertex collection mappings.

        Creates a visualization showing the relationship between source resources
        and vertex collections. The visualization is saved as a PDF file.
        """
        g = nx.MultiDiGraph()
        for resource in self.ingestion_model.resources:
            self._add_resource_vertex_links(g, resource)

        self._style_nodes(g, force_labels=True)
        self._style_edges(g)

        ag = nx.nx_agraph.to_agraph(g)
        ag.graph_attr["rankdir"] = "LR"
        ag.graph_attr["splines"] = "spline"

        resource_nodes = [
            get_auxnode_id(
                AuxNodeType.RESOURCE,
                resource=resource.name,
                resource_type="resource",
            )
            for resource in self.ingestion_model.resources
        ]
        vertex_nodes = [
            get_auxnode_id(AuxNodeType.VERTEX, vertex=v)
            for v in sorted(self.schema.core_schema.vertex_config.vertex_set)
            if get_auxnode_id(AuxNodeType.VERTEX, vertex=v) in g.nodes
        ]
        if resource_nodes:
            ag.add_subgraph(resource_nodes, name="cluster_resources", rank="same")
        if vertex_nodes:
            ag.add_subgraph(vertex_nodes, name="cluster_vertices", rank="same")

        self._draw(ag, f"{self.prefix}_source2vc")

    def _extract_resource_vertex_reasons(self, resource) -> dict[str, set[str]]:
        """Collect vertex references for a resource with lightweight reason labels."""
        vertex_reasons: dict[str, set[str]] = {}
        known_vertices = set(self.schema.core_schema.vertex_config.vertex_set)
        actors = resource.root.collect_actors()

        def _add(vertex_name: str, reason: str) -> None:
            if vertex_name not in known_vertices:
                return
            if vertex_name not in vertex_reasons:
                vertex_reasons[vertex_name] = set()
            vertex_reasons[vertex_name].add(reason)

        for actor in actors:
            for vertex_name in actor.references_vertices():
                _add(vertex_name, type(actor).__name__)

            if isinstance(actor, VertexRouterActor):
                for vertex_name in actor.type_map.values():
                    _add(vertex_name, "VertexRouterActor(type_map)")
                for vertex_name in actor.vertex_from_map:
                    _add(vertex_name, "VertexRouterActor(vertex_from_map)")

            if isinstance(actor, EdgeRouterActor):
                for vertex_name in actor._source_type_map.values():
                    _add(vertex_name, "EdgeRouterActor(source_type_map)")
                for vertex_name in actor._target_type_map.values():
                    _add(vertex_name, "EdgeRouterActor(target_type_map)")

        return vertex_reasons

    def plot_source2vc_detailed(self):
        """Plot per-resource source-to-vertex mappings as dedicated detail pages."""
        for resource in self.ingestion_model.resources:
            g = nx.MultiDiGraph()
            self._add_resource_vertex_links(g, resource)

            self._style_nodes(g, force_labels=True)
            self._style_edges(g)

            ag = nx.nx_agraph.to_agraph(g)
            ag.graph_attr["rankdir"] = "LR"
            self._draw(ag, f"{self.schema.metadata.name}.resource2vc-{resource.name}")

    def plot_vc2vc(
        self,
        prune_leaves: bool = False,
        partition: dict[str, str] | None = None,
        partition_colors: dict[str, str] | None = None,
        color_by_partition: bool = False,
        group_by_partition: bool = False,
        include_all_vertices: bool = True,
        group_by_inferred_level: bool = False,
    ):
        """Plot vertex collection relationships.

        Creates a visualization showing the relationships between vertex collections.
        Optionally prunes leaf nodes from the visualization.

        This method discovers edges from both edge_config and resources to ensure
        all relationships are visualized, including those with dynamic relations.

        Args:
            prune_leaves: Whether to remove leaf nodes from the visualization
            partition: Optional vertex->group mapping used for coloring/grouping
            partition_colors: Optional group->color mapping for partition values
            color_by_partition: Whether to color nodes using partition values
            group_by_partition: Whether to add Graphviz clusters for each group
            include_all_vertices: Whether to include vertices with no known edges
            group_by_inferred_level: Group by inferred graph level (SCC-aware)

        Example:
            >>> plotter.plot_vc2vc(prune_leaves=True)
        """
        g = nx.MultiDiGraph()
        nodes: list[tuple[str, dict[str, object]]] = []
        rendered_edges: list[tuple] = []

        (
            discovered_edges,
            relation_source_by_edge_id,
            relation_from_key_by_edge_id,
        ) = self._discover_edges_from_resources()
        configured_edges = dict(self.schema.core_schema.edge_config.items())
        all_edges = self._merge_edges(configured_edges, discovered_edges)

        known_vertices = set(self.schema.core_schema.vertex_config.vertex_set)
        valid_edges, invalid_edges = self._filter_edges_with_known_vertices(
            all_edges,
            known_vertices,
        )
        if invalid_edges:
            sampled_invalid = sorted(invalid_edges)[:5]
            logger.error(
                "plot_vc2vc ignored %s edge(s) with unknown vertices not in vertex_config; sample=%s",
                len(invalid_edges),
                sampled_invalid,
            )

        edge_pairs = [(source, target) for (source, target, _relation) in valid_edges]
        for edge_id, edge in valid_edges.items():
            label = self._edge_label(
                edge,
                relation_source_field=relation_source_by_edge_id.get(edge_id),
                relation_from_key=relation_from_key_by_edge_id.get(edge_id, False),
            )
            source, target = edge_id[0], edge_id[1]
            source_id = get_auxnode_id(AuxNodeType.VERTEX, vertex=source)
            target_id = get_auxnode_id(AuxNodeType.VERTEX, vertex=target)
            if label is None:
                rendered_edges.append((source_id, target_id))
            else:
                rendered_edges.append((source_id, target_id, {"label": label}))

        # Create nodes for vertices involved in edges, optionally including all schema vertices
        vertices_in_edges = {
            vertex
            for (source, target, _relation) in valid_edges
            for vertex in (source, target)
        }
        all_vertices = (
            set(self.schema.core_schema.vertex_config.vertex_set)
            if include_all_vertices
            else vertices_in_edges
        )

        effective_partition = partition.copy() if partition is not None else {}
        if group_by_inferred_level:
            inferred_levels = self._infer_vertex_levels(edge_pairs)
            effective_partition.update(
                {vertex: f"level_{level}" for vertex, level in inferred_levels.items()}
            )

        for v in sorted(all_vertices):
            nodes += [
                (
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=v),
                    {
                        "type": AuxNodeType.VERTEX,
                        "label": get_auxnode_id(
                            AuxNodeType.VERTEX, vertex=v, label=True
                        ),
                    },
                )
            ]

        g.add_nodes_from(nodes)
        g.add_edges_from(rendered_edges)

        if prune_leaves:
            out_deg = g.out_degree()
            in_deg = g.in_degree()

            nodes_to_remove = set([k for k, v in out_deg if v == 0]) & set(
                [k for k, v in in_deg if v < 2]
            )
            g.remove_nodes_from(nodes_to_remove)

        partition_color_map = (
            self._build_partition_color_map(
                partition_values=set(effective_partition.values()),
                partition_colors=partition_colors,
            )
            if effective_partition and color_by_partition
            else {}
        )

        self._style_nodes(
            g,
            partition_by_vertex=effective_partition if partition_color_map else None,
            partition_color_map=partition_color_map if partition_color_map else None,
        )
        self._style_edges(g, source_type_style_map=EDGE_STYLE_BY_SOURCE_TYPE)

        ag = nx.nx_agraph.to_agraph(g)
        if group_by_partition and effective_partition:
            self._add_partition_subgraphs(ag, g, effective_partition)

        self._draw(ag, self._versioned_stem(f"{self.prefix}_vc2vc"))
