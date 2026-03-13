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
from graflo.architecture.actor import (
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
map_type2shape = {
    AuxNodeType.RESOURCE: "box",
    AuxNodeType.VERTEX_BLANK: "box",
    AuxNodeType.FIELD_DEFINITION: "trapezium",
    AuxNodeType.TRANSFORM: "oval",
    AuxNodeType.VERTEX: "ellipse",
    AuxNodeType.INDEX: "polygon",
    AuxNodeType.FIELD: "octagon",
}

# Mapping of node types to colors
map_type2color = {
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
edge_status = {AuxNodeType.VERTEX: "solid"}


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


def lto_dict(strings):
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
        config_filename,
        fig_path,
        output_format: str = "pdf",
        output_dpi: int | None = None,
    ):
        """Initialize the schema plotter.

        Args:
            config_filename: Path to schema configuration file
            fig_path: Path to save visualizations
        """
        self.fig_path = fig_path
        self.output_format = output_format.lower()
        self.output_dpi = output_dpi

        self.config = FileHandle.load(fpath=config_filename)
        manifest = GraphManifest.from_config(self.config)
        manifest.finish_init()
        self.schema = manifest.require_schema()
        self.ingestion_model = manifest.require_ingestion_model()

        self.name = self.schema.metadata.name
        self.prefix = self.name

    def _figure_path(self, stem: str) -> str:
        return os.path.join(self.fig_path, f"{stem}.{self.output_format}")

    def _draw(self, ag, stem: str, prog: str = "dot") -> None:
        if self.output_format == "png" and self.output_dpi is not None:
            ag.graph_attr["dpi"] = str(self.output_dpi)
        ag.draw(
            self._figure_path(stem),
            self.output_format,
            prog=prog,
        )

    def _discover_edges_from_resources(self):
        """Discover edges from resources by walking through ActorWrappers.

        This method finds all EdgeActors in resources and extracts their edges,
        which may include edges with dynamic relations (relation_field, relation_from_key)
        that aren't fully represented in edge_config.

        Returns:
            dict: Dictionary mapping (source, target, relation) to Edge objects
        """
        discovered_edges = {}

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

        return discovered_edges

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
        graph.add_nodes_from(self.schema.graph.vertex_config.vertex_set)
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
        vconf = self.schema.graph.vertex_config
        vertex_prefix_dict = lto_dict(
            [v for v in self.schema.graph.vertex_config.vertex_set]
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

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = props.copy()
            if "type" in upd_dict:
                upd_dict["shape"] = map_type2shape[props["type"]]
                upd_dict["color"] = map_type2color[props["type"]]
            if "label" in upd_dict:
                upd_dict["forcelabel"] = True
            upd_dict["style"] = "filled"

            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        for e in g.edges(data=True):
            s, t, _ = e
            upd_dict = {"style": "solid", "arrowhead": "vee"}
            for k, v in upd_dict.items():
                g.edges[s, t][k] = v

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
        self._draw(ag, f"{self.prefix}_vc2fields")

    def plot_resources(self):
        """Plot resource relationships.

        Creates visualizations for each resource in the schema, showing their
        internal structure and relationships. Each resource is saved as a
        separate PDF file.
        """
        resource_prefix_dict = lto_dict(
            [resource.name for resource in self.ingestion_model.resources]
        )
        vertex_prefix_dict = lto_dict(
            [v for v in self.schema.graph.vertex_config.vertex_set]
        )
        kwargs = {"vertex_sh": vertex_prefix_dict, "resource_sh": resource_prefix_dict}

        for resource in self.ingestion_model.resources:
            kwargs["resource"] = resource.name
            assemble_tree(
                resource.root,
                self._figure_path(
                    f"{self.schema.metadata.name}.resource-{resource.resource_name}"
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
        vertex_set = set(self.schema.graph.vertex_config.vertex_set)

        for resource in self.ingestion_model.resources:
            resource_id = get_auxnode_id(
                AuxNodeType.RESOURCE,
                resource=resource.name,
                resource_type="resource",
            )
            g.add_node(
                resource_id,
                type=AuxNodeType.RESOURCE,
                label=resource.name,
            )

            vertex_reasons = self._extract_resource_vertex_reasons(resource)
            for vertex_name, reasons in sorted(vertex_reasons.items()):
                if vertex_name not in vertex_set:
                    continue
                vertex_id = get_auxnode_id(AuxNodeType.VERTEX, vertex=vertex_name)
                g.add_node(
                    vertex_id,
                    type=AuxNodeType.VERTEX,
                    label=vertex_name,
                )
                edge_label = ", ".join(sorted(reasons))
                g.add_edge(resource_id, vertex_id, label=edge_label)

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = {
                "shape": map_type2shape[props["type"]],
                "color": map_type2color[props["type"]],
                "style": "filled",
            }
            if "label" in props:
                upd_dict["forcelabel"] = True
            for attr_key, attr_value in upd_dict.items():
                g.nodes[n][attr_key] = attr_value

        for s, t, k in g.edges:
            edge_data = g.edges[s, t, k]
            upd_dict = {
                "arrowhead": "vee",
                "style": "solid",
            }
            if "label" in edge_data:
                upd_dict["label"] = edge_data["label"]
            for attr_key, attr_value in upd_dict.items():
                g.edges[s, t, k][attr_key] = attr_value

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
            for v in sorted(self.schema.graph.vertex_config.vertex_set)
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
        known_vertices = set(self.schema.graph.vertex_config.vertex_set)
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
            resource_id = get_auxnode_id(
                AuxNodeType.RESOURCE,
                resource=resource.name,
                resource_type="resource",
            )
            g.add_node(
                resource_id,
                type=AuxNodeType.RESOURCE,
                label=resource.name,
            )

            vertex_reasons = self._extract_resource_vertex_reasons(resource)
            for vertex_name, reasons in sorted(vertex_reasons.items()):
                vertex_id = get_auxnode_id(AuxNodeType.VERTEX, vertex=vertex_name)
                g.add_node(
                    vertex_id,
                    type=AuxNodeType.VERTEX,
                    label=vertex_name,
                )
                g.add_edge(resource_id, vertex_id, label=", ".join(sorted(reasons)))

            for n in g.nodes():
                props = g.nodes()[n]
                upd_dict = {
                    "shape": map_type2shape[props["type"]],
                    "color": map_type2color[props["type"]],
                    "style": "filled",
                }
                for attr_key, attr_value in upd_dict.items():
                    g.nodes[n][attr_key] = attr_value

            for s, t, k in g.edges:
                edge_data = g.edges[s, t, k]
                upd_dict = {
                    "arrowhead": "vee",
                    "style": "solid",
                }
                if "label" in edge_data:
                    upd_dict["label"] = edge_data["label"]
                for attr_key, attr_value in upd_dict.items():
                    g.edges[s, t, k][attr_key] = attr_value

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
        nodes = []
        edges = []

        # Discover edges from resources (may include edges not in edge_config)
        discovered_edges = self._discover_edges_from_resources()

        # Collect all edges: from edge_config and discovered from resources
        all_edges = {}
        for edge_id, e in self.schema.graph.edge_config.edges_items():
            all_edges[edge_id] = e
        # Add discovered edges (they may already be in edge_config, but that's fine)
        for edge_id, e in discovered_edges.items():
            if edge_id not in all_edges:
                all_edges[edge_id] = e

        edge_pairs = [(source, target) for (source, target, _relation) in all_edges]

        # Create graph edges with relation labels
        for (source, target, relation), e in all_edges.items():
            # Determine label based on relation configuration
            label = None
            if e.relation is not None:
                # Static relation
                label = e.relation
            elif e.relation_field is not None:
                # Dynamic relation from field - show indicator
                label = f"[{e.relation_field}]"
            elif e.relation_from_key:
                # Dynamic relation from key - show indicator
                label = "[key]"

            if label is not None:
                ee = (
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=source),
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=target),
                    {"label": label},
                )
            else:
                ee = (
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=source),
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=target),
                )
            edges += [ee]

        # Create nodes for vertices involved in edges, optionally including all schema vertices
        vertices_in_edges = {
            vertex
            for (source, target, _relation) in all_edges
            for vertex in (source, target)
        }
        all_vertices = (
            set(self.schema.graph.vertex_config.vertex_set)
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

        for nid, weight in nodes:
            g.add_node(nid, **weight)

        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

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

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = {
                "shape": map_type2shape[props["type"]],
                "color": map_type2color[props["type"]],
                "style": "filled",
            }
            if partition_color_map:
                vertex_name = self._node_id_to_vertex(n)
                group_key = effective_partition.get(vertex_name)
                if group_key is not None:
                    upd_dict["fillcolor"] = partition_color_map[group_key]
            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        for e in g.edges:
            s, t, ix = e
            target_props = g.nodes[s]
            edge_data = g.edges[s, t, ix]
            upd_dict = {
                "style": edge_status[target_props["type"]],
                "arrowhead": "vee",
            }
            # Preserve existing label if present (for relation display)
            if "label" in edge_data:
                upd_dict["label"] = edge_data["label"]
            for k, v in upd_dict.items():
                g.edges[s, t, ix][k] = v

        ag = nx.nx_agraph.to_agraph(g)
        if group_by_partition and effective_partition:
            partition_to_nodes: dict[str, list[str]] = {}
            for node_id in g.nodes():
                vertex_name = self._node_id_to_vertex(node_id)
                group = effective_partition.get(vertex_name)
                if group is None:
                    continue
                if group not in partition_to_nodes:
                    partition_to_nodes[group] = []
                partition_to_nodes[group].append(node_id)

            for group in sorted(partition_to_nodes):
                ag.add_subgraph(
                    partition_to_nodes[group],
                    name=f"cluster_partition_{group}",
                    rank="same",
                    label=str(group),
                )

        self._draw(ag, f"{self.prefix}_vc2vc")
