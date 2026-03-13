"""manifest visualization tool for graph databases.

This module provides functionality for visualizing graph database schemas using Graphviz.
It includes tools for plotting vertex-to-vertex relationships, vertex fields, and resource
mappings. The module supports various visualization options and graph layout customization.

Key Components:
    - manifestPlotter: Main class for manifest visualization
    - knapsack: Utility for optimizing graph layout
    - plot_manifest: CLI command for manifest visualization

Graphviz Attributes Reference:
    - https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
    - https://rsms.me/graphviz/
    - https://graphviz.readthedocs.io/en/stable/examples.html
    - https://graphviz.org/doc/info/attrs.html

Example:
    >>> plot_manifest(manifest_path="manifest.yaml", figure_output_path="manifest.png")
"""

import logging
import sys

import click

from graflo.plot.plotter import ManifestPlotter

"""

graphviz attributes 

https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
https://rsms.me/graphviz/
https://graphviz.readthedocs.io/en/stable/examples.html
https://graphviz.org/doc/info/attrs.html

usage: 
    color='red',style='filled', fillcolor='blue',shape='square'

to keep 
level_one = [node1, node2]
sg_one = ag.add_subgraph(level_one, rank='same')

"""


def knapsack(weights, ks_size=7):
    """Split a set of weights into groups of at most threshold weight.

    This function implements a greedy algorithm to partition weights into groups
    where each group's total weight is at most ks_size. It's used for optimizing
    graph layout by balancing node distribution.

    Args:
        weights: List of weights to partition
        ks_size: Maximum total weight per group (default: 7)

    Returns:
        list[list[int]]: List of groups, where each group is a list of indices
            from the original weights list

    Raises:
        ValueError: If any single weight exceeds ks_size

    Example:
        >>> weights = [3, 4, 2, 5, 1]
        >>> knapsack(weights, ks_size=7)
        [[4, 0, 2], [1, 3]]  # Groups with weights [6, 7]
    """
    pp = sorted(list(zip(range(len(weights)), weights)), key=lambda x: x[1])
    print(pp)
    acc = []
    if pp[-1][1] > ks_size:
        raise ValueError("One of the items is larger than the knapsack")

    while pp:
        w_item = []
        w_item += [pp.pop()]
        ww_item = sum([item for _, item in w_item])
        while ww_item < ks_size:
            cnt = 0
            for j, item in enumerate(pp[::-1]):
                diff = ks_size - item[1] - ww_item
                if diff >= 0:
                    cnt += 1
                    w_item += [pp.pop(len(pp) - j - 1)]
                    ww_item += w_item[-1][1]
                else:
                    break
            if ww_item >= ks_size or cnt == 0:
                acc += [w_item]
                break
    acc_ret = [[y for y, _ in subitem] for subitem in acc]
    return acc_ret


@click.command()
@click.option("-c", "--manifest-path", type=click.Path(), required=True)
@click.option("-o", "--figure-output-path", type=click.Path(), required=True)
@click.option("-p", "--prune-low-degree-nodes", type=bool, default=False)
@click.option(
    "--group-vc-by-level",
    is_flag=True,
    default=False,
    help="Group vc2vc graph by inferred levels (SCC-aware layering).",
)
@click.option(
    "--color-vc-by-level",
    is_flag=True,
    default=False,
    help="Color vc2vc vertices by inferred levels.",
)
@click.option(
    "--include-all-vertices/--edges-only-vertices",
    default=True,
    help="Include isolated vertex collections in vc2vc plot.",
)
@click.option(
    "--output-format",
    type=click.Choice(["pdf", "png"], case_sensitive=False),
    default="pdf",
    show_default=True,
    help="Output figure format.",
)
@click.option(
    "--output-dpi",
    type=click.IntRange(min=72),
    default=300,
    show_default=True,
    help="DPI used when output format is png.",
)
def plot_manifest(
    manifest_path,
    figure_output_path,
    prune_low_degree_nodes,
    group_vc_by_level,
    color_vc_by_level,
    include_all_vertices,
    output_format,
    output_dpi,
):
    """Generate visualizations of the graph database manifest.

    This command creates multiple visualizations of the manifest:
    1. Vertex-to-vertex relationships
    2. Vertex fields and their relationships
    3. Resource mappings

    The visualizations are saved to the specified output path.

    Args:
        manifest_path: Path to the manifest configuration file
        figure_output_path: Path where the visualization will be saved
        prune_low_degree_nodes: Whether to remove nodes with low connectivity
            from the visualization (default: False)
        group_vc_by_level: Whether to cluster vc2vc by inferred graph level
        color_vc_by_level: Whether to color vc2vc nodes by inferred graph level
        include_all_vertices: Whether to include isolated vertex collections
        output_format: Output image format (pdf or png)
        output_dpi: DPI for raster outputs (png)

    Example:
        $ uv run plot_manifest -c manifest.yaml -o output_dir
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    plotter = ManifestPlotter(
        manifest_path,
        figure_output_path,
        output_format=output_format.lower(),
        output_dpi=output_dpi if output_format.lower() == "png" else None,
    )
    plotter.plot_vc2vc(
        prune_leaves=prune_low_degree_nodes,
        group_by_inferred_level=(group_vc_by_level or color_vc_by_level),
        color_by_partition=color_vc_by_level,
        group_by_partition=group_vc_by_level,
        include_all_vertices=include_all_vertices,
    )
    plotter.plot_vc2fields()
    plotter.plot_resources()
    plotter.plot_source2vc()
    plotter.plot_source2vc_detailed()


if __name__ == "__main__":
    plot_manifest()
