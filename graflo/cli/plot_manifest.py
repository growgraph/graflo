"""manifest visualization tool for graph databases.

This module provides functionality for visualizing graph database schemas using Graphviz.
It includes tools for plotting vertex-to-vertex relationships, vertex fields, and resource
mappings. The module supports various visualization options and graph layout customization.

Key Components:
    - manifestPlotter: Main class for manifest visualization
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

logger = logging.getLogger(__name__)

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


#: The figure families this command can draw, as ``--only`` accepts them.
FIGURES = ("vc2vc", "vc2fields", "resources", "source2vc", "source2vc_detailed")


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
    help=(
        "Color vc2vc vertices by inferred level: strongly connected vertex types "
        "share a level, level 0 (sources) has no incoming edges from other levels, "
        "each other level is one past "
        "its deepest predecessor. The legend names each colour."
    ),
)
@click.option(
    "--include-all-vertices/--edges-only-vertices",
    default=True,
    help="Include isolated vertex collections in vc2vc plot.",
)
@click.option(
    "--output-format",
    type=click.Choice(["svg", "pdf", "png", "dot"], case_sensitive=False),
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
@click.option(
    "--only",
    multiple=True,
    type=click.Choice(FIGURES, case_sensitive=False),
    help=(
        "Draw only these figures; repeatable. Omitted draws all of them: "
        + ", ".join(FIGURES)
        + "."
    ),
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
    only,
):
    """Generate visualizations of the graph database manifest.

    This command creates multiple visualizations of the manifest:
    1. Vertex-to-vertex relationships
    2. Vertex fields and their relationships
    3. Resource mappings

    The visualizations are saved to the specified output path. Every figure
    carries a legend of its colours and shapes. A figure that cannot be drawn
    is reported and the others are still drawn; the command then exits 1.

    Args:
        manifest_path: Path to the manifest configuration file
        figure_output_path: Path where the visualization will be saved
        prune_low_degree_nodes: Whether to remove nodes with low connectivity
            from the visualization (default: False)
        group_vc_by_level: Whether to cluster vc2vc by inferred graph level
        color_vc_by_level: Whether to color vc2vc nodes by inferred graph level
        include_all_vertices: Whether to include isolated vertex collections
        output_format: Output image format (svg, pdf, png or dot)
        output_dpi: DPI for raster outputs (png)
        only: Figures to draw; empty draws all of them

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
    wanted = {name.lower() for name in only} or set(FIGURES)
    figures = {
        "vc2vc": lambda: plotter.plot_vc2vc(
            prune_leaves=prune_low_degree_nodes,
            group_by_inferred_level=(group_vc_by_level or color_vc_by_level),
            color_by_partition=color_vc_by_level,
            group_by_partition=group_vc_by_level,
            include_all_vertices=include_all_vertices,
        ),
        "vc2fields": plotter.plot_vc2fields,
        "resources": plotter.plot_resources,
        "source2vc": plotter.plot_source2vc,
        "source2vc_detailed": plotter.plot_source2vc_detailed,
    }
    failed: list[str] = []
    for name in FIGURES:
        if name not in wanted:
            continue
        try:
            figures[name]()
        except RuntimeError as exc:
            # One figure that cannot be drawn does not cost the others.
            logger.error("figure %s not drawn: %s", name, exc)
            failed.append(name)
    if failed:
        raise click.ClickException(
            f"not drawn: {', '.join(failed)}. Leave a figure out with --only, or "
            "write its DOT source with --output-format dot and lay it out with a "
            "native Graphviz"
        )


if __name__ == "__main__":
    plot_manifest()
