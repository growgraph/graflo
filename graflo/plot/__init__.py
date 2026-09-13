"""Drawing what a manifest declares, and what reconciling two of them would do.

Three families, all rendered through Graphviz (the ``plot`` extra):

* :class:`~graflo.plot.plotter.ManifestPlotter` — a manifest as it stands:
  vertex types and their edges, vertices and their fields, resources and the
  vertices they feed, and each resource's actor pipeline.
* :func:`~graflo.plot.compose.plot_compose_preview` — what composing two
  manifests would do, and every way the declarations could refuse.
* :func:`~graflo.plot.merge.plot_merge_preview` and
  :func:`~graflo.plot.merge.plot_history` — where two branches of one lineage
  collided, and the commit DAG they collided on.

Each ``plot_*`` function has a ``build_*`` counterpart returning a plain
:mod:`networkx` graph, so what would be drawn can be inspected — or asserted
against — without Graphviz installed.

Example:
    >>> from graflo.plot import ManifestPlotter
    >>> ManifestPlotter(graph_manifest=manifest, fig_path="figs").plot_vc2vc()
"""

from .compose import build_preview_graph, plot_compose_preview
from .merge import (
    build_history_graph,
    build_merge_graph,
    plot_history,
    plot_merge_preview,
)
from .plotter import ManifestPlotter
from .render import OUTPUT_FORMATS

__all__ = [
    "OUTPUT_FORMATS",
    "ManifestPlotter",
    "build_history_graph",
    "build_merge_graph",
    "build_preview_graph",
    "plot_compose_preview",
    "plot_history",
    "plot_merge_preview",
]
