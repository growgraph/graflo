"""Drawing what a manifest declares, and what reconciling two of them would do.

Four families, all rendered through Graphviz (the ``plot`` extra):

* :class:`~graflo.plot.plotter.ManifestPlotter` — a manifest as it stands:
  vertex types and their edges, vertices and their fields, resources and the
  vertices they feed, and each resource's actor pipeline.
* :func:`~graflo.plot.merge.plot_merge_preview` — what merging two
  manifests would do, and every way the declarations could refuse.
* :func:`~graflo.plot.equivalence.plot_equivalence` and
  :func:`~graflo.plot.equivalence.plot_equivalences` — one equivalence as four
  small graphs: its members, attributes, identity and lookups.
* :func:`~graflo.plot.merge3.plot_merge3_preview` and
  :func:`~graflo.plot.merge3.plot_history` — where two branches of one lineage
  collided, and the commit DAG they collided on.

Each ``plot_*`` function has a ``build_*`` counterpart returning a plain
:mod:`networkx` graph, so what would be drawn can be inspected — or asserted
against — without Graphviz installed.

Example:
    >>> from graflo.plot import ManifestPlotter
    >>> ManifestPlotter(graph_manifest=manifest, fig_path="figs").plot_vc2vc()
"""

from .equivalence import build_equivalence_view, plot_equivalence, plot_equivalences
from .merge import build_preview_graph, plot_merge_preview
from .merge3 import (
    build_history_graph,
    build_merge3_graph,
    plot_history,
    plot_merge3_preview,
)
from .plotter import ManifestPlotter
from .render import OUTPUT_FORMATS

__all__ = [
    "OUTPUT_FORMATS",
    "ManifestPlotter",
    "build_equivalence_view",
    "build_history_graph",
    "build_merge3_graph",
    "build_preview_graph",
    "plot_equivalence",
    "plot_equivalences",
    "plot_history",
    "plot_merge3_preview",
    "plot_merge_preview",
]
