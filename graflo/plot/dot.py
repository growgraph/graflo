"""A Graphviz graph as graflo's figures build it, written out as DOT text.

The figures are assembled as :mod:`networkx` graphs, then grouped into
clusters and written here; nothing in this module needs Graphviz itself.
:class:`DotGraph` keeps the conventions of ``networkx.nx_agraph.to_agraph``:
``graph.graph["graph" | "node" | "edge"]`` are the default attributes, any
other graph-level key is a graph attribute, and every value is written as its
``str``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable, Mapping

    import networkx as nx


def quote(value: Any) -> str:
    """*value* as a DOT quoted string.

    Inside quotes DOT treats only ``\\"`` as an escape and keeps every other
    backslash for the label (``\\n``, ``\\l``, record escapes), so only quotes
    are escaped — plus a trailing lone backslash, which would otherwise
    swallow the closing quote.
    """
    text = str(value).replace('"', '\\"')
    trailing = len(text) - len(text.rstrip("\\"))
    if trailing % 2:
        text += "\\"
    return f'"{text}"'


def _attr_list(attrs: Mapping[str, Any]) -> str:
    return ", ".join(f"{quote(key)}={quote(value)}" for key, value in attrs.items())


@dataclass
class DotSubgraph:
    """A named subgraph; a ``cluster_`` name draws it as a box.

    Attributes:
        name: The subgraph name.
        nodes: Member node ids, in order.
        graph_attr: Attributes of the subgraph itself (``label``, ``rank``, …).
        node_attr: Defaults for the member nodes.
    """

    name: str
    nodes: list[str]
    graph_attr: dict[str, str] = field(default_factory=dict)
    node_attr: dict[str, str] = field(default_factory=dict)


class DotGraph:
    """Nodes, edges, subgraphs and default attributes of one Graphviz graph.

    Args:
        directed: ``digraph`` or ``graph``.
        strict: Write ``strict``: at most one edge per pair of nodes.
        name: The graph name.
    """

    def __init__(self, *, directed: bool = True, strict: bool = False, name: str = ""):
        self.directed = directed
        self.strict = strict
        self.name = name
        self.graph_attr: dict[str, str] = {}
        self.node_attr: dict[str, str] = {}
        self.edge_attr: dict[str, str] = {}
        self.nodes: dict[str, dict[str, str]] = {}
        self.edges: list[tuple[str, str, dict[str, str]]] = []
        self.subgraphs: list[DotSubgraph] = []

    @classmethod
    def from_networkx(cls, graph: nx.Graph) -> DotGraph:
        """*graph* with every node, edge and attribute carried over as strings."""
        import networkx as nx_mod

        dot = cls(
            directed=graph.is_directed(),
            strict=nx_mod.number_of_selfloops(graph) == 0 and not graph.is_multigraph(),
            name=str(graph.name or ""),
        )
        defaults = {"graph", "node", "edge"}
        dot.graph_attr.update(
            {k: str(v) for k, v in graph.graph.items() if k not in defaults | {"name"}}
        )
        dot.graph_attr.update(_strings(graph.graph.get("graph", {})))
        dot.node_attr.update(_strings(graph.graph.get("node", {})))
        dot.edge_attr.update(_strings(graph.graph.get("edge", {})))
        for node, data in graph.nodes(data=True):
            dot.add_node(str(node), **data)
        for tail, head, data in graph.edges(data=True):
            dot.add_edge(str(tail), str(head), **data)
        return dot

    def add_node(self, node: str, **attrs: Any) -> None:
        """Add *node*, or merge *attrs* into it if it is already there."""
        self.nodes.setdefault(node, {}).update(_strings(attrs))

    def add_edge(self, tail: str, head: str, **attrs: Any) -> None:
        """Add an edge, adding either endpoint that is missing."""
        for node in (tail, head):
            self.nodes.setdefault(node, {})
        self.edges.append((tail, head, _strings(attrs)))

    def add_subgraph(
        self, nodes: Iterable[Any], name: str, **attrs: Any
    ) -> DotSubgraph:
        """Group *nodes* under *name*; *attrs* are the subgraph's own attributes."""
        subgraph = DotSubgraph(
            name=name, nodes=[str(n) for n in nodes], graph_attr=_strings(attrs)
        )
        self.subgraphs.append(subgraph)
        return subgraph

    def node_order(self) -> list[str]:
        """Nodes in the order the written DOT declares them: subgraph members first.

        Graphviz creates a node where it first appears, and layout and
        :meth:`unflatten` both walk nodes in that order.
        """
        order: dict[str, None] = {}
        for subgraph in self.subgraphs:
            for node in subgraph.nodes:
                if node in self.nodes:
                    order.setdefault(node)
        for node in self.nodes:
            order.setdefault(node)
        return list(order)

    def unflatten(
        self, *, levels: int = 0, fans: bool = False, chain: int = 0
    ) -> DotGraph:
        """Graphviz's ``unflatten``, in place: make a wide directed graph taller.

        Edges between a node and its leaves get staggered ``minlen`` values
        (1..*levels*), so leaves stack in rows instead of one wide rank; with
        *fans*, so do edges to nodes on a simple chain; isolated nodes are tied
        into chains of *chain* by invisible edges. Edges that already carry a
        ``minlen`` are left alone. Same as ``unflatten -l levels [-f] -c chain``.

        Returns:
            This graph, for chaining.
        """
        out_edges: dict[str, list[dict[str, str]]] = {n: [] for n in self.nodes}
        in_edges: dict[str, list[dict[str, str]]] = {n: [] for n in self.nodes}
        heads_of: dict[int, str] = {}
        tails_of: dict[int, str] = {}
        for tail, head, attrs in self.edges:
            out_edges[tail].append(attrs)
            in_edges[head].append(attrs)
            heads_of[id(attrs)] = head
            tails_of[id(attrs)] = tail

        def indegree(node: str) -> int:
            # Graphviz counts self-loops on the in side only.
            return len(in_edges[node])

        def outdegree(node: str) -> int:
            return sum(1 for e in out_edges[node] if heads_of[id(e)] != node)

        def is_leaf(node: str) -> bool:
            return indegree(node) + outdegree(node) == 1

        def is_chain_node(node: str) -> bool:
            return indegree(node) == 1 and outdegree(node) == 1

        def unset(attrs: dict[str, str]) -> bool:
            return not attrs.get("minlen", self.edge_attr.get("minlen", ""))

        chain_node: str | None = None
        chain_size = 0
        for node in self.node_order():
            degree = indegree(node) + outdegree(node)
            if degree == 0:
                if chain < 1:
                    continue
                if chain_node is None:
                    chain_node = node
                    continue
                self.edges.append((chain_node, node, {"style": "invis"}))
                chain_size += 1
                if chain_size < chain:
                    chain_node = node
                else:
                    chain_node, chain_size = None, 0
            elif degree > 1:
                if levels < 1:
                    continue
                count = 0
                for attrs in in_edges[node]:
                    if is_leaf(tails_of[id(attrs)]) and unset(attrs):
                        attrs["minlen"] = str(count % levels + 1)
                        count += 1
                count = 0
                for attrs in out_edges[node]:
                    head = heads_of[id(attrs)]
                    if is_leaf(head) or (fans and is_chain_node(head)):
                        if unset(attrs):
                            attrs["minlen"] = str(count % levels + 1)
                        count += 1
        return self

    def to_string(self) -> str:
        """The graph as DOT source."""
        keyword = "digraph" if self.directed else "graph"
        arrow = "->" if self.directed else "--"
        strict = "strict " if self.strict else ""
        lines = [f"{strict}{keyword} {quote(self.name)} {{"]
        for kind, attrs in (
            ("graph", self.graph_attr),
            ("node", self.node_attr),
            ("edge", self.edge_attr),
        ):
            if attrs:
                lines.append(f"  {kind} [{_attr_list(attrs)}];")
        declared: set[str] = set()
        for subgraph in self.subgraphs:
            lines.append(f"  subgraph {quote(subgraph.name)} {{")
            if subgraph.graph_attr:
                lines.append(f"    graph [{_attr_list(subgraph.graph_attr)}];")
            if subgraph.node_attr:
                lines.append(f"    node [{_attr_list(subgraph.node_attr)}];")
            for node in subgraph.nodes:
                if node not in self.nodes:
                    continue
                lines.append(f"    {self._node_line(node, declared)}")
            lines.append("  }")
        for node in self.nodes:
            if node not in declared:
                lines.append(f"  {self._node_line(node, declared)}")
        for tail, head, attrs in self.edges:
            suffix = f" [{_attr_list(attrs)}]" if attrs else ""
            lines.append(f"  {quote(tail)} {arrow} {quote(head)}{suffix};")
        lines.append("}")
        return "\n".join(lines) + "\n"

    def _node_line(self, node: str, declared: set[str]) -> str:
        """A node statement: with its attributes the first time, by id after."""
        attrs = self.nodes[node] if node not in declared else {}
        declared.add(node)
        suffix = f" [{_attr_list(attrs)}]" if attrs else ""
        return f"{quote(node)}{suffix};"


def _strings(attrs: Mapping[str, Any]) -> dict[str, str]:
    return {str(k): str(v) for k, v in attrs.items()}


__all__ = ["DotGraph", "DotSubgraph", "quote"]
