"""Actor-based system for graph data transformation and processing.

This module implements a system for processing and transforming graph data.
It provides a flexible framework for defining and executing data transformations through
a tree of `actors`. The system supports various types of actors:

- VertexActor: Processes and transforms vertex data
- EdgeActor: Handles edge creation and transformation
- TransformActor: Applies transformations to data
- DescendActor: Manages hierarchical processing of nested data structures

The module uses an action context to maintain state during processing and supports
both synchronous and asynchronous operations. It integrates with the graph database
infrastructure to handle vertex and edge operations.

Example:
    >>> wrapper = ActorWrapper(vertex="user")
    >>> ctx = ActionContext()
    >>> result = wrapper(ctx, doc={"id": "123", "name": "John"})
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import reduce
from pathlib import Path
from types import MappingProxyType
from typing import Optional, Type

from graflo.architecture.actor_util import (
    add_blank_collections,
    render_edge,
    render_weights,
)
from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import (
    ActionContext,
    GraphEntity,
    LocationIndex,
    VertexRep,
)
from graflo.architecture.transform import ProtoTransform, Transform
from graflo.architecture.vertex import (
    VertexConfig,
)
from graflo.util.merge import (
    merge_doc_basis,
    merge_doc_basis_closest_preceding,
)
from graflo.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


DESCEND_KEY = "key"
DRESSING_TRANSFORMED_VALUE_KEY = "__value__"


class Actor(ABC):
    """Abstract base class for all actors in the system.

    Actors are the fundamental processing units in the graph transformation system.
    Each actor type implements specific functionality for processing graph data.

    Attributes:
        None (abstract class)
    """

    @abstractmethod
    def __call__(self, ctx: ActionContext, lindex: LocationIndex, *nargs, **kwargs):
        """Execute the actor's main processing logic.

        Args:
            ctx: The action context containing the current processing state
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Updated action context
        """
        pass

    def fetch_important_items(self):
        """Get a dictionary of important items for string representation.

        Returns:
            dict: Dictionary of important items
        """
        return {}

    def finish_init(self, **kwargs):
        """Complete initialization of the actor.

        Args:
            **kwargs: Additional initialization parameters
        """
        pass

    def init_transforms(self, **kwargs):
        """Initialize transformations for the actor.

        Args:
            **kwargs: Transformation parameters
        """
        pass

    def count(self):
        """Get the count of items processed by this actor.

        Returns:
            int: Number of items
        """
        return 1

    def _filter_items(self, items):
        """Filter out None and empty items.

        Args:
            items: Dictionary of items to filter

        Returns:
            dict: Filtered dictionary
        """
        return {k: v for k, v in items.items() if v is not None and v}

    def _stringify_items(self, items):
        """Convert items to string representation.

        Args:
            items: Dictionary of items to stringify

        Returns:
            dict: Dictionary with stringified values
        """
        return {
            k: ", ".join(list(v)) if isinstance(v, (tuple, list)) else v
            for k, v in items.items()
        }

    def __str__(self):
        """Get string representation of the actor.

        Returns:
            str: String representation
        """
        d = self.fetch_important_items()
        d = self._filter_items(d)
        d = self._stringify_items(d)
        d_list = [[k, d[k]] for k in sorted(d)]
        d_list_b = [type(self).__name__] + [": ".join(x) for x in d_list]
        d_list_str = "\n".join(d_list_b)
        return d_list_str

    __repr__ = __str__

    def fetch_actors(self, level, edges):
        """Fetch actor information for tree representation.

        Args:
            level: Current level in the actor tree
            edges: List of edges in the actor tree

        Returns:
            tuple: (level, actor_type, string_representation, edges)
        """
        return level, type(self), str(self), edges


class VertexActor(Actor):
    """Actor for processing vertex data.

    This actor handles the processing and transformation of vertex data, including
    field selection.

    Attributes:
        name: Name of the vertex
        keep_fields: Optional tuple of fields to keep
        vertex_config: Configuration for the vertex
    """

    def __init__(
        self,
        vertex: str,
        keep_fields: tuple[str, ...] | None = None,
        **kwargs,
    ):
        """Initialize the vertex actor.

        Args:
            vertex: Name of the vertex
            keep_fields: Optional tuple of fields to keep
            **kwargs: Additional initialization parameters
        """
        self.name = vertex
        self.keep_fields: tuple[str, ...] | None = keep_fields
        self.vertex_config: VertexConfig

    def fetch_important_items(self):
        """Get important items for string representation.

        Returns:
            dict: Dictionary of important items
        """
        sd = self.__dict__
        return {k: sd[k] for k in ["name", "keep_fields"]}

    def finish_init(self, **kwargs):
        """Complete initialization of the vertex actor.

        Args:
            **kwargs: Additional initialization parameters
        """
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")

    def __call__(self, ctx: ActionContext, lindex: LocationIndex, *nargs, **kwargs):
        """Process vertex data.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments including 'doc'

        Returns:
            Updated action context
        """
        doc: dict = kwargs.pop("doc", {})

        vertex_keys = self.vertex_config.fields(self.name, with_aux=True)
        buffer_vertex = ctx.buffer_vertex.pop(self.name, [])

        agg = []

        for item in ctx.buffer_transforms[lindex]:
            _doc: dict = dict()
            n_value_keys = len(
                [k for k in item if k.startswith(DRESSING_TRANSFORMED_VALUE_KEY)]
            )
            for j in range(n_value_keys):
                vkey = self.vertex_config.index(self.name).fields[j]
                v = item.pop(f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}")
                _doc[vkey] = v

            for vkey in set(vertex_keys) - set(_doc):
                v = item.pop(vkey, None)
                if v is not None:
                    _doc[vkey] = v

            if all(cfilter(doc) for cfilter in self.vertex_config.filters(self.name)):
                agg += [_doc]

        ctx.buffer_transforms[lindex] = [x for x in ctx.buffer_transforms[lindex] if x]

        for item in buffer_vertex:
            _doc = {k: item[k] for k in vertex_keys if k in item}

            if all(cfilter(doc) for cfilter in self.vertex_config.filters(self.name)):
                agg += [_doc]

        remaining_keys = set(vertex_keys) - reduce(
            lambda acc, d: acc | d.keys(), agg, set()
        )
        passthrough_doc = {}
        for k in remaining_keys:
            if k in doc:
                passthrough_doc[k] = doc.pop(k)
        if passthrough_doc:
            agg += [passthrough_doc]

        merged = merge_doc_basis(
            agg, index_keys=tuple(self.vertex_config.index(self.name).fields)
        )

        ctx.acc_vertex[self.name][lindex] += [
            VertexRep(
                vertex=m,
                ctx={q: w for q, w in doc.items() if not isinstance(w, (dict, list))},
            )
            for m in merged
        ]
        return ctx


class EdgeActor(Actor):
    """Actor for processing edge data.

    This actor handles the creation and transformation of edges between vertices,
    including weight calculations and relationship management.

    Attributes:
        edge: Edge configuration
        vertex_config: Vertex configuration
    """

    def __init__(
        self,
        **kwargs,
    ):
        """Initialize the edge actor.

        Args:
            **kwargs: Edge configuration parameters
        """
        self.edge = Edge.from_dict(kwargs)
        self.vertex_config: VertexConfig

    def fetch_important_items(self):
        """Get important items for string representation.

        Returns:
            dict: Dictionary of important items
        """
        sd = self.edge.__dict__
        return {k: sd[k] for k in ["source", "target", "match_source", "match_target"]}

    def finish_init(self, **kwargs):
        """Complete initialization of the edge actor.

        Args:
            **kwargs: Additional initialization parameters
        """
        self.vertex_config: VertexConfig = kwargs.pop("vertex_config")
        edge_config: Optional[EdgeConfig] = kwargs.pop("edge_config", None)
        if edge_config is not None and self.vertex_config is not None:
            self.edge.finish_init(vertex_config=self.vertex_config)
            edge_config.update_edges(self.edge, vertex_config=self.vertex_config)

    def __call__(self, ctx: ActionContext, lindex: LocationIndex, *nargs, **kwargs):
        """Process edge data.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Updated action context
        """

        ctx = self.merge_vertices(ctx)
        edges = render_edge(self.edge, self.vertex_config, ctx, lindex=lindex)

        edges = render_weights(
            self.edge,
            self.vertex_config,
            ctx.acc_vertex,
            edges,
        )

        for relation, v in edges.items():
            ctx.acc_global[self.edge.source, self.edge.target, relation] += v

        return ctx

    def merge_vertices(self, ctx) -> ActionContext:
        for vertex, dd in ctx.acc_vertex.items():
            for lindex, vertex_list in dd.items():
                vvv = merge_doc_basis_closest_preceding(
                    vertex_list,
                    tuple(self.vertex_config.index(vertex).fields),
                )
                # vvv = pick_unique_dict(vvv)
                ctx.acc_vertex[vertex][lindex] = vvv
        return ctx


class TransformActor(Actor):
    """Actor for applying transformations to data.

    This actor handles the application of transformations to input data, supporting
    both simple and complex transformation scenarios.

    Attributes:
        _kwargs: Original initialization parameters
        vertex: Optional target vertex
        transforms: Dictionary of available transforms
        name: Transform name
        params: Transform parameters
        t: Transform instance
    """

    def __init__(self, **kwargs):
        """Initialize the transform actor.

        Args:
            **kwargs: Transform configuration parameters
        """
        self._kwargs = kwargs
        self.vertex: Optional[str] = kwargs.pop("target_vertex", None)
        self.transforms: dict
        self.name = kwargs.get("name", None)
        self.params = kwargs.get("params", {})
        self.t: Transform = Transform(**kwargs)

    def fetch_important_items(self):
        """Get important items for string representation.

        Returns:
            dict: Dictionary of important items
        """
        sd = self.__dict__
        sm = {k: sd[k] for k in ["name", "vertex"]}
        smb = {"t.input": self.t.input, "t.output": self.t.output}
        return {**sm, **smb}

    def init_transforms(self, **kwargs):
        """Initialize available transforms.

        Args:
            **kwargs: Transform initialization parameters
        """
        self.transforms = kwargs.pop("transforms", {})
        try:
            pt = ProtoTransform(
                **{
                    k: self._kwargs[k]
                    for k in ProtoTransform.get_fields_members()
                    if k in self._kwargs
                }
            )
            if pt.name is not None and pt._foo is not None:
                if pt.name not in self.transforms:
                    self.transforms[pt.name] = pt
                elif pt.params:
                    self.transforms[pt.name] = pt
        except Exception:
            pass

    def finish_init(self, **kwargs):
        """Complete initialization of the transform actor.

        Args:
            **kwargs: Additional initialization parameters
        """
        self.transforms: dict[str, ProtoTransform] = kwargs.pop("transforms", {})

        if self.name is not None:
            pt = self.transforms.get(self.name, None)
            if pt is not None:
                self.t._foo = pt._foo
                self.t.module = pt.module
                self.t.foo = pt.foo
                if pt.params and not self.t.params:
                    self.t.params = pt.params
                    if (
                        pt.input
                        and not self.t.input
                        and pt.output
                        and not self.t.output
                    ):
                        self.t.input = pt.input
                        self.t.output = pt.output
                        self.t.__post_init__()

    def __call__(self, ctx: ActionContext, lindex: LocationIndex, *nargs, **kwargs):
        """Apply transformation to input data.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments including 'doc'

        Returns:
            Updated action context

        Raises:
            ValueError: If no document is provided
        """
        logging.debug(f"transforms : {id(self.transforms)} {len(self.transforms)}")

        if kwargs:
            doc: Optional[dict] = kwargs.get("doc")
        elif nargs:
            doc = nargs[0]
        else:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        _update_doc: dict
        if isinstance(doc, dict):
            _update_doc = self.t(doc)
        else:
            value = self.t(doc)
            if isinstance(value, tuple):
                _update_doc = {
                    f"{DRESSING_TRANSFORMED_VALUE_KEY}#{j}": v
                    for j, v in enumerate(value)
                }
            elif isinstance(value, dict):
                _update_doc = value
            else:
                _update_doc = {f"{DRESSING_TRANSFORMED_VALUE_KEY}#0": value}

        if self.vertex is None:
            ctx.buffer_transforms[lindex] += [_update_doc]
        else:
            ctx.buffer_vertex[self.vertex] += [_update_doc]
        return ctx


class DescendActor(Actor):
    """Actor for processing hierarchical data structures.

    This actor manages the processing of nested data structures by coordinating
    the execution of child actors.

    Attributes:
        key: Optional key for accessing nested data
        _descendants: List of child actor wrappers
    """

    def __init__(self, key: str | None, descendants_kwargs: list, **kwargs):
        """Initialize the descend actor.

        Args:
            key: Optional key for accessing nested data
            descendants_kwargs: List of child actor configurations
            **kwargs: Additional initialization parameters
        """
        self.key = key
        self._descendants: list[ActorWrapper] = []
        for descendant_kwargs in descendants_kwargs:
            self._descendants += [ActorWrapper(**descendant_kwargs, **kwargs)]

    def fetch_important_items(self):
        """Get important items for string representation.

        Returns:
            dict: Dictionary of important items
        """
        sd = self.__dict__
        sm = {k: sd[k] for k in ["key"]}
        return {**sm}

    def add_descendant(self, d: ActorWrapper):
        """Add a child actor wrapper.

        Args:
            d: Actor wrapper to add
        """
        self._descendants += [d]

    def count(self):
        """Get total count of items processed by all descendants.

        Returns:
            int: Total count
        """
        return sum(d.count() for d in self.descendants)

    @property
    def descendants(self) -> list[ActorWrapper]:
        """Get sorted list of descendant actors.

        Returns:
            list[ActorWrapper]: Sorted list of descendant actors
        """
        return sorted(self._descendants, key=lambda x: _NodeTypePriority[type(x.actor)])

    def init_transforms(self, **kwargs):
        """Initialize transforms for all descendants.

        Args:
            **kwargs: Transform initialization parameters
        """
        for an in self.descendants:
            an.init_transforms(**kwargs)

    def finish_init(self, **kwargs):
        """Complete initialization of the descend actor and its descendants.

        Args:
            **kwargs: Additional initialization parameters
        """
        self.vertex_config: VertexConfig = kwargs.get(
            "vertex_config", VertexConfig(vertices=[])
        )

        for an in self.descendants:
            an.finish_init(**kwargs)

        available_fields = set()
        for anw in self.descendants:
            actor = anw.actor
            if isinstance(actor, TransformActor):
                available_fields |= set(list(actor.t.output))

        present_vertices = [
            anw.actor.name
            for anw in self.descendants
            if isinstance(anw.actor, VertexActor)
        ]

        for v in present_vertices:
            available_fields -= set(self.vertex_config.fields(v))

        for v in self.vertex_config.vertex_list:
            # Use field_names property for cleaner set operations
            v_field_names = set(v.field_names)
            intersection = available_fields & v_field_names
            if intersection and v.name not in present_vertices:
                new_descendant = ActorWrapper(vertex=v.name)
                new_descendant.finish_init(**kwargs)
                self.add_descendant(new_descendant)

        logger.debug(
            f"""type, priority: {
                [
                    (t.__name__, _NodeTypePriority[t])
                    for t in (type(x.actor) for x in self.descendants)
                ]
            }"""
        )

    def __call__(self, ctx: ActionContext, lindex: LocationIndex, **kwargs):
        """Process hierarchical data structure.

        Args:
            ctx: Action context
            **kwargs: Additional keyword arguments including 'doc'

        Returns:
            Updated action context

        Raises:
            ValueError: If no document is provided
        """
        doc = kwargs.pop("doc")

        if doc is None:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        if not doc:
            return ctx

        if self.key is not None:
            if isinstance(doc, dict) and self.key in doc:
                doc = doc[self.key]
            else:
                return ctx

        doc_level = doc if isinstance(doc, list) else [doc]

        logger.debug(f"{len(doc_level)}")

        for idoc, sub_doc in enumerate(doc_level):
            logger.debug(f"docs: {idoc + 1}/{len(doc_level)}")
            if isinstance(sub_doc, dict):
                nargs: tuple = tuple()
                kwargs["doc"] = sub_doc
            else:
                nargs = (sub_doc,)

            # down the tree
            extra_step = (idoc,) if self.key is None else (self.key, idoc)
            for j, anw in enumerate(self.descendants):
                logger.debug(
                    f"{type(anw.actor).__name__}: {j + 1}/{len(self.descendants)}"
                )
                ctx = anw(
                    ctx,
                    lindex.extend(extra_step),
                    *nargs,
                    **kwargs,
                )
        return ctx

    def fetch_actors(self, level, edges):
        """Fetch actor information for tree representation.

        Args:
            level: Current level in the actor tree
            edges: List of edges in the actor tree

        Returns:
            tuple: (level, actor_type, string_representation, edges)
        """
        label_current = str(self)
        cname_current = type(self)
        hash_current = hash((level, cname_current, label_current))
        logger.info(f"{hash_current}, {level, cname_current, label_current}")
        props_current = {"label": label_current, "class": cname_current, "level": level}
        for d in self.descendants:
            level_a, cname, label_a, edges_a = d.fetch_actors(level + 1, edges)
            hash_a = hash((level_a, cname, label_a))
            props_a = {"label": label_a, "class": cname, "level": level_a}
            edges = [(hash_current, hash_a, props_current, props_a)] + edges_a
        return level, type(self), str(self), edges


_NodeTypePriority: MappingProxyType[Type[Actor], int] = MappingProxyType(
    {
        DescendActor: 10,
        TransformActor: 20,
        VertexActor: 50,
        EdgeActor: 90,
    }
)


class ActorWrapper:
    """Wrapper class for managing actor instances.

    This class provides a unified interface for creating and managing different types
    of actors, handling initialization and execution.

    Attributes:
        actor: The wrapped actor instance
        vertex_config: Vertex configuration
        edge_config: Edge configuration
    """

    def __init__(self, *args, **kwargs):
        """Initialize the actor wrapper.

        Args:
            *args: Positional arguments for actor initialization
            **kwargs: Keyword arguments for actor initialization

        Raises:
            ValueError: If unable to initialize an actor
        """
        self.actor: Actor
        self.vertex_config: VertexConfig
        self.edge_config: EdgeConfig
        if self._try_init_descend(*args, **kwargs):
            pass
        elif self._try_init_transform(**kwargs):
            pass
        elif self._try_init_vertex(**kwargs):
            pass
        elif self._try_init_edge(**kwargs):
            pass
        else:
            raise ValueError(f"Not able to init ActionNodeWrapper with {kwargs}")

    def init_transforms(self, **kwargs):
        """Initialize transforms for the wrapped actor.

        Args:
            **kwargs: Transform initialization parameters
        """
        self.actor.init_transforms(**kwargs)

    def finish_init(self, **kwargs):
        """Complete initialization of the wrapped actor.

        Args:
            **kwargs: Additional initialization parameters
        """
        kwargs["transforms"]: dict[str, ProtoTransform] = kwargs.get("transforms", {})
        self.actor.init_transforms(**kwargs)

        self.vertex_config = kwargs.get("vertex_config", VertexConfig(vertices=[]))
        kwargs["vertex_config"] = self.vertex_config
        self.edge_config = kwargs.get("edge_config", EdgeConfig())
        kwargs["edge_config"] = self.edge_config
        self.actor.finish_init(**kwargs)

    def count(self):
        """Get count of items processed by the wrapped actor.

        Returns:
            int: Number of items
        """
        return self.actor.count()

    def _try_init_descend(self, *args, **kwargs) -> bool:
        """Try to initialize a descend actor.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            bool: True if successful, False otherwise
        """

        descend_key = kwargs.pop(DESCEND_KEY, None)

        descendants = kwargs.pop("apply", None)
        if descendants is not None:
            if isinstance(descendants, list):
                descendants = descendants
            else:
                descendants = [descendants]
        elif len(args) > 0:
            descendants = list(args)
        else:
            return False
        self.actor = DescendActor(descend_key, descendants_kwargs=descendants, **kwargs)
        return True

    def _try_init_transform(self, **kwargs) -> bool:
        """Try to initialize a transform actor.

        Args:
            **kwargs: Keyword arguments

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.actor = TransformActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_vertex(self, **kwargs) -> bool:
        """Try to initialize a vertex actor.

        Args:
            **kwargs: Keyword arguments

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.actor = VertexActor(**kwargs)
            return True
        except Exception:
            return False

    def _try_init_edge(self, **kwargs) -> bool:
        """Try to initialize an edge actor.

        Args:
            **kwargs: Keyword arguments

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.actor = EdgeActor(**kwargs)
            return True
        except Exception:
            return False

    def __call__(
        self,
        ctx: ActionContext,
        lindex: LocationIndex = LocationIndex(),
        *nargs,
        **kwargs,
    ) -> ActionContext:
        """Execute the wrapped actor.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Updated action context
        """
        ctx = self.actor(ctx, lindex, *nargs, **kwargs)
        return ctx

    def normalize_ctx(self, ctx: ActionContext) -> defaultdict[GraphEntity, list]:
        """Normalize the action context.

        Args:
            ctx: Action context to normalize

        Returns:
            defaultdict[GraphEntity, list]: Normalized context
        """

        for edge_id, edge in self.edge_config.edges_items():
            s, t, _ = edge_id
            edges_ids = [k for k in ctx.acc_global if not isinstance(k, str)]
            if not any(s == sp and t == tp for sp, tp, _ in edges_ids):
                extra_edges = render_edge(
                    edge=edge, vertex_config=self.vertex_config, ctx=ctx
                )
                extra_edges = render_weights(
                    edge,
                    self.vertex_config,
                    ctx.acc_vertex,
                    extra_edges,
                )

                for relation, v in extra_edges.items():
                    ctx.acc_global[s, t, relation] += v

        for vertex_name, dd in ctx.acc_vertex.items():
            for lindex, vertex_list in dd.items():
                vertex_list = [x.vertex for x in vertex_list]
                vertex_list_updated = merge_doc_basis(
                    vertex_list,
                    tuple(self.vertex_config.index(vertex_name).fields),
                )
                vertex_list_updated = pick_unique_dict(vertex_list_updated)

                ctx.acc_global[vertex_name] += vertex_list_updated

        ctx = add_blank_collections(ctx, self.vertex_config)

        return ctx.acc_global

    @classmethod
    def from_dict(cls, data: dict | list):
        """Create an actor wrapper from a dictionary or list.

        Args:
            data: Dictionary or list containing actor configuration

        Returns:
            ActorWrapper: New actor wrapper instance
        """
        if isinstance(data, list):
            return cls(*data)
        else:
            return cls(**data)

    def assemble_tree(self, fig_path: Optional[Path] = None):
        """Assemble and optionally visualize the actor tree.

        Args:
            fig_path: Optional path to save the visualization

        Returns:
            Optional[networkx.MultiDiGraph]: Graph representation of the actor tree
        """
        _, _, _, edges = self.fetch_actors(0, [])
        logger.info(f"{len(edges)}")
        try:
            import networkx as nx
        except ImportError as e:
            logger.error(f"not able to import networks {e}")
            return None
        nodes = {}
        g = nx.MultiDiGraph()
        for ha, hb, pa, pb in edges:
            nodes[ha] = pa
            nodes[hb] = pb
        from graflo.plot.plotter import fillcolor_palette

        map_class2color = {
            DescendActor: fillcolor_palette["green"],
            VertexActor: "orange",
            EdgeActor: fillcolor_palette["violet"],
            TransformActor: fillcolor_palette["blue"],
        }

        for n, props in nodes.items():
            nodes[n]["fillcolor"] = map_class2color[props["class"]]
            nodes[n]["style"] = "filled"
            nodes[n]["color"] = "brown"

        edges = [(ha, hb) for ha, hb, _, _ in edges]
        g.add_edges_from(edges)
        g.add_nodes_from(nodes.items())

        if fig_path is not None:
            ag = nx.nx_agraph.to_agraph(g)
            ag.draw(
                fig_path,
                "pdf",
                prog="dot",
            )
            return None
        else:
            return g

    def fetch_actors(self, level, edges):
        """Fetch actor information for tree representation.

        Args:
            level: Current level in the actor tree
            edges: List of edges in the actor tree

        Returns:
            tuple: (level, actor_type, string_representation, edges)
        """
        return self.actor.fetch_actors(level, edges)
