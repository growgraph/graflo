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
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Type

from graflo.architecture.actor_config import (
    ActorConfig,
    DescendActorConfig,
    EdgeActorConfig,
    TransformActorConfig,
    VertexActorConfig,
    VertexRouterActorConfig,
    parse_root_config,
    normalize_actor_step,
    validate_actor_step,
)

from graflo.architecture.actor_util import (
    add_blank_collections,
)
from graflo.architecture.assemble import assemble_edges
from graflo.architecture.edge import Edge, EdgeConfig
from graflo.architecture.onto import (
    ActionContext,
    AssemblyContext,
    EdgeId,
    ExtractionContext,
    GraphEntity,
    LocationIndex,
    TransformPayload,
    VertexRep,
)
from graflo.architecture.transform import ProtoTransform, Transform
from graflo.architecture.vertex import (
    VertexConfig,
)
from graflo.onto import ExpressionFlavor
from graflo.util.merge import (
    merge_doc_basis,
)
from graflo.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


class ActorConstants:
    """Constants used throughout the actor system.

    This class centralizes magic strings and constants to improve
    maintainability and make the codebase more self-documenting.
    """

    # Key used for accessing nested data in DescendActor
    DESCEND_KEY: str = "key"

    # Prefix for transformed values in vertex processing
    # Format: f"{DRESSING_TRANSFORMED_VALUE_KEY}#{index}"
    DRESSING_TRANSFORMED_VALUE_KEY: str = "__value__"


@dataclass(slots=True)
class ActorInitContext:
    """Typed initialization state shared across actor tree."""

    vertex_config: VertexConfig
    edge_config: EdgeConfig
    transforms: dict[str, ProtoTransform]
    infer_edges: bool = True
    infer_edge_only: set[EdgeId] = field(default_factory=set)
    infer_edge_except: set[EdgeId] = field(default_factory=set)


class Actor(ABC):
    """Abstract base class for all actors in the system.

    Actors are the fundamental processing units in the graph transformation system.
    Each actor type implements specific functionality for processing graph data.

    Attributes:
        None (abstract class)
    """

    @abstractmethod
    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs, **kwargs
    ) -> ExtractionContext:
        """Execute the actor's main processing logic.

        Args:
            ctx: The action context containing the current processing state
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            Updated action context
        """
        pass

    def fetch_important_items(self) -> dict[str, Any]:
        """Get a dictionary of important items for string representation.

        Returns:
            dict[str, Any]: Dictionary of important items
        """
        return {}

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Complete initialization of the actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        pass

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        """Initialize transformations for the actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        pass

    def count(self) -> int:
        """Get the count of items processed by this actor.

        Returns:
            int: Number of items
        """
        return 1

    def references_vertices(self) -> set[str]:
        """Return vertex names this actor references."""
        return set()

    def _filter_items(self, items: dict[str, Any]) -> dict[str, Any]:
        """Filter out None and empty items.

        Args:
            items: Dictionary of items to filter

        Returns:
            dict[str, Any]: Filtered dictionary
        """
        return {k: v for k, v in items.items() if v is not None and v}

    def _stringify_items(self, items: dict[str, Any]) -> dict[str, str]:
        """Convert items to string representation.

        Args:
            items: Dictionary of items to stringify

        Returns:
            dict[str, str]: Dictionary with stringified values
        """
        return {
            k: ", ".join(list(v)) if isinstance(v, (tuple, list)) else str(v)
            for k, v in items.items()
        }

    def _fetch_items_from_dict(self, keys: tuple[str, ...]) -> dict[str, Any]:
        """Helper method to extract items from instance dict for string representation.

        Args:
            keys: Tuple of attribute names to extract

        Returns:
            dict[str, Any]: Dictionary of extracted items
        """
        return {k: self.__dict__[k] for k in keys if k in self.__dict__}

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

    def __init__(self, config: VertexActorConfig):
        """Initialize the vertex actor from config."""
        self.name = config.vertex
        self.from_doc: dict[str, str] | None = config.from_doc
        self.keep_fields: tuple[str, ...] | None = (
            tuple(config.keep_fields) if config.keep_fields else None
        )
        self.vertex_config: VertexConfig

    @classmethod
    def from_config(cls, config: VertexActorConfig) -> VertexActor:
        """Create a VertexActor from a VertexActorConfig."""
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        """Get important items for string representation.

        Returns:
            dict[str, Any]: Dictionary of important items
        """
        return self._fetch_items_from_dict(("name", "from_doc", "keep_fields"))

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Complete initialization of the vertex actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        self.vertex_config = init_ctx.vertex_config

    def _filter_and_aggregate_vertex_docs(
        self, docs: list[dict[str, Any]], doc: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Filter and aggregate vertex documents based on vertex filters.

        Args:
            docs: List of vertex documents to filter
            doc: Original document for filter context

        Returns:
            list[dict]: Filtered list of vertex documents
        """
        filters = self.vertex_config.filters(self.name)
        return [
            _doc
            for _doc in docs
            if all(cfilter(kind=ExpressionFlavor.PYTHON, **_doc) for cfilter in filters)
        ]

    def _extract_vertex_doc_from_transformed_item(
        self,
        item: Any,
        vertex_keys: tuple[str, ...],
        index_keys: tuple[str, ...],
    ) -> dict[str, Any]:
        """Extract vertex document from a transformed payload.

        Args:
            item: Transform payload (new typed payload or legacy dict)
            vertex_keys: Tuple of vertex field keys
            index_keys: Tuple of vertex identity field keys

        Returns:
            dict: Extracted vertex document
        """
        if isinstance(item, TransformPayload):
            doc: dict[str, Any] = {}
            consumed_named: set[str] = set()
            for k, v in item.named.items():
                if k in vertex_keys and v is not None:
                    doc[k] = v
                    consumed_named.add(k)
            for j, value in enumerate(item.positional):
                if j >= len(index_keys):
                    break
                doc[index_keys[j]] = value
            # Keep old semantics: values are consumed once by the first matching vertex.
            for key in consumed_named:
                item.named.pop(key, None)
            if item.positional:
                item.positional = ()
            return doc

        if isinstance(item, dict):
            # Legacy compatibility path (magic-key payloads)
            doc = {}
            value_keys = sorted(
                (
                    k
                    for k in item
                    if k.startswith(ActorConstants.DRESSING_TRANSFORMED_VALUE_KEY)
                ),
                key=lambda x: int(x.rsplit("#", 1)[-1]),
            )
            for j, vkey in enumerate(value_keys):
                if j >= len(index_keys):
                    break
                doc[index_keys[j]] = item.pop(vkey)
            for vkey in vertex_keys:
                if vkey not in doc and vkey in item and item[vkey] is not None:
                    doc[vkey] = item.pop(vkey)
            return doc

        return {}

    def _process_transformed_items(
        self,
        ctx: ExtractionContext,
        lindex: LocationIndex,
        doc: dict[str, Any],
        vertex_keys: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """Process items from buffer_transforms.

        Args:
            ctx: Action context
            lindex: Location index
            doc: Document being processed
            vertex_keys: Tuple of vertex field keys

        Returns:
            list[dict]: List of processed documents
        """
        index_keys = tuple(self.vertex_config.index(self.name).fields)
        payloads = ctx.buffer_transforms[lindex]
        extracted_docs = [
            self._extract_vertex_doc_from_transformed_item(
                item, vertex_keys, index_keys
            )
            for item in payloads
        ]
        ctx.buffer_transforms[lindex] = [
            item
            for item in payloads
            if not (
                isinstance(item, TransformPayload)
                and not item.named
                and not item.positional
            )
            and not (isinstance(item, dict) and not item)
        ]

        return self._filter_and_aggregate_vertex_docs(extracted_docs, doc)

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        """Process vertex data.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments including 'doc'

        Returns:
            ExtractionContext: Updated extraction context
        """
        doc: dict[str, Any] = kwargs.get("doc", {})

        vertex_keys_list = self.vertex_config.fields_names(self.name)
        # Convert to tuple of strings for type compatibility
        vertex_keys: tuple[str, ...] = tuple(vertex_keys_list)

        agg = []
        if self.from_doc:
            projected = {v_f: doc.get(d_f) for v_f, d_f in self.from_doc.items()}
            if any(v is not None for v in projected.values()):
                agg.append(projected)

        # Process transformed items
        agg.extend(self._process_transformed_items(ctx, lindex, doc, vertex_keys))

        # Add passthrough items from doc
        remaining_keys = set(vertex_keys) - set().union(*[d.keys() for d in agg])
        passthrough_doc = {k: doc.pop(k) for k in remaining_keys if k in doc}
        if passthrough_doc:
            agg.append(passthrough_doc)

        # Merge and create vertex representations
        merged = merge_doc_basis(
            agg, index_keys=tuple(self.vertex_config.index(self.name).fields)
        )

        obs_ctx = {q: w for q, w in doc.items() if not isinstance(w, (dict, list))}
        for m in merged:
            vertex_rep = VertexRep(vertex=m, ctx=obs_ctx)
            ctx.acc_vertex[self.name][lindex].append(vertex_rep)
            ctx.record_vertex_observation(
                vertex_name=self.name,
                location=lindex,
                vertex=vertex_rep.vertex,
                ctx=vertex_rep.ctx,
            )
        return ctx

    def references_vertices(self) -> set[str]:
        return {self.name}


class EdgeActor(Actor):
    """Actor for processing edge data.

    This actor handles the creation and transformation of edges between vertices,
    including weight calculations and relationship management.

    Attributes:
        edge: Edge configuration
        vertex_config: Vertex configuration
    """

    def __init__(self, config: EdgeActorConfig):
        """Initialize the edge actor from config."""
        kwargs = config.model_dump(by_alias=False, exclude_none=True)
        kwargs.pop("type", None)
        self.edge = Edge.from_dict(kwargs)
        self.vertex_config: VertexConfig

    @classmethod
    def from_config(cls, config: EdgeActorConfig) -> EdgeActor:
        """Create an EdgeActor from an EdgeActorConfig."""
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        """Get important items for string representation.

        Returns:
            dict[str, Any]: Dictionary of important items
        """
        return {
            k: self.edge.__dict__[k]
            for k in ["source", "target", "match_source", "match_target"]
            if k in self.edge.__dict__
        }

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Complete initialization of the edge actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        self.vertex_config = init_ctx.vertex_config
        if self.vertex_config is not None:
            init_ctx.edge_config.update_edges(
                self.edge, vertex_config=self.vertex_config
            )

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        """Process edge data.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            ExtractionContext: Updated extraction context
        """

        ctx.edge_requests.append((self.edge, lindex))
        ctx.record_edge_intent(edge=self.edge, location=lindex)
        return ctx

    def references_vertices(self) -> set[str]:
        return {self.edge.source, self.edge.target}


class TransformActor(Actor):
    """Actor for applying transformations to data.

    This actor handles the application of transformations to input data, supporting
    both simple and complex transformation scenarios.

    Attributes:
        _kwargs: Config dump for init_transforms (module, foo, input, output)
        transforms: Dictionary of available transforms
        name: Transform name
        params: Transform parameters
        t: Transform instance
    """

    def __init__(self, config: TransformActorConfig):
        """Initialize the transform actor from config."""
        self._kwargs = config.model_dump(by_alias=True)
        self.transforms = {}
        self.name = config.name
        self.params = config.params
        self.t: Transform = Transform(
            map=config.map or {},
            name=config.name,
            params=config.params,
            module=config.module,
            foo=config.foo,
            input=tuple(config.input) if config.input else (),
            output=tuple(config.output) if config.output else (),
            dress=config.dress,
        )

    def fetch_important_items(self) -> dict[str, Any]:
        """Get important items for string representation.

        Returns:
            dict[str, Any]: Dictionary of important items
        """
        items = self._fetch_items_from_dict(("name",))
        items.update({"t.input": self.t.input, "t.output": self.t.output})
        return items

    @classmethod
    def from_config(cls, config: TransformActorConfig) -> TransformActor:
        """Create a TransformActor from a TransformActorConfig."""
        return cls(config)

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        """Initialize available transforms.

        Args:
            init_ctx: Shared typed initialization context
        """
        self.transforms = init_ctx.transforms
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
        except (TypeError, ValueError, AttributeError) as e:
            logger.debug("Failed to initialize ProtoTransform: %s", e)
            pass

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Complete initialization of the transform actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        self.transforms = init_ctx.transforms

        if self.name is not None:
            pt = self.transforms.get(self.name, None)
            if pt is not None:
                next_params = self.t.params
                next_input = self.t.input
                next_output = self.t.output

                if pt.params and not self.t.params:
                    next_params = pt.params
                    if (
                        pt.input
                        and not self.t.input
                        and pt.output
                        and not self.t.output
                    ):
                        next_input = pt.input
                        next_output = pt.output

                # Rebuild a new Transform instance rather than mutating private attrs.
                self.t = Transform(
                    fields=self.t.fields,
                    map=self.t.map,
                    dress=self.t.dress,
                    name=self.t.name,
                    module=pt.module,
                    foo=pt.foo,
                    params=next_params,
                    input=next_input,
                    output=next_output,
                )

    def _extract_doc(self, nargs: tuple[Any, ...], **kwargs: Any) -> dict[str, Any]:
        """Extract document from arguments.

        Args:
            nargs: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            dict[str, Any]: Extracted document

        Raises:
            ValueError: If no document is provided
        """
        if kwargs:
            doc: dict[str, Any] | None = kwargs.get("doc")
        elif nargs:
            doc = nargs[0]
        else:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        if doc is None:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        return doc

    def _format_transform_result(self, result: Any) -> TransformPayload:
        """Format transformation result into typed payload.

        Args:
            result: Result from transform

        Returns:
            TransformPayload: Typed transform payload
        """
        return TransformPayload.from_result(result)

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        """Apply transformation to input data.

        Args:
            ctx: Action context
            *nargs: Additional positional arguments
            **kwargs: Additional keyword arguments including 'doc'

        Returns:
            ExtractionContext: Updated extraction context

        Raises:
            ValueError: If no document is provided
        """
        logger.debug("transforms : %s %s", id(self.transforms), len(self.transforms))

        doc = self._extract_doc(nargs, **kwargs)

        transform_result = self.t(doc)

        _update_doc = self._format_transform_result(transform_result)

        ctx.buffer_transforms[lindex].append(_update_doc)
        ctx.record_transform_observation(location=lindex, payload=_update_doc)
        return ctx

    def references_vertices(self) -> set[str]:
        return set()


class DescendActor(Actor):
    """Actor for processing hierarchical data structures.

    This actor manages the processing of nested data structures by coordinating
    the execution of child actors.

    Attributes:
        key: Optional key for accessing nested data
        any_key: If True, processes all keys in a dictionary instead of a specific key
        _descendants: List of child actor wrappers
    """

    def __init__(
        self,
        key: str | None,
        any_key: bool = False,
        *,
        _descendants: list[ActorWrapper] | None = None,
    ):
        """Initialize the descend actor.

        Args:
            key: Optional key for accessing nested data. If provided, only this key
                will be processed. Mutually exclusive with `any_key`.
            any_key: If True, processes all keys in a dictionary instead of a specific key.
            _descendants: Pre-built list of child ActorWrappers (from config).
        """
        self.key = key
        self.any_key = any_key
        self._descendants: list[ActorWrapper] = (
            list(_descendants) if _descendants else []
        )
        self._descendants_sorted = True
        self._descendants.sort(key=lambda x: _NodeTypePriority[type(x.actor)])

    def fetch_important_items(self):
        """Get important items for string representation.

        Returns:
            dict: Dictionary of important items
        """
        items = self._fetch_items_from_dict(("key",))
        if self.any_key:
            items["any_key"] = True
        return items

    def add_descendant(self, d: ActorWrapper):
        """Add a child actor wrapper.

        Args:
            d: Actor wrapper to add
        """
        self._descendants.append(d)
        self._descendants_sorted = False

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
        if not self._descendants_sorted:
            self._descendants.sort(key=lambda x: _NodeTypePriority[type(x.actor)])
            self._descendants_sorted = True
        return self._descendants

    @classmethod
    def from_config(cls, config: DescendActorConfig) -> DescendActor:
        """Create a DescendActor from a DescendActorConfig."""
        wrappers = [ActorWrapper.from_config(c) for c in config.pipeline]
        return cls(key=config.key, any_key=config.any_key, _descendants=wrappers)

    def _infer_vertex_descendants_from_transforms(
        self, init_ctx: ActorInitContext
    ) -> None:
        """Infer implicit VertexActors from untargeted transform outputs.

        This is used when a pipeline contains transform map steps without explicit
        target vertices and no explicit vertex actors at the same level.
        """
        if any(isinstance(an.actor, VertexActor) for an in self.descendants):
            return

        transform_output_fields: set[str] = set()
        for an in self.descendants:
            if isinstance(an.actor, TransformActor):
                transform_output_fields.update(str(k) for k in an.actor.t.map.keys())

        if not transform_output_fields:
            return

        inferred_vertices: list[str] = []
        for vertex_name in sorted(init_ctx.vertex_config.vertex_set):
            identity_fields = {
                str(field) for field in init_ctx.vertex_config.index(vertex_name).fields
            }
            if identity_fields and identity_fields.issubset(transform_output_fields):
                inferred_vertices.append(vertex_name)

        if not inferred_vertices:
            return

        existing_targets: set[str] = set()
        for an in self.descendants:
            existing_targets.update(
                str(v) for v in an.actor.references_vertices() if v is not None
            )
        for vertex_name in inferred_vertices:
            if vertex_name in existing_targets:
                continue
            self.add_descendant(
                ActorWrapper.from_config(VertexActorConfig(vertex=vertex_name))
            )
            logger.debug(
                "DescendActor: inferred implicit VertexActor(%s) from untargeted transform fields %s",
                vertex_name,
                sorted(transform_output_fields),
            )

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        """Initialize transforms for all descendants.

        Args:
            init_ctx: Shared typed initialization context
        """
        for an in self.descendants:
            an.init_transforms(init_ctx)

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Complete initialization of the descend actor and its descendants.

        Args:
            init_ctx: Shared typed initialization context
        """
        self.vertex_config = init_ctx.vertex_config
        self._infer_vertex_descendants_from_transforms(init_ctx)

        for an in self.descendants:
            an.finish_init(init_ctx)

    def _expand_document(self, doc: dict | list) -> list[tuple[str | None, Any]]:
        """Expand document into list of (key, item) tuples for processing.

        Args:
            doc: Document to expand

        Returns:
            list[tuple[str | None, Any]]: List of (key, item) tuples
        """
        if self.key is not None:
            if isinstance(doc, dict) and self.key in doc:
                items = doc[self.key]
                aux = items if isinstance(items, list) else [items]
                return [(self.key, item) for item in aux]
            return []
        elif self.any_key:
            if isinstance(doc, dict):
                result = []
                for key, items in doc.items():
                    aux = items if isinstance(items, list) else [items]
                    result.extend([(key, item) for item in aux])
                return result
            return []
        else:
            # Process as list or single item
            if isinstance(doc, list):
                return [(None, item) for item in doc]
            return [(None, doc)]

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs, **kwargs: Any
    ) -> ExtractionContext:
        """Process hierarchical data structure.

        Args:
            ctx: Action context
            **kwargs: Additional keyword arguments including 'doc'

        Returns:
            ExtractionContext: Updated extraction context

        Raises:
            ValueError: If no document is provided
        """
        doc: Any = kwargs.pop("doc")

        if doc is None:
            raise ValueError(f"{type(self).__name__}: doc should be provided")

        if not doc:
            return ctx

        doc_expanded = self._expand_document(doc)
        if not doc_expanded:
            return ctx

        logger.debug("Expanding %s items", len(doc_expanded))

        for idoc, (key, sub_doc) in enumerate(doc_expanded):
            logger.debug("Processing item %s/%s", idoc + 1, len(doc_expanded))
            if isinstance(sub_doc, dict):
                nargs: tuple[Any, ...] = tuple()
                # Create new dict to avoid mutating original kwargs
                child_kwargs = {**kwargs, "doc": sub_doc}
            else:
                nargs = (sub_doc,)
                # Use original kwargs when passing non-dict as positional arg
                child_kwargs = kwargs

            # Extend location index for nested processing
            extra_step = (idoc,) if key is None else (key, idoc)
            for j, anw in enumerate(self.descendants):
                logger.debug(
                    f"{type(anw.actor).__name__}: {j + 1}/{len(self.descendants)}"
                )
                ctx = anw(
                    ctx,
                    lindex.extend(extra_step),
                    *nargs,
                    **child_kwargs,
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
        logger.info(
            "%s, %s",
            hash_current,
            (level, cname_current, label_current),
        )
        props_current = {"label": label_current, "class": cname_current, "level": level}
        for d in self.descendants:
            level_a, cname, label_a, edges_a = d.fetch_actors(level + 1, edges)
            hash_a = hash((level_a, cname, label_a))
            props_a = {"label": label_a, "class": cname, "level": level_a}
            edges = [(hash_current, hash_a, props_current, props_a)] + edges_a
        return level, type(self), str(self), edges


class VertexRouterActor(Actor):
    """Routes documents to the correct VertexActor based on a type field.

    Maintains an internal ``dict[str, ActorWrapper]`` mapping vertex type names
    to pre-initialised VertexActor wrappers, giving O(1) dispatch per document
    instead of iterating over all known vertex types.

    On ``__call__``:

    1. Read ``doc[type_field]`` to determine the vertex type name.
    2. Look up ``_vertex_actors[vtype]`` for the matching wrapper.
    3. Strip *prefix* from field keys (or apply *field_map*) to build a sub-doc.
    4. Delegate to the looked-up wrapper.

    Attributes:
        type_field: Document field whose value names the target vertex type.
        prefix: Optional prefix to strip from field keys.
        field_map: Optional explicit rename mapping (original_key -> vertex_key).
    """

    def __init__(self, config: VertexRouterActorConfig):
        """Initialise from config."""
        self.type_field = config.type_field
        self.prefix = config.prefix
        self.field_map = config.field_map
        self._vertex_actors: dict[str, ActorWrapper] = {}
        self._init_kwargs: dict[str, Any] = {}
        self.vertex_config: VertexConfig = VertexConfig(vertices=[])

    @classmethod
    def from_config(cls, config: VertexRouterActorConfig) -> VertexRouterActor:
        """Create a VertexRouterActor from a VertexRouterActorConfig."""
        return cls(config)

    def fetch_important_items(self) -> dict[str, Any]:
        """Get important items for string representation."""
        items: dict[str, Any] = {"type_field": self.type_field}
        if self.prefix:
            items["prefix"] = self.prefix
        if self.field_map:
            items["field_map"] = self.field_map
        items["vertex_types"] = sorted(self._vertex_actors.keys())
        return items

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        """Store initialization state for on-demand wrapper creation."""
        self.vertex_config = init_ctx.vertex_config
        self._init_kwargs = {"init_ctx": init_ctx}
        self._vertex_actors.clear()

    def _get_or_create_wrapper(self, vertex_type: str) -> ActorWrapper | None:
        if vertex_type not in self.vertex_config.vertex_set:
            return None
        wrapper = self._vertex_actors.get(vertex_type)
        if wrapper is not None:
            return wrapper

        wrapper = ActorWrapper.from_config(VertexActorConfig(vertex=vertex_type))
        wrapper.finish_init(**self._init_kwargs)
        self._vertex_actors[vertex_type] = wrapper
        logger.debug(
            "VertexRouterActor: lazily registered VertexActor(%s) for type_field=%s",
            vertex_type,
            self.type_field,
        )
        return wrapper

    def count(self) -> int:
        """Total actors managed by this router (self + all wrapped vertex actors)."""
        return 1 + sum(w.count() for w in self._vertex_actors.values())

    def _extract_sub_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Build the vertex sub-document from *doc*.

        If *prefix* is set, extracts and strips prefixed keys.
        If *field_map* is set, renames keys according to the map.
        Otherwise returns *doc* unchanged.
        """
        if self.prefix:
            return {
                k[len(self.prefix) :]: v
                for k, v in doc.items()
                if k.startswith(self.prefix)
            }
        if self.field_map:
            return {
                new_key: doc[old_key]
                for old_key, new_key in self.field_map.items()
                if old_key in doc
            }
        return doc

    def __call__(
        self, ctx: ExtractionContext, lindex: LocationIndex, *nargs: Any, **kwargs: Any
    ) -> ExtractionContext:
        """Route the document to the matching VertexActor.

        Args:
            ctx: Action context.
            lindex: Current location index.
            **kwargs: Must contain ``doc``.

        Returns:
            Updated extraction context.
        """
        doc: dict[str, Any] = kwargs.get("doc", {})
        vtype = doc.get(self.type_field)
        if vtype is None:
            logger.debug(
                "VertexRouterActor: type_field '%s' not in doc, skipping",
                self.type_field,
            )
            return ctx

        wrapper = self._get_or_create_wrapper(vtype)
        if wrapper is None:
            logger.debug(
                "VertexRouterActor: vertex type '%s' (from field '%s') "
                "not in VertexConfig, skipping",
                vtype,
                self.type_field,
            )
            return ctx

        sub_doc = self._extract_sub_doc(doc)
        if not sub_doc:
            return ctx

        return wrapper(ctx, lindex, doc=sub_doc)


_NodeTypePriority: MappingProxyType[Type[Actor], int] = MappingProxyType(
    {
        DescendActor: 10,
        TransformActor: 20,
        VertexRouterActor: 30,
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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the actor wrapper from config only.

        Accepts the same shapes as parse_root_config:
        - Single step dict or **kwargs: e.g. ActorWrapper(vertex="user")
        - Pipeline: ActorWrapper(pipeline=[...]) or ActorWrapper(*list_of_steps)

        Raises:
            ValueError: If input does not validate as ActorConfig
        """
        config = parse_root_config(*args, **kwargs)
        w = ActorWrapper.from_config(config)
        self.actor = w.actor
        self.vertex_config = w.vertex_config
        self.edge_config = w.edge_config
        self.infer_edges = w.infer_edges
        self.infer_edge_only = w.infer_edge_only
        self.infer_edge_except = w.infer_edge_except

    def init_transforms(
        self, init_ctx: ActorInitContext | None = None, **kwargs: Any
    ) -> None:
        """Initialize transforms for the wrapped actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        if init_ctx is None:
            init_ctx = ActorInitContext(
                vertex_config=kwargs.get("vertex_config", VertexConfig(vertices=[])),
                edge_config=kwargs.get("edge_config", EdgeConfig()),
                transforms=kwargs.get("transforms", {}),
                infer_edges=kwargs.get("infer_edges", self.infer_edges),
                infer_edge_only=set(
                    kwargs.get("infer_edge_only", self.infer_edge_only)
                ),
                infer_edge_except=set(
                    kwargs.get("infer_edge_except", self.infer_edge_except)
                ),
            )
        self.actor.init_transforms(init_ctx)

    def finish_init(
        self, init_ctx: ActorInitContext | None = None, **kwargs: Any
    ) -> None:
        """Complete initialization of the wrapped actor.

        Args:
            init_ctx: Shared typed initialization context
        """
        if init_ctx is None:
            init_ctx = ActorInitContext(
                vertex_config=kwargs.get("vertex_config", VertexConfig(vertices=[])),
                edge_config=kwargs.get("edge_config", EdgeConfig()),
                transforms=kwargs.get("transforms", {}),
                infer_edges=kwargs.get("infer_edges", self.infer_edges),
                infer_edge_only=set(
                    kwargs.get("infer_edge_only", self.infer_edge_only)
                ),
                infer_edge_except=set(
                    kwargs.get("infer_edge_except", self.infer_edge_except)
                ),
            )
        self.actor.init_transforms(init_ctx)
        self.vertex_config = init_ctx.vertex_config
        self.edge_config = init_ctx.edge_config
        self.infer_edges = init_ctx.infer_edges
        self.infer_edge_only = set(init_ctx.infer_edge_only)
        self.infer_edge_except = set(init_ctx.infer_edge_except)
        self.actor.finish_init(init_ctx)

    def count(self):
        """Get count of items processed by the wrapped actor.

        Returns:
            int: Number of items
        """
        return self.actor.count()

    @classmethod
    def from_config(cls, config: ActorConfig) -> ActorWrapper:
        """Create an ActorWrapper from a validated ActorConfig (Pydantic model)."""
        if isinstance(config, VertexActorConfig):
            actor = VertexActor.from_config(config)
        elif isinstance(config, TransformActorConfig):
            actor = TransformActor.from_config(config)
        elif isinstance(config, EdgeActorConfig):
            actor = EdgeActor.from_config(config)
        elif isinstance(config, DescendActorConfig):
            actor = DescendActor.from_config(config)
        elif isinstance(config, VertexRouterActorConfig):
            actor = VertexRouterActor.from_config(config)
        else:
            raise ValueError(
                f"Expected VertexActorConfig, TransformActorConfig, EdgeActorConfig, "
                f"DescendActorConfig, or VertexRouterActorConfig, got {type(config)}"
            )
        wrapper = cls.__new__(cls)
        wrapper.actor = actor
        wrapper.vertex_config = VertexConfig(vertices=[])
        wrapper.edge_config = EdgeConfig()
        wrapper.infer_edges = True
        wrapper.infer_edge_only = set()
        wrapper.infer_edge_except = set()
        return wrapper

    @classmethod
    def _from_step(cls, step: dict[str, Any]) -> ActorWrapper:
        """Build an ActorWrapper from a single pipeline step dict (normalize + validate + from_config)."""
        config = validate_actor_step(normalize_actor_step(step))
        return cls.from_config(config)

    def __call__(
        self,
        ctx: ExtractionContext,
        lindex: LocationIndex = LocationIndex(),
        *nargs: Any,
        **kwargs: Any,
    ) -> ExtractionContext:
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

    def assemble(
        self, ctx: ExtractionContext | AssemblyContext | ActionContext
    ) -> defaultdict[GraphEntity, list]:
        """Assemble final graph entities from extracted context.

        Args:
            ctx: Extraction or assembly context

        Returns:
            defaultdict[GraphEntity, list]: Assembled graph entities
        """
        if isinstance(ctx, AssemblyContext):
            assembly_ctx = ctx
        else:
            assembly_ctx = AssemblyContext.from_extraction(ctx)
        assemble_edges(
            ctx=assembly_ctx,
            vertex_config=self.vertex_config,
            edge_config=self.edge_config,
            edge_greedy=self.infer_edges,
            infer_edge_only=self.infer_edge_only,
            infer_edge_except=self.infer_edge_except,
        )

        for vertex_name, dd in assembly_ctx.acc_vertex.items():
            for lindex, vertex_list in dd.items():
                vertex_list = [x.vertex for x in vertex_list]
                vertex_list_updated = merge_doc_basis(
                    vertex_list,
                    tuple(self.vertex_config.index(vertex_name).fields),
                )
                vertex_list_updated = pick_unique_dict(vertex_list_updated)

                assembly_ctx.acc_global[vertex_name] += vertex_list_updated

        assembly_ctx = add_blank_collections(assembly_ctx, self.vertex_config)

        if isinstance(ctx, ActionContext):
            ctx.acc_global = assembly_ctx.acc_global
            return ctx.acc_global
        return assembly_ctx.acc_global

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

    def assemble_tree(self, fig_path: Path | None = None):
        """Assemble and optionally visualize the actor tree.

        Args:
            fig_path: Optional path to save the visualization

        Returns:
            networkx.MultiDiGraph | None: Graph representation of the actor tree
        """
        _, _, _, edges = self.fetch_actors(0, [])
        logger.info("%s", len(edges))
        try:
            import networkx as nx
        except ImportError as e:
            logger.error("not able to import networks %s", e)
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

    def collect_actors(self) -> list[Actor]:
        """Collect all actors from the actor tree.

        Traverses the entire actor tree and collects all actor instances,
        including nested actors within DescendActor.

        Returns:
            list[Actor]: List of all actors in the tree
        """
        actors = [self.actor]
        if isinstance(self.actor, DescendActor):
            for descendant in self.actor.descendants:
                actors.extend(descendant.collect_actors())
        return actors

    def find_descendants(
        self,
        predicate: Callable[[ActorWrapper], bool] | None = None,
        *,
        actor_type: type[Actor] | None = None,
        **attr_in: Any,
    ) -> list[ActorWrapper]:
        """Find all descendant ActorWrappers matching the given criteria.

        Traverses the actor tree and returns every ActorWrapper whose wrapped
        actor matches. You can use a custom predicate, or filter by actor type
        and attribute membership in sets.

        Args:
            predicate: Optional callable(ActorWrapper) -> bool. If given, only
                descendants for which predicate returns True are included.
            actor_type: If given, only descendants whose wrapped actor is an
                instance of this type are included (e.g. VertexActor,
                TransformActor).
            **attr_in: Attribute filters. Each key is an attribute name on the
                wrapped actor; the value must be a set. A descendant is included
                only if getattr(actor, key, None) is in that set. Examples:
                name={"user", "product"} for VertexActor,
                from_doc for VertexActor with projection.

        Returns:
            list[ActorWrapper]: All matching descendants in the tree.

        Example:
            >>> # All VertexActor descendants with name in {"user", "product"}
            >>> wrappers.find_descendants(actor_type=VertexActor, name={"user", "product"})
            >>> # Custom predicate
            >>> wrappers.find_descendants(predicate=lambda w: isinstance(w.actor, VertexActor) and w.actor.name == "user")
        """
        if predicate is None:

            def _predicate(w: ActorWrapper) -> bool:
                if actor_type is not None and not isinstance(w.actor, actor_type):
                    return False
                for attr, allowed in attr_in.items():
                    if allowed is None:
                        continue
                    val = getattr(w.actor, attr, None)
                    if val not in allowed:
                        return False
                return True

            predicate = _predicate

        result: list[ActorWrapper] = []
        if isinstance(self.actor, DescendActor):
            for d in self.actor.descendants:
                if predicate(d):
                    result.append(d)
                result.extend(d.find_descendants(predicate=predicate))
        return result

    def remove_descendants_if(self, predicate: Callable[[ActorWrapper], bool]) -> None:
        """Remove descendants for which predicate returns True.

        Mutates the tree in place: for each DescendActor, filters its
        descendants to exclude wrappers matching the predicate, after
        recursing into each descendant.  Intermediate DescendActor
        wrappers that become empty after pruning are also removed.

        Args:
            predicate: Callable(ActorWrapper) -> bool. Descendants for
                which this returns True are removed from the tree.
        """
        if isinstance(self.actor, DescendActor):
            for d in list(self.actor.descendants):
                d.remove_descendants_if(predicate=predicate)
            self.actor._descendants[:] = [
                d
                for d in self.actor.descendants
                if not predicate(d)
                and not (isinstance(d.actor, DescendActor) and d.count() == 0)
            ]
