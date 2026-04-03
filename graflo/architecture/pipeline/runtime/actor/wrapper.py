"""Actor wrapper for managing actor instances and assembly."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from ..assemble import assemble_edges
from .base import Actor, ActorInitContext
from .config import (
    ActorConfig,
    DescendActorConfig,
    EdgeActorConfig,
    EdgeRouterActorConfig,
    TransformActorConfig,
    VertexActorConfig,
    VertexRouterActorConfig,
    parse_root_config,
    normalize_actor_step,
    validate_actor_step,
)
from .edge_render import add_blank_collections
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.graph_types import (
    ActionContext,
    AssemblyContext,
    EdgeId,
    ExtractionContext,
    GraphEntity,
    LocationIndex,
)
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import DBType
from graflo.util.merge import merge_doc_basis
from graflo.util.transform import pick_unique_dict

from .descend import DescendActor
from .edge import EdgeActor
from .edge_router import EdgeRouterActor
from .transform import TransformActor
from .vertex import VertexActor
from .vertex_router import VertexRouterActor


class ActorWrapper:
    """Wrapper class for managing actor instances."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        config = parse_root_config(*args, **kwargs)
        w = ActorWrapper.from_config(config)
        self.actor = w.actor
        self.init_ctx = w.init_ctx

    @property
    def vertex_config(self) -> VertexConfig:
        return self.init_ctx.vertex_config

    @property
    def edge_config(self) -> EdgeConfig:
        return self.init_ctx.edge_config

    @property
    def infer_edges(self) -> bool:
        return self.init_ctx.infer_edges

    @property
    def infer_edge_only(self) -> set[EdgeId]:
        return self.init_ctx.infer_edge_only

    @property
    def infer_edge_except(self) -> set[EdgeId]:
        return self.init_ctx.infer_edge_except

    @property
    def target_db_flavor(self) -> DBType | None:
        return self.init_ctx.target_db_flavor

    def init_transforms(self, init_ctx: ActorInitContext) -> None:
        self.init_ctx = init_ctx
        self.actor.init_transforms(init_ctx)

    def finish_init(self, init_ctx: ActorInitContext) -> None:
        self.init_ctx = init_ctx
        self.actor.init_transforms(init_ctx)
        self.actor.finish_init(init_ctx)

    def count(self) -> int:
        return self.actor.count()

    @classmethod
    def from_config(cls, config: ActorConfig) -> ActorWrapper:
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
        elif isinstance(config, EdgeRouterActorConfig):
            actor = EdgeRouterActor.from_config(config)
        else:
            raise ValueError(
                f"Expected VertexActorConfig, TransformActorConfig, EdgeActorConfig, "
                f"DescendActorConfig, VertexRouterActorConfig, or EdgeRouterActorConfig, "
                f"got {type(config)}"
            )
        wrapper = cls.__new__(cls)
        wrapper.actor = actor
        wrapper.init_ctx = ActorInitContext(
            vertex_config=VertexConfig(vertices=[]),
            edge_config=EdgeConfig(),
            transforms={},
            allowed_vertex_names=None,
            infer_edges=True,
            infer_edge_only=set(),
            infer_edge_except=set(),
        )
        return wrapper

    @classmethod
    def _from_step(cls, step: dict[str, Any]) -> ActorWrapper:
        config = validate_actor_step(normalize_actor_step(step))
        return cls.from_config(config)

    def __call__(
        self,
        ctx: ExtractionContext,
        lindex: LocationIndex = LocationIndex(),
        *nargs: Any,
        **kwargs: Any,
    ) -> ExtractionContext:
        ctx = self.actor(ctx, lindex, *nargs, **kwargs)
        return ctx

    def assemble(
        self, ctx: ExtractionContext | AssemblyContext | ActionContext
    ) -> defaultdict[GraphEntity, list]:
        if isinstance(ctx, AssemblyContext):
            assembly_ctx = ctx
        else:
            assembly_ctx = AssemblyContext.from_extraction(ctx)
        assemble_edges(
            ctx=assembly_ctx,
            vertex_config=self.vertex_config,
            edge_config=self.edge_config,
            infer_edges=self.infer_edges,
            infer_edge_only=self.infer_edge_only,
            infer_edge_except=self.infer_edge_except,
            target_db_flavor=self.target_db_flavor,
            edge_derivation=self.init_ctx.edge_derivation,
        )

        for vertex_name, dd in assembly_ctx.acc_vertex.items():
            for lindex, vertex_list in dd.items():
                vertex_list = [x.vertex for x in vertex_list]
                vertex_list_updated = merge_doc_basis(
                    vertex_list,
                    tuple(self.vertex_config.identity_fields(vertex_name)),
                )
                vertex_list_updated = pick_unique_dict(vertex_list_updated)
                assembly_ctx.acc_global[vertex_name] += vertex_list_updated

        assembly_ctx = add_blank_collections(assembly_ctx, self.vertex_config)

        if isinstance(ctx, ActionContext):
            ctx.acc_global = assembly_ctx.acc_global
            return ctx.acc_global
        return assembly_ctx.acc_global

    @classmethod
    def from_dict(cls, data: dict | list) -> ActorWrapper:
        if isinstance(data, list):
            return cls(*data)
        return cls(**data)

    def assemble_tree(self, fig_path: Path | None = None):
        import logging

        logger = logging.getLogger(__name__)
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
            VertexRouterActor: fillcolor_palette["peach"],
            EdgeRouterActor: fillcolor_palette["red"],
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
            ag.draw(fig_path, "pdf", prog="dot")
            return None
        return g

    def fetch_actors(self, level: int, edges: list) -> tuple[int, type, str, list]:
        return self.actor.fetch_actors(level, edges)

    def collect_actors(self) -> list[Actor]:
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
        if isinstance(self.actor, DescendActor):
            for d in list(self.actor.descendants):
                d.remove_descendants_if(predicate=predicate)
            self.actor._descendants[:] = [
                d
                for d in self.actor.descendants
                if not predicate(d)
                and not (isinstance(d.actor, DescendActor) and d.count() == 0)
            ]
