"""Execution surface for actor extraction and assembly orchestration."""

from __future__ import annotations

from collections import defaultdict

from graflo.architecture.actor import ActorWrapper
from graflo.architecture.onto import (
    AssemblyContext,
    ExtractionContext,
    GraphAssemblyResult,
    GraphEntity,
)


class ActorExecutor:
    """Owns runtime extraction and assembly orchestration for an ActorWrapper."""

    def __init__(self, root: ActorWrapper):
        self.root = root

    def extract(self, doc: dict) -> ExtractionContext:
        extraction_ctx = ExtractionContext()
        return self.root(extraction_ctx, doc=doc)

    def assemble(
        self, extraction_ctx: ExtractionContext
    ) -> defaultdict[GraphEntity, list]:
        assembly_ctx = AssemblyContext.from_extraction(extraction_ctx)
        return self.root.assemble(assembly_ctx)

    def assemble_result(self, extraction_ctx: ExtractionContext) -> GraphAssemblyResult:
        return GraphAssemblyResult(entities=self.assemble(extraction_ctx))
