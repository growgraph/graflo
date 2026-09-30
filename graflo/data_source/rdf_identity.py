"""Identity of RDF nodes that have no stable IRI of their own.

* A blank node is keyed on a digest of its outgoing triples, so one content
  gets one key on every read, whatever label the parser or the endpoint gave it.
* IRIs joined by ``owl:sameAs`` form a component, and its smallest IRI stands
  for every member.

Nothing here depends on an RDF library: terms arrive as :class:`Term`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from typing import Literal, NamedTuple

OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"

#: Prefix of the ``_uri`` given to a blank node.
BLANK_PREFIX = "_:"

_KEY_LENGTH = 32


class Term(NamedTuple):
    """An RDF term, independent of the library that read it.

    ``value`` is the IRI, the blank-node label, or the literal's lexical form.
    """

    kind: Literal["iri", "bnode", "literal"]
    value: str
    datatype: str | None = None
    lang: str | None = None


def term_text(term: Term) -> str:
    """One spelling per IRI or literal; a plain literal is an ``xsd:string``."""
    if term.kind == "iri":
        return f"<{term.value}>"
    text = json.dumps(term.value, ensure_ascii=False)
    if term.lang:
        return f"{text}@{term.lang.lower()}"
    if term.datatype and term.datatype != XSD_STRING:
        return f"{text}^^<{term.datatype}>"
    return text


#: The ``(predicate IRI, object)`` pairs a blank node is the subject of.
Outgoing = Callable[[str], Iterable[tuple[str, Term]]]


class _Frame:
    """One blank node on the way down: its triples read so far."""

    __slots__ = ("label", "lines", "lowest", "pending", "waiting")

    def __init__(
        self, label: str, pending: Iterable[tuple[str, Term]], depth: int
    ) -> None:
        self.label = label
        self.pending = iter(pending)
        self.lines: list[str] = []
        # Depth of the shallowest node that this one, or a node below it,
        # refers back to. Its own depth or less means it lies on a cycle.
        self.lowest = depth + 1
        self.waiting = ""


class BlankNodeKeys:
    """Content keys for blank nodes.

    The key is a digest of the node's outgoing triples. A blank-node object
    counts by its own key, so a nested structure is keyed as a whole. A
    reference back to a node on the way down counts by how far back it
    points, which ends a cycle without naming a label.

    Two blank nodes with the same content have the same key.
    """

    def __init__(self, outgoing: Outgoing) -> None:
        self._outgoing = outgoing
        # A node on no cycle has one digest wherever it is met. A node on a
        # cycle is keyed from itself, and that key stands for it only when it
        # is the node asked for.
        self._acyclic: dict[str, str] = {}
        self._cyclic: dict[str, str] = {}

    def key(self, label: str) -> str:
        """The key of the blank node *label*."""
        known = self._acyclic.get(label) or self._cyclic.get(label)
        if known is not None:
            return known

        stack = [_Frame(label, self._outgoing(label), 0)]
        depth_of = {label: 0}
        while True:
            frame = stack[-1]
            depth = len(stack) - 1
            descended = False
            for predicate, obj in frame.pending:
                if obj.kind != "bnode":
                    frame.lines.append(f"<{predicate}> {term_text(obj)}")
                elif obj.value in self._acyclic:
                    frame.lines.append(f"<{predicate}> _:{self._acyclic[obj.value]}")
                elif obj.value in depth_of:
                    back = depth_of[obj.value]
                    frame.lowest = min(frame.lowest, back)
                    frame.lines.append(f"<{predicate}> _:^{depth - back}")
                else:
                    frame.waiting = predicate
                    depth_of[obj.value] = depth + 1
                    stack.append(
                        _Frame(obj.value, self._outgoing(obj.value), depth + 1)
                    )
                    descended = True
                    break
            if descended:
                continue

            digest = hashlib.sha256(
                "\n".join(sorted(frame.lines)).encode("utf-8")
            ).hexdigest()[:_KEY_LENGTH]
            stack.pop()
            del depth_of[frame.label]
            on_cycle = frame.lowest <= depth
            if not on_cycle:
                self._acyclic[frame.label] = digest
            if not stack:
                if on_cycle:
                    self._cyclic[frame.label] = digest
                return digest
            parent = stack[-1]
            parent.lines.append(f"<{parent.waiting}> _:{digest}")
            parent.lowest = min(parent.lowest, frame.lowest)


class SameAs:
    """Components of ``owl:sameAs`` statements between IRIs."""

    def __init__(self, pairs: Iterable[tuple[str, str]]) -> None:
        parent: dict[str, str] = {}

        def find(iri: str) -> str:
            root = iri
            while parent.setdefault(root, root) != root:
                root = parent[root]
            while parent[iri] != root:
                parent[iri], iri = root, parent[iri]
            return root

        for left, right in pairs:
            a, b = find(left), find(right)
            if a != b:
                # The smaller IRI is the root, so a root is its component's smallest.
                parent[max(a, b)] = min(a, b)

        self._canonical = {iri: find(iri) for iri in parent}
        self._members: dict[str, list[str]] = {}
        for iri, root in self._canonical.items():
            self._members.setdefault(root, []).append(iri)
        for members in self._members.values():
            members.sort()

    def __bool__(self) -> bool:
        return bool(self._canonical)

    def canonical(self, iri: str) -> str:
        """The IRI that stands for *iri*: the smallest of its component."""
        return self._canonical.get(iri, iri)

    def members(self, iri: str) -> list[str]:
        """Every IRI of the component of *iri*, sorted; ``[iri]`` when it is alone."""
        return self._members.get(self.canonical(iri), [iri])
