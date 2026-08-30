"""Declared naming style for the identifiers a schema invents.

A schema names three kinds of thing, and only two of them are free choices.
Vertex types and relations are *invented* — nothing outside the schema decides
whether a customer concept is ``Customer``, ``customer`` or ``customers``.
Properties are not: a field name binds to a key in the source document, so
renaming one without also rewriting ingestion produces a column that never
populates.

That asymmetry does not stop properties having a convention — the documents
GraFlo ingests are JSON, so camelCase is the sensible default — but it does mean
styling them is a *rewrite* rather than a choice. Whoever sets a
``property_case`` other than ``preserve`` owes the matching
``transform.rename``, which :meth:`NamingConvention.rename_map` computes.
Without it the schema declares ``customerEmail`` while the document still holds
``customer_email``, and the column populates with nothing, silently.

Declaring the convention is what makes it enforceable and reusable: an agent
inferring a second schema against the same registry can read the style the first
one was authored in rather than guessing, and a reviewer can tell a deliberate
name from an accidental one.

Purely declarative, like :mod:`~graflo.architecture.schema.semantics`: nothing
in identity, storage naming or ingestion consults it. It records the intent that
produced the names; it does not rewrite them.

**Merging schemas across conventions** is the failure this module exists to
prevent. Composition matches vertices and edges by name, so a manifest written
in PascalCase merged with one written in snake_case yields a graph carrying both
``Customer`` and ``customer`` as unrelated types with the data split between
them, and nothing raises. :func:`canonical_key` and :func:`same_concept` are
what a merge should compare on.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import BaseEnum


class NameCase(BaseEnum):
    """Surface style for a generated identifier.

    ``PRESERVE`` means "whatever the source called it" and is meaningful only
    for properties, where the name is not the schema's to choose.
    """

    PASCAL = "pascal"
    CAMEL = "camel"
    SNAKE = "snake"
    UPPER_SNAKE = "upper_snake"
    KEBAB = "kebab"
    PRESERVE = "preserve"


_WORD_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SEPARATORS = re.compile(r"[^A-Za-z0-9]+")


def split_words(name: str) -> list[str]:
    """Break an identifier into lowercase words.

    Handles the forms that actually turn up in source data and in model output:
    ``customerEmail``, ``customer_email``, ``Customer-Email``, ``CUSTOMER_EMAIL``.
    """
    spaced = _WORD_BOUNDARY.sub(" ", name)
    return [w.lower() for w in _SEPARATORS.sub(" ", spaced).split() if w]


#: Stems where a trailing ``es`` is unambiguously the plural marker, because
#: English cannot end a singular noun in these without one. Deliberately
#: excludes a lone ``s``: that would take ``houses`` to ``hous`` and
#: ``analyses`` to ``analys``.
_ES_STEMS = ("ss", "x", "z", "ch", "sh")

#: Nouns whose plural equals their singular, and which the suffix rules would
#: otherwise mangle. ``time_series`` is a plausible resource name and ``Sery``
#: is not a plausible vertex type.
_INVARIANT = frozenset({"series", "species", "news", "data", "media"})


def singularize(word: str) -> str:
    """Crude singular of one lowercase word.

    Covers the regular English plurals that appear in table and column names.
    Where a suffix is genuinely ambiguous — ``analyses`` is either ``analysis``
    or ``analyse``, ``houses`` is ``house`` — it takes the conservative branch
    and strips only the ``s``.

    That bias is deliberate. This decides what a vertex type is *called*, and
    the two failure modes are not equal: under-stripping leaves a recognisable
    English word, while over-stripping coins a non-word (``Analys``) and renames
    the concept to something no other source will ever match.
    """
    if word in _INVARIANT:
        return word
    if len(word) > 3 and word.endswith("ies"):
        return f"{word[:-3]}y"
    if len(word) > 4 and word.endswith("es") and word[:-2].endswith(_ES_STEMS):
        return word[:-2]
    if len(word) > 3 and word.endswith("s") and not word.endswith(("ss", "us", "is")):
        return word[:-1]
    return word


def apply_case(name: str, case: NameCase | str) -> str:
    """Render *name* in *case*. ``PRESERVE`` returns it untouched.

    Accepts the raw string as well as the enum member: ``ConfigBaseModel`` sets
    ``use_enum_values=True``, so a field declared ``NameCase`` holds ``"pascal"``
    once validated. Comparing by identity works only for unvalidated defaults,
    which is the kind of difference that passes every test written against a
    default-constructed object and fails on the first configured one.
    """
    resolved = NameCase(case)
    if resolved is NameCase.PRESERVE:
        return name
    words = split_words(name)
    if not words:
        return name
    if resolved is NameCase.PASCAL:
        return "".join(w.capitalize() for w in words)
    if resolved is NameCase.CAMEL:
        head, *rest = words
        return head + "".join(w.capitalize() for w in rest)
    if resolved is NameCase.SNAKE:
        return "_".join(words)
    if resolved is NameCase.UPPER_SNAKE:
        return "_".join(w.upper() for w in words)
    return "-".join(words)


#: Worked example per style. A convention stated as ``camel`` is ambiguous to a
#: reader and to a model; stated as ``camelCase (placedBy)`` it is not, and this
#: string goes straight into the designer prompt.
_EXAMPLES = {
    NameCase.PASCAL: "PascalCase (Customer, OrderLine)",
    NameCase.CAMEL: "camelCase (placedBy, reportsTo)",
    NameCase.SNAKE: "snake_case (placed_by, order_line)",
    NameCase.UPPER_SNAKE: "UPPER_SNAKE_CASE (PLACED_BY)",
    NameCase.KEBAB: "kebab-case (placed-by)",
    NameCase.PRESERVE: "exactly as the source names them",
}


class NamingConvention(ConfigBaseModel):
    """How a schema spells the identifiers it invents.

    Defaults follow the convention GraFlo declares canonical: singular
    PascalCase vertex types, camelCase relations, and camelCase properties.
    """

    vertex_case: NameCase = PydanticField(
        default=NameCase.PASCAL,
        description=("Style for vertex type names, e.g. ``Customer``, ``OrderLine``."),
    )
    relation_case: NameCase = PydanticField(
        default=NameCase.CAMEL,
        description=(
            "Style for edge relation names, e.g. ``placedBy``, ``reportsTo``. "
            "Matches the RDF/OWL object-property convention, so a relation "
            "grounded via ``semantics.iri`` reads the same either side."
        ),
    )
    property_case: NameCase = PydanticField(
        default=NameCase.CAMEL,
        description=(
            "Style for property names, e.g. ``customerEmail``. camelCase is "
            "the default because the documents GraFlo ingests and emits are "
            "JSON. **A property name binds to a key in the source document**, "
            "so any value other than ``preserve`` obliges the author to emit "
            "the rename that :meth:`NamingConvention.rename_map` computes — "
            "without it the column silently never populates."
        ),
    )
    singular_vertex_names: bool = PydanticField(
        default=True,
        description=(
            "Name a vertex for one instance rather than the collection: a "
            "resource called ``customers`` holds ``Customer`` records."
        ),
    )

    def vertex(self, name: str) -> str:
        """Render a vertex type name in this convention.

        Applies the singular rule it declares: only the last word is
        singularized, so ``order_lines`` becomes ``OrderLine`` and not
        ``OrderLine`` via a mangled ``order``.
        """
        if not self.singular_vertex_names:
            return apply_case(name, self.vertex_case)
        words = split_words(name)
        if not words:
            return apply_case(name, self.vertex_case)
        words[-1] = singularize(words[-1])
        return apply_case(" ".join(words), self.vertex_case)

    def relation(self, name: str) -> str:
        """Render an edge relation name in this convention."""
        return apply_case(name, self.relation_case)

    def prop(self, name: str) -> str:
        """Render a property name in this convention."""
        return apply_case(name, self.property_case)

    def rename_map(self, source_names: Iterable[str]) -> dict[str, str]:
        """Source field name -> convention-styled name, for names that change.

        The other half of a non-``preserve`` ``property_case``. Declaring a
        property as ``customerEmail`` when the document key is
        ``customer_email`` produces a column that never populates and no error,
        so the convention is only safe when this map is emitted alongside it as
        a ``transform.rename``.

        Entries are omitted where the name is already in the target style, so
        the resulting transform is empty exactly when no rewriting is needed.
        """
        mapping: dict[str, str] = {}
        for source in source_names:
            styled = self.prop(source)
            if styled != source:
                mapping[source] = styled
        return mapping

    def describe(self) -> str:
        """One-line human- and LLM-readable statement of the convention."""
        vertex = NameCase(self.vertex_case)
        relation = NameCase(self.relation_case)
        prop = NameCase(self.property_case)
        parts = [
            (
                f"vertex types {_EXAMPLES[vertex]}"
                if not self.singular_vertex_names
                else f"vertex types singular {_EXAMPLES[vertex]}"
            ),
            f"relations {_EXAMPLES[relation]}",
            (
                "properties exactly as the source names them"
                if prop is NameCase.PRESERVE
                else f"properties {_EXAMPLES[prop]}"
            ),
        ]
        return "; ".join(parts)


def convert(name: str, to_case: NameCase | str) -> str:
    """Restyle an identifier into *to_case*, whatever it is currently in.

    No source convention need be given: :func:`split_words` recovers the words
    from any of the styles this module emits, so conversion is one-way by
    construction and cannot be told the wrong origin.
    """
    return apply_case(name, to_case)


def canonical_key(name: str) -> tuple[str, ...]:
    """Convention-independent identity of a name, as its lowercase words.

    ``Customer``, ``customer``, ``customers`` and ``CUSTOMER`` all key to
    ``("customer",)``; ``OrderLine``, ``order_line`` and ``orderLine`` all key
    to ``("order", "line")``.

    **This is what merges must compare on.** Schema composition — GraFlo's
    evolution ops, and any agent extending a stored manifest — matches vertices
    and edges *by name*. Two manifests authored under different conventions
    therefore merge into a graph holding both ``Customer`` and ``customer`` as
    unrelated types, with the source data split between them and nothing
    raising. Comparing canonical keys is what detects that; the trailing plural
    is folded too, since ``Customers`` and ``Customer`` are one concept in every
    schema anyone means to write.
    """
    words = split_words(name)
    if not words:
        return ()
    return (*words[:-1], singularize(words[-1]))


def same_concept(left: str, right: str) -> bool:
    """Whether two identifiers denote the same concept under any convention."""
    return canonical_key(left) == canonical_key(right)


#: The convention GraFlo declares canonical when a schema does not say.
DEFAULT_NAMING = NamingConvention()

__all__ = [
    "DEFAULT_NAMING",
    "NameCase",
    "NamingConvention",
    "apply_case",
    "canonical_key",
    "convert",
    "same_concept",
    "singularize",
    "split_words",
]
