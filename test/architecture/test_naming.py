"""Declared naming conventions, conversion, and cross-convention identity."""

from __future__ import annotations

import pytest

from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.naming import (
    DEFAULT_NAMING,
    NameCase,
    NamingConvention,
    apply_case,
    canonical_key,
    convert,
    same_concept,
    singularize,
    split_words,
)


@pytest.mark.parametrize(
    ("raw", "words"),
    [
        ("customerEmail", ["customer", "email"]),
        ("customer_email", ["customer", "email"]),
        ("Customer-Email", ["customer", "email"]),
        ("CUSTOMER_EMAIL", ["customer", "email"]),
        ("OrderLine", ["order", "line"]),
        ("id", ["id"]),
        ("", []),
    ],
)
def test_split_words_recovers_words_from_any_style(raw, words) -> None:
    """Conversion is one-way because the source style never has to be declared."""
    assert split_words(raw) == words


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        (NameCase.PASCAL, "CustomerEmail"),
        (NameCase.CAMEL, "customerEmail"),
        (NameCase.SNAKE, "customer_email"),
        (NameCase.UPPER_SNAKE, "CUSTOMER_EMAIL"),
        (NameCase.KEBAB, "customer-email"),
        (NameCase.PRESERVE, "customer_email"),
    ],
)
def test_apply_case(case, expected) -> None:
    assert apply_case("customer_email", case) == expected


def test_apply_case_accepts_the_validated_string() -> None:
    """``use_enum_values=True`` means a validated field holds ``"pascal"``.

    Identity comparison against the enum member works for an unvalidated
    default and fails for every configured instance — a difference invisible to
    any test written only against ``NamingConvention()``.
    """
    convention = NamingConvention(vertex_case=NameCase.SNAKE)
    assert isinstance(convention.vertex_case, str)
    assert convention.vertex("OrderLine") == "order_line"


@pytest.mark.parametrize(
    ("plural", "singular"),
    [
        ("customers", "customer"),
        ("companies", "company"),
        ("addresses", "address"),
        ("classes", "class"),
        ("boxes", "box"),
        ("houses", "house"),
        ("status", "status"),
        ("basis", "basis"),
        ("series", "series"),
        ("species", "species"),
    ],
)
def test_singularize(plural, singular) -> None:
    assert singularize(plural) == singular


def test_singularize_prefers_a_real_word_over_a_coined_one() -> None:
    """``analyses`` is ambiguous; the conservative branch keeps it a word.

    Over-stripping would coin ``analys`` and rename the concept to something no
    other source will ever match, which is worse than leaving it plural.
    """
    assert singularize("analyses") == "analyse"


def test_default_convention() -> None:
    assert DEFAULT_NAMING.vertex("order_lines") == "OrderLine"
    assert DEFAULT_NAMING.relation("placed_by") == "placedBy"
    assert DEFAULT_NAMING.prop("customer_email") == "customerEmail"


def test_singular_rule_folds_only_the_last_word() -> None:
    assert DEFAULT_NAMING.vertex("supply_terms") == "SupplyTerm"
    assert NamingConvention(singular_vertex_names=False).vertex("customers") == (
        "Customers"
    )


def test_rename_map_covers_exactly_the_names_that_change() -> None:
    """The other half of a non-preserve property_case.

    A styled property name that the document does not carry populates with
    nothing and raises nothing, so the convention is only safe when this map is
    emitted with it.
    """
    mapping = DEFAULT_NAMING.rename_map(
        ["customer_email", "id", "total_eur", "alreadyCamel"]
    )
    assert mapping == {
        "customer_email": "customerEmail",
        "total_eur": "totalEur",
    }


def test_rename_map_is_empty_when_the_source_already_matches() -> None:
    assert DEFAULT_NAMING.rename_map(["customerEmail", "id"]) == {}
    preserving = NamingConvention(property_case=NameCase.PRESERVE)
    assert preserving.rename_map(["customer_email", "TOTAL"]) == {}


def test_convert_between_conventions() -> None:
    assert convert("customer_email", NameCase.CAMEL) == "customerEmail"
    assert convert("customerEmail", NameCase.SNAKE) == "customer_email"
    assert convert("CUSTOMER_EMAIL", NameCase.PASCAL) == "CustomerEmail"


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("Customer", "customer"),
        ("Customers", "Customer"),
        ("OrderLine", "order_line"),
        ("orderLine", "ORDER_LINE"),
        ("order-lines", "OrderLine"),
    ],
)
def test_same_concept_across_conventions(left, right) -> None:
    """What a merge must compare on.

    Composition matches by name, so without this a manifest in PascalCase
    merged with one in snake_case yields two unrelated vertex types holding
    half the data each, and nothing raises.
    """
    assert same_concept(left, right)


def test_same_concept_does_not_conflate_different_words() -> None:
    assert not same_concept("Customer", "Client")
    assert not same_concept("Order", "OrderLine")


def test_canonical_key_is_a_word_tuple() -> None:
    assert canonical_key("OrderLines") == ("order", "line")
    assert canonical_key("") == ()


def test_metadata_accepts_and_omits_the_block() -> None:
    assert GraphMetadata(name="shop").naming is None
    declared = GraphMetadata(
        name="shop", naming={"vertex_case": "snake", "relation_case": "snake"}
    )
    assert declared.naming is not None
    assert declared.naming.vertex("Customers") == "customer"


def test_describe_names_a_style_and_shows_one() -> None:
    """This string goes into the designer prompt; ``camel`` alone is ambiguous."""
    described = DEFAULT_NAMING.describe()
    assert "PascalCase" in described and "Customer" in described
    assert "camelCase" in described and "placedBy" in described
