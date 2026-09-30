"""Blank-node content keys and ``owl:sameAs`` components."""

from __future__ import annotations

from graflo.data_source.rdf_identity import BlankNodeKeys, SameAs, Term

P = "http://example.org/p"
Q = "http://example.org/q"


def _keys(triples: dict[str, list[tuple[str, Term]]]) -> BlankNodeKeys:
    return BlankNodeKeys(lambda label: triples.get(label, []))


def _lit(value: str) -> Term:
    return Term("literal", value)


def _blank(label: str) -> Term:
    return Term("bnode", label)


class TestBlankNodeKeys:
    def test_the_key_does_not_depend_on_the_label(self) -> None:
        first = _keys({"a": [(P, _lit("1")), (Q, Term("iri", "http://example.org/x"))]})
        second = _keys(
            {"z9": [(Q, Term("iri", "http://example.org/x")), (P, _lit("1"))]}
        )
        assert first.key("a") == second.key("z9")

    def test_different_content_gives_a_different_key(self) -> None:
        keys = _keys({"a": [(P, _lit("1"))], "b": [(P, _lit("2"))]})
        assert keys.key("a") != keys.key("b")

    def test_the_predicate_is_part_of_the_content(self) -> None:
        keys = _keys({"a": [(P, _lit("1"))], "b": [(Q, _lit("1"))]})
        assert keys.key("a") != keys.key("b")

    def test_a_literal_and_an_iri_with_one_text_differ(self) -> None:
        keys = _keys(
            {
                "a": [(P, _lit("http://example.org/x"))],
                "b": [(P, Term("iri", "http://example.org/x"))],
            }
        )
        assert keys.key("a") != keys.key("b")

    def test_the_datatype_and_the_language_are_part_of_a_literal(self) -> None:
        integer = "http://www.w3.org/2001/XMLSchema#integer"
        keys = _keys(
            {
                "plain": [(P, _lit("1"))],
                "typed": [(P, Term("literal", "1", datatype=integer))],
                "tagged": [(P, Term("literal", "1", lang="en"))],
            }
        )
        assert len({keys.key("plain"), keys.key("typed"), keys.key("tagged")}) == 3

    def test_a_plain_literal_is_an_xsd_string(self) -> None:
        string = "http://www.w3.org/2001/XMLSchema#string"
        keys = _keys(
            {
                "plain": [(P, _lit("x"))],
                "typed": [(P, Term("literal", "x", datatype=string))],
            }
        )
        assert keys.key("plain") == keys.key("typed")

    def test_a_nested_blank_node_counts_by_its_content(self) -> None:
        first = _keys({"a": [(P, _blank("n"))], "n": [(Q, _lit("1"))]})
        second = _keys({"b": [(P, _blank("m"))], "m": [(Q, _lit("1"))]})
        other = _keys({"c": [(P, _blank("k"))], "k": [(Q, _lit("2"))]})
        assert first.key("a") == second.key("b")
        assert first.key("a") != other.key("c")

    def test_a_cycle_ends_and_does_not_depend_on_the_labels(self) -> None:
        first = _keys(
            {"a": [(P, _blank("b")), (Q, _lit("1"))], "b": [(P, _blank("a"))]}
        )
        second = _keys(
            {"x": [(P, _blank("y")), (Q, _lit("1"))], "y": [(P, _blank("x"))]}
        )
        assert first.key("a") == second.key("x")
        assert first.key("b") == second.key("y")
        assert first.key("a") != first.key("b")

    def test_a_node_in_a_cycle_has_one_key_however_it_is_reached(self) -> None:
        triples = {
            "a": [(P, _blank("b")), (Q, _lit("1"))],
            "b": [(P, _blank("a"))],
        }
        alone = _keys(triples).key("b")
        keys = _keys(triples)
        keys.key("a")
        assert keys.key("b") == alone

    def test_a_long_chain_is_keyed(self) -> None:
        length = 5000
        triples = {
            f"n{i}": [(P, _blank(f"n{i + 1}")), (Q, _lit(str(i)))]
            for i in range(length)
        }
        triples[f"n{length}"] = [(Q, _lit("end"))]
        assert len(_keys(triples).key("n0")) == 32


class TestSameAs:
    def test_an_iri_nothing_is_said_about_stands_for_itself(self) -> None:
        same_as = SameAs([])
        assert same_as.canonical("http://example.org/a") == "http://example.org/a"
        assert same_as.members("http://example.org/a") == ["http://example.org/a"]

    def test_the_smallest_iri_stands_for_the_component(self) -> None:
        same_as = SameAs([("http://example.org/b", "http://example.org/a")])
        assert same_as.canonical("http://example.org/a") == "http://example.org/a"
        assert same_as.canonical("http://example.org/b") == "http://example.org/a"

    def test_statements_chain(self) -> None:
        same_as = SameAs(
            [
                ("http://example.org/c", "http://example.org/d"),
                ("http://example.org/b", "http://example.org/c"),
                ("http://example.org/x", "http://example.org/y"),
            ]
        )
        assert same_as.members("http://example.org/d") == [
            "http://example.org/b",
            "http://example.org/c",
            "http://example.org/d",
        ]
        assert same_as.canonical("http://example.org/y") == "http://example.org/x"
