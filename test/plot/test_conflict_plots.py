"""The conflict figures: what gets drawn, and where each mark lands.

Everything here asserts against the networkx graph, never against a rendered
file — ``build_preview_graph`` / ``build_merge3_graph`` are separate from the
drawing for exactly this reason, so the suite needs no Graphviz. The tests that
reach the writer replace the SVG renderer with a stub.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from graflo.architecture.evolution.preview import (
    Merge3Preview,
    MergeFinding,
    MergeOutcome,
    MergePreview,
    PreviewEdge,
    PreviewNode,
    SlotNode,
)
from graflo.plot import merge as merge_plot
from graflo.plot import merge3 as merge3_plot
from graflo.plot import render as render_module
from graflo.plot.merge import (
    HEADER_COLOR,
    SEVERITY_COLOR,
    build_preview_graph,
    plot_merge_preview,
)
from graflo.plot.merge3 import build_history_graph, build_merge3_graph
from graflo.plot.render import (
    header_band,
    record_escape,
    resolve_format,
    sanitize_id,
)


def _preview(**updates) -> MergePreview:
    """A two-class merge with one cluster and one attribute rename."""
    nodes = [
        PreviewNode(id="left:Firm", kind="class", name="Firm", side="left"),
        PreviewNode(
            id="left:Firm.firm_id",
            kind="attribute",
            name="firm_id",
            side="left",
            owner="left:Firm",
            identity=True,
            field_type="STRING",
        ),
        PreviewNode(
            id="left:Firm.note",
            kind="attribute",
            name="note",
            side="left",
            owner="left:Firm",
        ),
        PreviewNode(id="right:Org", kind="class", name="Org", side="right"),
        PreviewNode(
            id="right:Org.org_id",
            kind="attribute",
            name="org_id",
            side="right",
            owner="right:Org",
            identity=True,
        ),
        PreviewNode(id="merged:Company", kind="merged", name="Company"),
        PreviewNode(
            id="merged:Company.company_id",
            kind="attribute",
            name="company_id",
            owner="merged:Company",
            identity=True,
        ),
    ]
    edges = [
        PreviewEdge(
            id="member:left:Firm->merged:Company",
            source="left:Firm",
            target="merged:Company",
            kind="member",
        ),
        PreviewEdge(
            id="member:right:Org->merged:Company",
            source="right:Org",
            target="merged:Company",
            kind="member",
        ),
        PreviewEdge(
            id="property_map:left:Firm.firm_id->merged:Company.company_id",
            source="left:Firm.firm_id",
            target="merged:Company.company_id",
            kind="property_map",
            label="company_id",
        ),
    ]
    return MergePreview(
        left_name="a", right_name="b", nodes=nodes, edges=edges, **updates
    )


# ── ids and escaping ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("left:Firm", "left_Firm"),
        ("left:Firm.firm_id", "left_Firm_firm_id"),
        ("merged:Company", "merged_Company"),
        ("9lives", "n9lives"),
    ],
)
def test_an_id_is_made_safe_for_graphviz(raw, expected):
    """``:`` is a port separator and ``.`` a record one, so neither can survive."""
    assert sanitize_id(raw) == expected


def test_ids_that_sanitize_alike_are_still_told_apart():
    taken: dict[str, str] = {}
    first = sanitize_id("a:b", taken)
    second = sanitize_id("a.b", taken)

    assert first != second
    assert sanitize_id("a:b", taken) == first, "stable for the same input"


def test_record_syntax_in_a_name_is_escaped():
    """An unescaped ``|``, brace or angle bracket splits or breaks a record field."""
    assert record_escape("a|b") == "a\\|b"
    assert record_escape("{x} <p>") == "\\{x\\} \\<p\\>"
    assert record_escape("back\\slash") == "back\\\\slash"
    assert record_escape("two\nlines") == "two\\nlines"


@pytest.mark.parametrize("suffix", [".svg", ".pdf", ".png", ".dot"])
def test_every_supported_suffix_resolves(suffix):
    assert resolve_format(Path(f"x{suffix}"), None) == suffix.lstrip(".")


def test_an_unsupported_format_is_refused():
    with pytest.raises(ValueError, match="unsupported output format"):
        resolve_format(Path("x.jpeg"), None)


# ── the merge figure ──────────────────────────────────────────────────────


def test_attributes_are_rows_on_their_class_not_nodes_of_their_own():
    graph = build_preview_graph(_preview())

    assert set(graph.nodes) == {"left_Firm", "right_Org", "merged_Company", "legend"}
    label = graph.nodes["left_Firm"]["label"]
    assert "firm_id" in label and "note" in label


def test_an_identity_attribute_is_marked_in_its_row():
    graph = build_preview_graph(_preview())

    label = graph.nodes["left_Firm"]["label"]
    assert "<p_firm_id> firm_id (key)" in label
    assert "note (key)" not in label


def test_a_declared_type_reaches_the_row():
    graph = build_preview_graph(_preview())

    assert "STRING" in graph.nodes["left_Firm"]["label"]


def test_only_the_header_field_is_coloured():
    """A record fills whole; a hard two-stop gradient colours just its header."""
    graph = build_preview_graph(_preview())

    node = graph.nodes["left_Firm"]
    colour, rest = node["fillcolor"].split(";")
    assert colour == HEADER_COLOR["left"]
    assert rest.endswith(":white")
    assert node["gradientangle"] == "270", "the band starts at the top"


def test_the_header_band_is_the_headers_share_of_the_height():
    # Graphviz: a field is lines * fontsize * 1.2 + 8 points high.
    band = header_band("#ABCDEF", header_lines=2, rows=3, fontsize=10)
    header, row = 2 * 12 + 8, 12 + 8
    assert band["fillcolor"] == f"#ABCDEF;{header / (header + 3 * row):.4f}:white"
    assert header_band("#ABCDEF", header_lines=1, rows=0, fontsize=10) == {
        "fillcolor": "#ABCDEF"
    }


def test_a_property_edge_lands_on_the_rows_it_renames():
    """The whole reason for record labels: an attribute edge is attribute-level."""
    graph = build_preview_graph(_preview())

    attrs = next(
        data
        for _u, _v, data in graph.edges(data=True)
        if data["kind"] == "property_map"
    )
    assert attrs["tailport"] == "p_firm_id"
    assert attrs["headport"] == "p_company_id"


def test_a_right_side_edge_is_drawn_backwards_with_its_ports_swapped():
    """Reversing keeps the right column on the right; the ports must follow."""
    preview = _preview()
    preview.edges.append(
        PreviewEdge(
            id="property_map:right:Org.org_id->merged:Company.company_id",
            source="right:Org.org_id",
            target="merged:Company.company_id",
            kind="property_map",
        )
    )
    graph = build_preview_graph(preview)

    reversed_edge = next(
        (u, v, data)
        for u, v, data in graph.edges(data=True)
        if data.get("dir") == "back" and data["kind"] == "property_map"
    )
    source, target, attrs = reversed_edge
    assert (source, target) == ("merged_Company", "right_Org")
    assert attrs["tailport"] == "p_company_id", "the merged end is now the tail"
    assert attrs["headport"] == "p_org_id"


def test_a_finding_colours_what_it_names_and_earns_a_badge():
    preview = _preview(
        findings=[
            MergeFinding(
                kind="identity_disagreement",
                severity="possible",
                message="members disagree",
                source="structure",
                nodes=["left:Firm"],
            )
        ]
    )
    graph = build_preview_graph(preview)

    assert graph.nodes["left_Firm"]["color"] == SEVERITY_COLOR["possible"]
    assert "[1]" in graph.nodes["left_Firm"]["label"]
    assert "identity_disagreement" in graph.nodes["legend"]["label"]


def test_a_refusal_outranks_a_possible_finding_on_the_same_node():
    """A node in two findings is drawn at the more serious of them."""
    preview = _preview(
        findings=[
            MergeFinding(
                kind="disagreement",
                severity="possible",
                message="maybe",
                source="structure",
                nodes=["left:Firm"],
            ),
            MergeFinding(
                kind="disagreement",
                severity="refusal",
                message="refused",
                source="merge",
                nodes=["left:Firm"],
            ),
        ]
    )
    graph = build_preview_graph(preview)

    assert graph.nodes["left_Firm"]["color"] == SEVERITY_COLOR["refusal"]


def test_an_empty_findings_list_still_gets_a_legend():
    graph = build_preview_graph(_preview())

    assert "no conflicts found" in graph.nodes["legend"]["label"]


def test_the_legend_can_be_left_out():
    graph = build_preview_graph(_preview(), legend=False)

    assert "legend" not in graph.nodes


def test_the_title_says_how_the_compose_ended():
    graph = build_preview_graph(
        _preview(outcome=MergeOutcome(status="refused", error_type="Boom"))
    )

    assert "refused: Boom" in graph.graph["graph"]["label"]


# ── the row budget ──────────────────────────────────────────────────────────


def _wide(count: int) -> MergePreview:
    nodes: list[PreviewNode] = [
        PreviewNode(id="left:Wide", kind="class", name="Wide", side="left")
    ]
    nodes += [
        PreviewNode(
            id=f"left:Wide.f{index}",
            kind="attribute",
            name=f"f{index}",
            side="left",
            owner="left:Wide",
            identity=index == 0,
        )
        for index in range(count)
    ]
    return MergePreview(nodes=nodes)


def test_a_wide_class_is_trimmed_and_says_so():
    graph = build_preview_graph(_wide(40), max_rows=5, legend=False)

    label = graph.nodes["left_Wide"]["label"]
    assert "more" in label
    assert len(label.split("|")) == 7  # header, five rows, the summary


def test_trimming_never_drops_an_attribute_the_reader_came_for():
    """An identity or flagged row survives the budget; a filler row need not."""
    preview = _wide(40)
    preview.findings.append(
        MergeFinding(
            kind="type_conflict",
            severity="possible",
            message="types differ",
            source="structure",
            nodes=["left:Wide.f37"],
        )
    )
    graph = build_preview_graph(preview, max_rows=3, legend=False)

    label = graph.nodes["left_Wide"]["label"]
    assert "f0" in label, "the identity row"
    assert "f37" in label, "the flagged row"


def test_no_budget_draws_everything():
    graph = build_preview_graph(_wide(40), max_rows=0, legend=False)

    assert "more" not in graph.nodes["left_Wide"]["label"]


# ── the writer ──────────────────────────────────────────────────────────────


def _stub_svg(monkeypatch) -> list[dict[str, str]]:
    """Replace the WebAssembly renderer; record what it was asked to draw."""
    calls: list[dict[str, str]] = []

    def _render(source: str, *, prog: str = "dot") -> bytes:
        calls.append({"source": source, "prog": prog})
        return b"<svg xmlns='http://www.w3.org/2000/svg'/>"

    monkeypatch.setattr(render_module, "render_svg", _render)
    return calls


def test_drawing_creates_the_directory_and_picks_the_format(monkeypatch, tmp_path):
    calls = _stub_svg(monkeypatch)
    target = tmp_path / "nested" / "deeper" / "preview.svg"

    written = plot_merge_preview(_preview(), target)

    assert written == target
    assert target.parent.is_dir(), "a missing directory is created, not raised on"
    assert target.read_bytes().startswith(b"<svg")
    assert [call["prog"] for call in calls] == ["dot"]


def test_dot_is_written_as_source_rather_than_laid_out(monkeypatch, tmp_path):
    calls = _stub_svg(monkeypatch)
    target = tmp_path / "preview.dot"

    plot_merge_preview(_preview(), target)

    assert target.read_text().startswith("digraph")
    assert not calls, "dot needs no layout run"


def test_each_side_becomes_its_own_column(tmp_path):
    target = tmp_path / "preview.dot"

    plot_merge_preview(_preview(), target)

    source = target.read_text()
    for name in ("cluster_left", "cluster_merged", "cluster_right"):
        assert f'subgraph "{name}"' in source


def test_an_unsupported_plot_format_is_refused(tmp_path):
    with pytest.raises(ValueError, match="unsupported output format"):
        plot_merge_preview(_preview(), tmp_path / "preview.gif")


def test_a_missing_plot_extra_says_what_to_install(monkeypatch, tmp_path):
    monkeypatch.setitem(sys.modules, "wasi_graphviz", None)

    with pytest.raises(RuntimeError, match=r"graflo\[plot\].*") as caught:
        plot_merge_preview(_preview(), tmp_path / "preview.svg")
    assert "wasi_graphviz" in str(caught.value)


def test_a_runtime_that_cannot_load_says_what_to_install(monkeypatch, tmp_path):
    wasi_graphviz = pytest.importorskip("wasi_graphviz")

    def _boom(*args, **kwargs):
        raise OSError("Error loading shared library libgcc_s.so.1: No such file")

    monkeypatch.setattr(wasi_graphviz, "render", _boom)

    with pytest.raises(RuntimeError) as caught:
        plot_merge_preview(_preview(), tmp_path / "preview.svg")
    message = str(caught.value)
    assert "libgcc_s.so.1" in message
    assert "apk add libgcc" in message


# ── the merge figures ───────────────────────────────────────────────────────


def _merge_preview() -> Merge3Preview:
    return Merge3Preview(
        nodes=[
            SlotNode(id="vertex", segment="vertex", depth=0),
            SlotNode(id="vertex/person", segment="person", depth=1, parent="vertex"),
            SlotNode(
                id="vertex/person/identity",
                segment="identity",
                depth=2,
                parent="vertex/person",
                contested=True,
                reason="both sides changed this slot",
                left_ops=["replace_identity"],
                right_ops=["replace_identity"],
                base_excerpt={"identity": ["ssn"], "name": "person"},
            ),
        ],
        conflicts=1,
        clean=False,
    )


def test_the_slot_tree_is_drawn_as_a_tree():
    graph = build_merge3_graph(_merge_preview())

    assert set(graph.nodes) == {"vertex", "vertex_person", "vertex_person_identity"}
    assert list(graph.edges) == [
        ("vertex", "vertex_person"),
        ("vertex_person", "vertex_person_identity"),
    ]


def test_a_contested_slot_shows_both_branches_and_the_ancestors_keys():
    graph = build_merge3_graph(_merge_preview())

    label = graph.nodes["vertex_person_identity"]["label"]
    assert "left" in label and "right" in label
    assert "replace_identity" in label
    assert "base" in label and "identity, name" in label
    assert graph.nodes["vertex_person_identity"]["contested"] == "true"


def test_a_clean_merge_says_so_in_the_title():
    graph = build_merge3_graph(Merge3Preview(clean=True))

    assert "clean" in graph.graph["graph"]["label"]


def test_the_commit_dag_marks_the_base_and_the_two_heads():
    history = SimpleNamespace(
        commits=[
            SimpleNamespace(id="a" * 12, parents=[], kind="edit", label="root"),
            SimpleNamespace(id="b" * 12, parents=["a" * 12], kind="edit", label="left"),
            SimpleNamespace(
                id="c" * 12, parents=["a" * 12], kind="edit", label="right"
            ),
        ]
    )

    graph = build_history_graph(
        history, heads=["b" * 12, "c" * 12], merge_base="a" * 12
    )

    assert graph.nodes[sanitize_id("a" * 12)]["role"] == "base"
    assert "merge base" in graph.nodes[sanitize_id("a" * 12)]["label"]
    assert graph.nodes[sanitize_id("b" * 12)]["role"] == "head"
    assert graph.number_of_edges() == 2


def test_only_the_first_parent_edge_is_solid():
    """A commit's ops are a diff from its first parent; the rest is lineage."""
    history = SimpleNamespace(
        commits=[
            SimpleNamespace(id="a", parents=[], kind="edit", label=None),
            SimpleNamespace(id="b", parents=[], kind="edit", label=None),
            SimpleNamespace(id="m", parents=["a", "b"], kind="merge", label=None),
        ]
    )

    graph = build_history_graph(history)

    assert graph.edges["a", "m"]["style"] == "solid"
    assert graph.edges["b", "m"]["style"] == "dashed"
    assert graph.nodes["m"]["shape"] == merge3_plot.COMMIT_SHAPE["merge"]


def test_the_two_figures_share_one_rendering_path():
    """Both go through the same writer, so formats and mkdir behave alike."""
    assert merge_plot.draw is merge3_plot.draw
