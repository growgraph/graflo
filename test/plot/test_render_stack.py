"""Graphs deeper than the WebAssembly stack: drawn when they fit, refused clearly when not."""

from __future__ import annotations

import threading

import pytest

pytest.importorskip("wasi_graphviz")

from graflo.plot.dot import DotGraph
from graflo.plot.render import StackExhausted, draw, render_svg


def _chain(length: int) -> str:
    return "digraph{" + ";".join(f"n{i}->n{i + 1}" for i in range(length)) + "}"


#: Deeper than wasmtime's default wasm stack lets Graphviz's layout recurse.
DEEP = 20_000
#: Deeper than the largest wasm stack wasmtime accepts.
TOO_DEEP = 100_000


def test_a_graph_deeper_than_the_default_wasm_stack_is_drawn():
    assert render_svg(_chain(DEEP)).lstrip().startswith(b"<?xml")


def test_a_graph_too_deep_to_lay_out_says_so_and_the_next_one_draws():
    with pytest.raises(StackExhausted, match=r"ran out of stack.*format `dot`"):
        render_svg(_chain(TOO_DEEP))

    assert b"<svg" in render_svg("digraph{a->b}")


def test_draw_names_the_size_of_a_graph_too_deep_to_lay_out(tmp_path):
    dot = DotGraph()
    for i in range(TOO_DEEP):
        dot.add_edge(f"n{i}", f"n{i + 1}")

    with pytest.raises(
        RuntimeError, match=rf"{TOO_DEEP + 1} nodes and {TOO_DEEP} edges"
    ):
        draw(dot, tmp_path / "chain.svg")
    assert not (tmp_path / "chain.svg").exists()


def test_a_caller_on_a_small_stack_thread_still_draws_a_deep_graph():
    result: list[bytes] = []
    previous = threading.stack_size(256 * 1024)
    try:
        thread = threading.Thread(
            target=lambda: result.append(render_svg(_chain(DEEP)))
        )
        thread.start()
    finally:
        threading.stack_size(previous)
    thread.join()

    assert result and b"<svg" in result[0]
