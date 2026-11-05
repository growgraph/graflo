"""``plot_manifest`` draws every figure it can and exits 1 naming the rest."""

from __future__ import annotations

import pathlib

import click
from click.testing import CliRunner

from graflo.cli.plot_manifest import FIGURES, plot_manifest
from graflo.plot import plotter as plotter_module

MANIFEST = (
    pathlib.Path(__file__).resolve().parents[2]
    / "examples"
    / "01-csv-two-resources"
    / "manifest.yaml"
)


def _record_drawn(monkeypatch) -> list[str]:
    drawn: list[str] = []

    def _draw(dot, path, **kwargs):
        drawn.append(pathlib.Path(path).name)
        return pathlib.Path(path)

    monkeypatch.setattr(plotter_module, "draw", _draw)
    return drawn


def test_a_figure_that_cannot_be_drawn_does_not_stop_the_others(monkeypatch, tmp_path):
    drawn = _record_drawn(monkeypatch)

    def _too_large(self):
        raise RuntimeError("Graphviz ran out of stack laying out a graph")

    monkeypatch.setattr(plotter_module.ManifestPlotter, "plot_vc2fields", _too_large)

    result = CliRunner().invoke(
        plot_manifest,
        ["-c", str(MANIFEST), "-o", str(tmp_path), "--output-format", "svg"],
        standalone_mode=False,
    )

    assert isinstance(result.exception, click.ClickException)
    assert result.exception.exit_code == 1
    assert "not drawn: vc2fields" in result.exception.message
    assert "--only" in result.exception.message
    assert any("_vc2vc" in name for name in drawn)
    assert any("_source2vc" in name for name in drawn)
    assert not any("_vc2fields" in name for name in drawn)


def test_every_figure_drawn_exits_0(monkeypatch, tmp_path):
    drawn = _record_drawn(monkeypatch)

    result = CliRunner().invoke(
        plot_manifest, ["-c", str(MANIFEST), "-o", str(tmp_path)]
    )

    assert result.exit_code == 0, result.output
    for figure in ("vc2vc", "vc2fields", "source2vc"):
        assert any(f"_{figure}" in name for name in drawn)
    assert len(FIGURES) == 5
