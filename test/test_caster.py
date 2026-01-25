import logging
import os
import pathlib
from os.path import dirname, realpath
from test.conftest import verify

import pytest
from suthing import FileHandle

from graflo.hq.caster import Caster

logger = logging.getLogger(__name__)


@pytest.fixture()
def current_path():
    return dirname(realpath(__file__))


@pytest.fixture(scope="function")
def modes():
    return [("kg", "json"), ("ibes", "csv")]


def cast(modes, schema_obj, current_path, level, reset, n_cores=1):
    for mode, ext in modes:
        # work with main resource
        resource_name = mode.split("_")[0]
        schema = schema_obj(mode)

        # Create the directory if it doesn't exist
        output_dir = "test/figs"
        pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Try to generate tree visualizations if graphviz is available
        try:
            from graflo.plot.plotter import assemble_tree

            for r in schema.resources:
                assemble_tree(
                    r.root, f"{output_dir}/{mode}.resource-{r.resource_name}.pdf"
                )
        except ImportError:
            # graphviz/pygraphviz not available, skip visualization
            logger.debug("graphviz not available, skipping tree visualization")
        caster = Caster(schema, n_cores=n_cores)

        if level == 0:
            fname = os.path.join(
                current_path,
                f"./data/{mode}",
                f"{mode}.{ext}.gz",
            )

            caster.process_resource(
                resource_instance=pathlib.Path(fname), resource_name=resource_name
            )
        else:
            data_obj = FileHandle.load(
                f"test.data.{mode}",
                f"{mode}.{ext}.gz",
            )

            if level == 1:
                caster.process_resource(
                    resource_instance=data_obj, resource_name=resource_name
                )
            elif level == 2:
                data = caster.normalize_resource(data_obj)
                graph = caster.cast_normal_resource(data, resource_name=resource_name)

                graph.pick_unique()

                vc = {k: len(v) for k, v in graph.items()}
                verify(
                    vc,
                    current_path=current_path,
                    mode=mode,
                    test_type="cast",
                    reset=reset,
                )


def test_cast(modes, schema_obj, current_path, reset):
    cast(modes, schema_obj, current_path, level=0, reset=reset)
    cast(modes, schema_obj, current_path, level=1, reset=reset)
    cast(modes, schema_obj, current_path, level=2, reset=reset)
    cast(modes, schema_obj, current_path, level=2, reset=reset, n_cores=4)
