from graflo.architecture.table import TConfigurator
from graflo.arango.util import define_edge_indices, define_edge_collections


def add_extra_graphs(
    db_client,
    config,
):
    conf_obj = TConfigurator(config)
    graph_config = conf_obj.graph_config
    define_edge_collections(db_client, graph_config)
    define_edge_indices(db_client, graph_config)
