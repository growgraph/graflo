"""Unit tests for NebulaConfig."""

from graflo.connections.onto import NebulaConfig
from graflo.onto import DB_TYPE_TO_EXPRESSION_FLAVOR, DBType, ExpressionFlavor


def test_ngql_expression_flavor_exists():
    assert ExpressionFlavor.NGQL == "ngql"


def test_nebula_in_flavor_mapping():
    assert DBType.NEBULA in DB_TYPE_TO_EXPRESSION_FLAVOR
    assert DB_TYPE_TO_EXPRESSION_FLAVOR[DBType.NEBULA] == ExpressionFlavor.NGQL


def test_nebula_config_defaults():
    cfg = NebulaConfig(uri="nebula://localhost:9669")
    assert cfg.version == "3"
    assert cfg.is_v3 is True
    assert cfg.vid_type is None
    assert cfg.partition_num is None
    assert cfg.replica_factor is None


def test_nebula_config_v5():
    cfg = NebulaConfig(uri="nebula://localhost:9669", version="5")
    assert cfg.is_v3 is False


def test_nebula_config_default_port():
    cfg = NebulaConfig()
    assert cfg._get_default_port() == 9669
