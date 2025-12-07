import logging
from io import StringIO

import pandas as pd
import pytest
import yaml

from graflo.architecture import EdgeConfig, VertexConfig
from graflo.architecture.actor import ActorWrapper
from graflo.architecture.onto import ActionContext

logger = logging.getLogger(__name__)


@pytest.fixture()
def schema_ibes_vertices():
    tc = yaml.safe_load("""
    blank_vertices:
    -   publication
    vertices:
    -   name: publication
        fields:
        -   datetime_review
        -   datetime_announce
        indexes:
        -   fields:
            -   _key
    -   name: ticker
        fields:
        -   cusip
        indexes:
        -   fields:
            -   cusip
    -   name: agency
        fields:
        -   aname
        indexes:
        -   fields:
            -   aname
    -   name: analyst
        fields:
        -   last_name
        -   initial
        indexes:
        -   fields:
            -   last_name
            -   initial
    -   name: recommendation
        fields:
        -   erec
        indexes:
        -   fields:
            -   irec
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def schema_ibes_edges():
    tc = yaml.safe_load("""
    edges:
    -   source: publication
        target: ticker
    -   source: analyst
        target: agency
        weights:
            direct:
            -   datetime_review
            -   datetime_announce
    -   source: analyst
        target: publication
    -   source: publication
        target: recommendation
    """)
    return EdgeConfig.from_dict(tc)


@pytest.fixture()
def sample_ibes():
    s = """
TICKER,CUSIP,CNAME,OFTIC,ACTDATS,ESTIMID,ANALYST,ERECCD,ETEXT,IRECCD,ITEXT,EMASKCD,AMASKCD,USFIRM,ACTTIMS,REVDATS,REVTIMS,ANNDATS,ANNTIMS
0000,87482X10,TALMER BANCORP,TLMR,20140310,RBCDOMIN,ARFSTROM      J,2,OUTPERFORM,2,BUY,00000659,00071182,1,8:54:03,20160126,9:35:52,20140310,0:20:00
0000,87482X10,TALMER BANCORP,TLMR,20140311,JPMORGAN,ALEXOPOULOS   S,,OVERWEIGHT,2,BUY,00001243,00079092,1,17:10:47,20160126,10:09:34,20140310,0:25:00
0000,87482X10,TALMER BANCORP,TLMR,20140311,KEEFE,MCGRATTY      C,2,OUTPERFORM,2,BUY,00001308,00119962,1,15:17:15,20150730,7:25:58,20140309,17:05:00    """
    df = pd.read_csv(StringIO(s), sep=",").to_dict(orient="records")
    return df


@pytest.fixture()
def resource_ibes():
    an = yaml.safe_load("""
        -   vertex: ticker
        -   vertex: analyst
        -   vertex: recommendation
        -   vertex: agency
        -   foo: parse_date_ibes
            module: graflo.util.transform
            input:
            -   ANNDATS
            -   ANNTIMS
            output:
            -   datetime_announce
        -   foo: parse_date_ibes
            module: graflo.util.transform
            input:
            -   REVDATS
            -   REVTIMS
            output:
            -   datetime_review
        -   foo: cast_ibes_analyst
            module: graflo.util.transform
            input:
            -   ANALYST
            output:
            -   last_name
            -   initial
        -   map:
                CUSIP: cusip
                CNAME: cname
                OFTIC: oftic
        -   map:
                ESTIMID: aname
        -   map:
                ERECCD: erec
                ETEXT: etext
                IRECCD: irec
                ITEXT: itext
    """)
    return an


def test_actio_node_wrapper_openalex(
    resource_ibes, schema_ibes_vertices, schema_ibes_edges, sample_ibes
):
    # TODO blank vertices are introduced at the level of db ingestion
    anw = ActorWrapper(*resource_ibes, vertex_config=schema_ibes_vertices)
    for doc in sample_ibes:
        ctx = ActionContext()
        ctx = anw(ctx, doc=doc)
        # rdoc = ctx.acc
    # assert ctx.acc[("publication", "ticker", None)] == [{"id": "John"}, {"id": "Mary"}]
    assert True
