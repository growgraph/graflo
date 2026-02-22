import pytest
import yaml


@pytest.fixture()
def table_config_ibes():
    tc = yaml.safe_load(
        """
        tabletype: ibes
        encoding: ISO-8859-1
        transforms:
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
    """
    )
    return tc


@pytest.fixture()
def vertex_config_ibes():
    vc = yaml.safe_load(
        """
        blanks:
        -   publication
        collections:
            publication:
                dbname: publications
                fields:
                -   datetime_review
                -   datetime_announce
                extra_index:
                -   type: hash
                    unique: false
                    fields:
                    -   datetime_review
                -   type: hash
                    unique: false
                    fields:
                    -   datetime_announce
            ticker:
                dbname: tickers
                fields:
                -   cusip
                -   cname
                -   oftic
                index:
                -   cusip
                -   cname
                -   oftic
            agency:
                dbname: agencies
                fields:
                -   aname
                index:
                -   aname
            analyst:
                dbname: analysts
                fields:
                -   last_name
                -   initial
                index:
                -   last_name
                -   initial
            recommendation:
                dbname: recommendations
                fields:
                -   erec
                -   etext
                -   irec
                -   itext
                index:
                -   irec
    """
    )
    return vc


@pytest.fixture()
def edge_config_ibes():
    vc = yaml.safe_load(
        """
        main:
        -   source: publication
            target: ticker
        -   source: analyst
            target: agency
            weight:
            -   datetime_review
            -   datetime_announce
        -   source: analyst
            target: publication
        -   source: publication
            target: recommendation
    """
    )
    return vc


@pytest.fixture()
def edge_config_ticker():
    ec = yaml.safe_load(
        """
        main:
        -   source: ticker
            target: feature
            weight:
                fields:
                -   t_obs
            vertex:
            -   name: feature
                fields:
                -   name
            index:
            -   fields:
                -   t_obs
                -   name
    """
    )
    return ec


@pytest.fixture()
def vertex_config_ticker():
    vc = yaml.safe_load(
        """
        collections:
            ticker:
                dbname: tickers
                fields:
                -   cusip
                -   cname
                -   oftic
                index:
                -   cusip
                -   cname
                -   oftic
            feature:
                dbname: features
                fields:
                -   name
                -   value
                index:
                -   name
                -   value
                extra_index:
                -   type: hash
                    unique: false
                    fields:
                    -   value
                -   type: hash
                    unique: false
                    fields:
                    -   name
                filters:
                -   or:
                    -   if_then:
                        -   field: name
                            foo: __eq__
                            value: Open
                        -   field: value
                            foo: __gt__
                            value: 0
                    -   if_then:
                        -   field: name
                            foo: __eq__
                            value: Close
                        -   field: value
                            foo: __gt__
                            value: 0
                -
                    field: name
                    foo: __ne__
                    value: Volume    
    """
    )
    return vc


@pytest.fixture()
def table_config_ticker():
    tc = yaml.safe_load(
        """
        tabletype: _all
        transforms:
        -   foo: round_str
            module: graflo.util.transform
            params:
                ndigits: 3
            input:
            -   Open
            dress:
                key: name
                value: value
        -   foo: round_str
            module: graflo.util.transform
            params:
                ndigits: 3
            input:
            -   Close
            dress:
                key: name
                value: value
        -   foo: int
            module: builtins
            input:
            -   Volume
            dress:
                key: name
                value: value
        -   foo: parse_date_yahoo
            module: graflo.util.transform
            map:
                Date: t_obs
        -   map:
                __ticker: oftic
    """
    )
    return tc
