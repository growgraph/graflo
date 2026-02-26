import pathlib

import pytest
import yaml
from suthing import FileHandle

from graflo import EdgeConfig
from graflo.architecture import VertexConfig


@pytest.fixture(scope="session", autouse=True)
def create_test_dirs():
    test_dirs = [
        "test/figs",
    ]

    for dir_path in test_dirs:
        pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

    yield


@pytest.fixture()
def vertex_pub():
    tc = yaml.safe_load(
        """
        name: publication
        dbname: publications
        fields:
        -   arxiv
        -   doi
        -   created
        -   data_source
        indexes:
        -   fields:
            -   arxiv
            -   doi
        -   unique: false
            fields:
            -   created
        -   unique: false
            fields:
            -   created
        filters:
        -   OR:
            -   IF_THEN:
                -   field: name
                    cmp_operator: "=="
                    value: Open
                -   field: value
                    cmp_operator: ">"
                    value: 0
            -   IF_THEN:
                -   field: name
                    cmp_operator: "=="
                    value: Close
                -   field: value
                    cmp_operator: ">"
                    value: 0
        transforms:
        -   foo: cast_ibes_analyst
            module: graflo.util.transform
            input:
            -   ANALYST
            output:
            -   last_name
            -   initial
    """
    )
    return tc


@pytest.fixture()
def vertex_helper():
    tc = yaml.safe_load(
        """
        name: analyst
    """
    )
    return tc


@pytest.fixture()
def vertex_helper_b():
    tc = yaml.safe_load(
        """
            fields:
            -   datetime_review
            -   datetime_announce
    """
    )
    return tc


@pytest.fixture()
def edge_with_weights():
    tc = yaml.safe_load(
        """
        source: analyst
        target: agency
        weights:
            vertices:
                -   
                    name: ticker
                    fields:
                        - cusip
                -
                    fields:
                        - datetime_review
                        - datetime_announce
    """
    )
    return tc


@pytest.fixture()
def edge_indexes():
    tc = yaml.safe_load(
        """
        source: entity
        target: entity
        purpose: aux
        indexes:
        -   
            fields:
            -   start_date
            -   end_date
        -   
            fields:
            -   spec
    """
    )
    return tc


@pytest.fixture()
def edge_with_vertex_indexes():
    tc = yaml.safe_load(
        """
        source: entity
        target: entity
        indexes:
        -   name: publication
        -   exclude_edge_endpoints: true
            unique: false
            name: publication
            fields:
            -   _key
    """
    )
    return tc


@pytest.fixture()
def index_a():
    tc = yaml.safe_load(
        """
    fields:
        -   start_date
        -   end_date
    """
    )
    return tc


@pytest.fixture()
def vertex_config_kg():
    vc = yaml.safe_load(
        """
    vertices:
    -   name: publication
        dbname: publications
        fields:
        -   arxiv
        -   doi
        -   created
        -   data_source
        identity:
        -   arxiv
        -   doi
    -   name: entity
        dbname: entities
        fields:
        -   linker_type
        -   ent_db_type
        -   id
        -   ent_type
        -   original_form
        -   description
        identity:
        -   id
        -   ent_type
    -   name: mention
        dbname: mentions
        fields:
        -   text
        identity:
        -   _key
    """
    )
    return vc


@pytest.fixture()
def edge_config_kg():
    tc = yaml.safe_load(
        """
    edges:
    -   source: entity
        target: entity
        index:
        -   name: publication
            fields:
            -   _key
        -   exclude_edge_endpoints: true
            unique: false
            fields:
            -   publication@_key
    -   source: entity
        target: entity
        purpose: aux
        index:
        -   fields:
            -   start_date
            -   end_date
        -   fields:
            -   spec
    -   source: mention
        target: entity
        index:
        -   name: publication
            fields:
            -   _key
    """
    )
    return tc


@pytest.fixture()
def resource_concept():
    mn = yaml.safe_load(
        """
        -   vertex: concept
        -   foo: split_keep_part
            module: graflo.util.transform
            params:
                sep: "/"
                keep: -1
            input:
            -   wikidata
            output:
            -   wikidata
    """
    )
    return mn


@pytest.fixture()
def schema_vc_openalex():
    tc = yaml.safe_load("""
    vertices:
    -   name: author
        dbname: authors
        fields:
        -   _key
        -   display_name
        -   updated_date
        identity:
        -   _key
    -   name: concept
        dbname: concepts
        fields:
        -   _key
        -   wikidata
        -   display_name
        -   level
        -   mag
        -   created_date
        -   updated_date
        identity:
        -   _key
    -   name: institution
        dbname: institutions
        fields:
        -   _key
        -   display_name
        -   country
        -   type
        -   ror
        -   grid
        -   wikidata
        -   mag
        -   created_date
        -   updated_date
        identity:
        -   _key
    -   name: source
        dbname: sources
        fields:
        -   _key
        -   issn_l
        -   type
        -   display_name
        -   created_date
        -   updated_date
        -   country_code
        identity:
        -   _key
    -   name: work
        dbname: works
        fields:
        -   _key
        -   doi
        -   title
        -   created_date
        -   updated_date
        -   publication_date
        -   publication_year
        identity:
        -   _key
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def resource_descend():
    tc = yaml.safe_load(
        """
        key: publications
        apply:
        - key: abc
          apply:
            name: a
        - vertex: work
        """
    )
    return tc


@pytest.fixture()
def action_node_edge():
    tc = yaml.safe_load(
        """
        source: source
        target: work
        relation: contains
        """
    )
    return tc


@pytest.fixture()
def action_node_transform():
    an = yaml.safe_load("""
        foo: parse_date_ibes
        module: graflo.util.transform
        input:
        -   ANNDATS
        -   ANNTIMS
        output:
        -   datetime_announce
    """)
    return an


@pytest.fixture()
def sample_openalex():
    an = FileHandle.load("test/data/json/openalex.works.json")
    return an


@pytest.fixture()
def vertex_config_collision():
    tc = yaml.safe_load("""
    vertex_config:
    vertices:
    -   name: person
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    -   name: company
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def sample_cross():
    an = yaml.safe_load("""
    -   name: John
        id: Apple
    -   name: Mary
        id: Oracle
    """)
    return an


@pytest.fixture()
def resource_cross():
    an = yaml.safe_load("""
    -   vertex: person
    -   vertex: company 
    -   map:
            name: id
            id: name
    """)
    return an


@pytest.fixture()
def resource_collision():
    an = yaml.safe_load("""
    -   vertex: person
    -   vertex: company 
    -   target_vertex: person
        map:
            name: id
    """)
    return an


@pytest.fixture()
def vertex_config_cross():
    tc = yaml.safe_load("""
    vertex_config:
    vertices:
    -   name: person
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    -   name: company
        fields:
        -   name
        indexes:
        -   fields:
            -   name
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def resource_cross_implicit():
    an = yaml.safe_load("""
    -   map:
            name: id
            id: name
    """)
    return an


@pytest.fixture()
def vc_openalex():
    tc = yaml.safe_load("""
    vertices:
    -   name: author
        dbname: authors
        fields:
        -   _key
        -   display_name
        indexes:
        -   fields:
            -   _key
    -   name: institution
        dbname: institutions
        fields:
        -   _key
        -   display_name
        -   country
        -   type
        indexes:
        -   fields:
            -   _key
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def sample_openalex_authors():
    sample = FileHandle.load("test/data/json/openalex.authors.json")
    return sample


@pytest.fixture()
def resource_openalex_authors():
    an = yaml.safe_load("""
    -   vertex: author
    -   name: keep_suffix_id
        foo: split_keep_part
        module: graflo.util.transform
        params:
            sep: "/"
            keep: -1
        input:
        -   id
        output:
        -   _key
    -   key: last_known_institution
        apply:
        -   vertex: institution   
        -   name: keep_suffix_id
    -   source: author
        target: institution
        weights:
            direct:
            -   updated_date
            -   created_date
    """)
    return an


@pytest.fixture()
def mention_data():
    return {
        "triple_index": {"hash": "7a440c01a1205de3dbf33fc244489016b7072d12"},
        "triple": [
            {
                "hash": "5e18cc3aa82dae330049e923aaba1978277e4758",
                "text": "habitat shifts",
                "role": "source",
            },
            {
                "hash": "0f0f2562463a606ad107b0faac431f71f4c7c253",
                "text": "occurs in",
                "role": "relation",
            },
            {
                "hash": "c7f68d9f1d0ad2d51a0aea2bb95b195fc7f62b78",
                "text": "paleogene",
                "role": "target",
            },
        ],
    }


@pytest.fixture()
def resource_kg_menton_triple():
    an = yaml.safe_load("""
    -   key: triple_index
        apply:
        -   vertex: mention
        -   map:
                hash: _key
    -   key: triple
        apply:
        -   apply:
            -   vertex: mention
            -   map:
                    hash: _key
                    role: _role
    -   source: mention
        target: mention
        match_source: triple_index
        match_target: triple
        weights:
            direct:
            -   _role
        indexes:
        -   fields:
            -   _role
    """)
    return an


@pytest.fixture()
def vertex_config_kg_mention():
    tc = yaml.safe_load("""
    vertex_config:
    vertices:
    -   name: mention
        dbname: mentions
        fields:
        -   text
        identity:
        -   _key
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def data_key_property():
    return [
        {
            "name": "0ad-data-common",
            "version": "0.0.26-1",
            "dependencies": {
                "depends": [
                    {"name": "fonts-dejavu-core"},
                    {"name": "fonts-freefont-ttf"},
                    {"name": "fonts-texgyre"},
                ],
                "depends_aliases": [
                    {"source": "fonts-dejavu-core", "target": "ttf-dejavu-core"},
                    {"source": "fonts-freefont-ttf", "target": "ttf-freefont"},
                    {"source": "fonts-texgyre", "target": "tex-gyre"},
                ],
                "pre-depends": [{"name": "dpkg", "version": ">= 1.15.6~"}],
                "suggests": [{"name": "0ad"}],
                "breaks": [{"name": "0ad-data", "version": "<< 0.0.12-1~"}],
            },
        }
    ]


@pytest.fixture()
def vertex_key_property():
    tc = yaml.safe_load(
        """
    vertex_config:
    vertices:
        -   name: package
            fields:
            -   name
            -   version
            indexes:
            -   fields:
                -   name
    """
    )
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def schema_vc_deb():
    tc = yaml.safe_load("""
    vertices:
    -   name: package
        fields:
        -   name
        -   version
        indexes:
        -   fields:
            -   name
    -   name: maintainer
        fields:
        -   name
        -   email
        indexes:
        -   fields:
            -   email
    -   name: bug
        fields:
        -   id
        -   subject
        -   severity
        -   date
        indexes:
        -   fields:
            -   id
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def vc_ticker():
    tc = yaml.safe_load(
        """
        vertices:
        -   name: ticker
            dbname: tickers
            fields:
            -   cusip
            -   cname
            -   oftic
            indexes:
            -   fields:
                -   cusip
                -   cname
                -   oftic
        -   name: feature
            dbname: features
            fields:
            -   name
            -   value
            indexes:
            -   fields:
                -   name
                -   value
            -   type: hash
                unique: false
                fields:
                -   value
            -   type: hash
                unique: false
                fields:
                -   name
    """
    )
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def ec_ticker():
    tc = yaml.safe_load(
        """
    edges:
    -   source: ticker
        target: feature
        weights:
            direct:
            -   t_obs
            vertices:
            -   name: feature
                fields:
                -   name
        indexes:
        -   fields:
            -   t_obs
            -   name
    """
    )
    return EdgeConfig.from_dict(tc)


@pytest.fixture()
def vc_ticker_filtered():
    tc = yaml.safe_load(
        """
        vertices:
        -   name: ticker
            dbname: tickers
            fields:
            -   cusip
            -   cname
            -   oftic
            indexes:
            -   fields:
                -   cusip
                -   cname
                -   oftic
        -   name: feature
            dbname: features
            fields:
            -   name
            -   value
            indexes:
            -   fields:
                -   name
                -   value
            -   type: hash
                unique: false
                fields:
                -   value
            -   type: hash
                unique: false
                fields:
                -   name
            filters:
            -   field: name
                foo: __ne__
                value: Volume                        
    """
    )
    return VertexConfig.from_dict(tc)
