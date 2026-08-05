import pathlib

import pytest
import yaml
from suthing import FileHandle

from graflo.architecture import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig


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
        properties:
        -   arxiv
        -   doi
        -   created
        -   data_source
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
def vertex_config_kg():
    vc = yaml.safe_load(
        """
    vertices:
    -   name: publication
        properties:
        -   arxiv
        -   doi
        -   created
        -   data_source
        identity:
        -   arxiv
        -   doi
    -   name: entity
        properties:
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
        properties:
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
    -   source: entity
        target: entity
        relation: aux
    -   source: mention
        target: entity
    """
    )
    return tc


@pytest.fixture()
def resource_concept():
    mn = yaml.safe_load(
        """
        -   vertex: concept
        -   transform:
                call:
                    module: graflo.util.transform
                    foo: split_keep_part
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
        properties:
        -   _key
        -   display_name
        -   updated_date
        identity:
        -   _key
    -   name: concept
        properties:
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
        properties:
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
        properties:
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
        properties:
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
            transform:
              rename:
                x: y
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
        transform:
            call:
                module: graflo.util.transform
                foo: parse_date_ibes
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
    vertices:
    -   name: person
        properties:
        -   id
    -   name: company
        properties:
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
    -   transform:
            rename:
                name: id
                id: name
    """)
    return an


@pytest.fixture()
def resource_collision():
    return [
        {"vertex": "person", "from": {"id": "name"}},
        {"vertex": "company", "from": {"id": "id"}},
    ]


@pytest.fixture()
def vertex_config_cross():
    tc = yaml.safe_load("""
    vertices:
    -   name: person
        properties:
        -   id
    -   name: company
        properties:
        -   name
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def resource_cross_implicit():
    an = yaml.safe_load("""
    -   transform:
            rename:
                name: id
                id: name
    """)
    return an


@pytest.fixture()
def vc_openalex():
    tc = yaml.safe_load("""
    vertices:
    -   name: author
        properties:
        -   _key
        -   display_name
    -   name: institution
        properties:
        -   _key
        -   display_name
        -   country
        -   type
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
    -   transform:
            call:
                module: graflo.util.transform
                foo: split_keep_part
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
        -   transform:
                call:
                    module: graflo.util.transform
                    foo: split_keep_part
                    params:
                        sep: "/"
                        keep: -1
                    input:
                    -   id
                    output:
                    -   _key
    -   source: author
        target: institution
        properties:
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
        -   transform:
                rename:
                    hash: _key
    -   key: triple
        apply:
        -   apply:
            -   vertex: mention
            -   transform:
                    rename:
                        hash: _key
                        role: _role
    -   source: mention
        target: mention
        match_source: triple_index
        match_target: triple
        properties:
        -   _role
    """)
    return an


@pytest.fixture()
def vertex_config_kg_mention():
    tc = yaml.safe_load("""
    vertices:
    -   name: mention
        properties:
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
    vertices:
        -   name: package
            properties:
            -   name
            -   version
    """
    )
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def schema_vc_deb():
    tc = yaml.safe_load("""
    vertices:
    -   name: package
        properties:
        -   name
        -   version
    -   name: maintainer
        properties:
        -   name
        -   email
    -   name: bug
        properties:
        -   id
        -   subject
        -   severity
        -   date
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def vc_ticker():
    tc = yaml.safe_load(
        """
        vertices:
        -   name: ticker
            properties:
            -   cusip
            -   cname
            -   oftic
        -   name: feature
            properties:
            -   name
            -   value
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
        properties:
        -   t_obs
    """
    )
    return EdgeConfig.from_dict(tc)


@pytest.fixture()
def vc_ticker_filtered():
    tc = yaml.safe_load(
        """
        vertices:
        -   name: ticker
            properties:
            -   cusip
            -   cname
            -   oftic
        -   name: feature
            properties:
            -   name
            -   value
            filters:
            -   field: name
                foo: __ne__
                value: Volume                        
    """
    )
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def context_schema():
    """Schema exercising every shape schema-context traversal must handle.

    Contains a self-loop, parallel edges between one pair, an undirected edge, a
    cycle, an isolated vertex type, and one type of each identity mode.
    """
    from graflo.architecture.graph_types import Index
    from graflo.architecture.schema.core import CoreSchema
    from graflo.architecture.schema.database_features import DatabaseProfile
    from graflo.architecture.schema.document import Schema
    from graflo.architecture.schema.edge import Edge
    from graflo.architecture.schema.metadata import GraphMetadata
    from graflo.architecture.schema.vertex import Field, Vertex

    vertices = [
        Vertex(
            name="person",
            properties=[
                Field(name="email", type="string"),
                Field(name="name", type="string"),
                Field(name="age", type="int"),
                Field(name="bio", type="string"),
            ],
            identity=["email"],
        ),
        Vertex(
            name="company",
            properties=[
                Field(name="tax_id", type="string"),
                Field(name="title", type="string"),
            ],
            identity=["tax_id"],
        ),
        Vertex(
            name="city",
            properties=[Field(name="code", type="string")],
            identity=["code"],
        ),
        Vertex(
            name="doc",
            properties=[Field(name="body", type="string")],
            blank=True,
        ),
        Vertex(
            name="orphan",
            properties=[Field(name="tag", type="string")],
            identity=["tag"],
        ),
    ]
    edges = [
        # self-loop
        Edge(source="person", target="person", relation="knows"),
        # parallel edges: same endpoints, different relations
        Edge(source="person", target="company", relation="works_at"),
        Edge(source="person", target="company", relation="founded"),
        # undirected — traversable from its target even under OUT
        Edge(source="company", target="city", relation="hq_in", directed=False),
        Edge(source="person", target="city", relation="lives_in"),
        # relation-less edge: exercises the None-relation sort key
        Edge(source="doc", target="person", relation=None),
    ]
    return Schema(
        metadata=GraphMetadata(
            name="context-fixture", version="1.0.0", description="context test schema"
        ),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(vertices=vertices),
            edge_config=EdgeConfig(edges=edges),
        ),
        db_profile=DatabaseProfile(
            vertex_indexes={"person": [Index(fields=["name"])]},
        ),
    )
