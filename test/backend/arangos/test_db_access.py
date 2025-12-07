import logging

import pytest

from graflo.db import ConnectionManager

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def collection_name():
    return "collection0"


@pytest.fixture
def create_collection(create_db, conn_conf, test_db_name, collection_name):
    _ = create_db
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        db_client.create_collection(collection_name)


def test_create_db(create_db):
    _ = create_db


def test_create_collection(create_db, create_collection):
    _ = create_collection


def test_insert_return(
    conn_conf, create_db, create_collection, collection_name, test_db_name
):
    _ = create_collection
    conn_conf.database = test_db_name
    with ConnectionManager(connection_config=conn_conf) as db_client:
        docs = [{"value": i} for i in range(5)]
        query0 = db_client.insert_return_batch(docs, collection_name)
        cursor = db_client.execute(query0)
    for item in cursor:
        logger.info(item)
