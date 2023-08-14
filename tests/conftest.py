from contextlib import suppress

import pytest
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.manager import SplitgillClient


@pytest.fixture
def mongo_client() -> MongoClient:
    with MongoClient("mongo", 27017) as client:
        yield client
        database_names = client.list_database_names()
        for name in database_names:
            # the list_database_names function gives us back names like "admin" which we
            # can't drop, so catch any exceptions to avoid silly errors but provide
            # maximum clean up
            with suppress(Exception):
                client.drop_database(name)


@pytest.fixture
def mongo_database(mongo_client: MongoClient) -> Database:
    yield mongo_client["test"]


@pytest.fixture
def data_collection(mongo_database: Database) -> Collection:
    yield mongo_database["data"]


@pytest.fixture
def elasticsearch_client() -> Elasticsearch:
    with Elasticsearch("http://es:9200") as es:
        yield es
        es.indices.delete(index="*")
        index_templates = es.indices.get_index_template(name="*")
        for index_template in index_templates['index_templates']:
            with suppress(Exception):
                es.indices.delete_index_template(name=index_template["name"])


@pytest.fixture
def splitgill(
    mongo_client: MongoClient, elasticsearch_client: Elasticsearch
) -> SplitgillClient:
    return SplitgillClient(mongo_client, elasticsearch_client)


# these are all nabbed from wikipedia: https://en.wikipedia.org/wiki/GeoJSON#Geometries
@pytest.fixture
def geojson_point() -> dict:
    return {"type": "Point", "coordinates": (30.0, 10.0)}


@pytest.fixture
def geojson_linestring() -> dict:
    return {
        "type": "LineString",
        "coordinates": [(30.0, 10.0), (10.0, 30.0), (40.0, 40.0)],
    }


@pytest.fixture
def geojson_polygon() -> dict:
    return {
        "type": "Polygon",
        "coordinates": [
            [(30.0, 10.0), (40.0, 40.0), (20.0, 40.0), (10.0, 20.0), (30.0, 10.0)]
        ],
    }


@pytest.fixture
def geojson_holed_polygon() -> dict:
    return {
        "type": "Polygon",
        "coordinates": [
            [(35.0, 10.0), (45.0, 45.0), (15.0, 40.0), (10.0, 20.0), (35.0, 10.0)],
            [(20.0, 30.0), (35.0, 35.0), (30.0, 20.0), (20.0, 30.0)],
        ],
    }
