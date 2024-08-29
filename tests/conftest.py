import json
from os import getenv
from contextlib import suppress
from typing import List

import pytest
from elastic_transport import NodeConfig
from elasticsearch import Elasticsearch, AsyncElasticsearch
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from shapely import from_geojson

from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.manager import SplitgillClient, SplitgillDatabase
from splitgill.model import ParsingOptions


@pytest.fixture
def mongo_client() -> MongoClient:
    with MongoClient(getenv("SPLITGILL_MONGO_HOST", "mongo"), 27017) as client:
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
def mongo_collection(mongo_database: Database) -> Collection:
    yield mongo_database["test"]


@pytest.fixture
def es_node_configs() -> List[NodeConfig]:
    node_configs = [NodeConfig("http", getenv("SPLITGILL_ES_HOST", "es"), 9200)]

    yield node_configs

    with Elasticsearch(node_configs) as es:
        es.indices.delete(index="*")
        index_templates = es.indices.get_index_template(name="*")
        for index_template in index_templates["index_templates"]:
            with suppress(Exception):
                es.indices.delete_index_template(name=index_template["name"])


@pytest.fixture
def elasticsearch_client(es_node_configs: List[NodeConfig]) -> Elasticsearch:
    with Elasticsearch(es_node_configs) as es:
        yield es


@pytest.fixture
async def async_elasticsearch_client(
    es_node_configs: List[NodeConfig],
) -> AsyncElasticsearch:
    async with AsyncElasticsearch(es_node_configs) as es:
        yield es


@pytest.fixture
def splitgill(
    mongo_client: MongoClient, elasticsearch_client: Elasticsearch
) -> SplitgillClient:
    return SplitgillClient(mongo_client, elasticsearch_client)


@pytest.fixture
def database(splitgill: SplitgillClient) -> SplitgillDatabase:
    return splitgill.get_database("test-db")


@pytest.fixture
def geojson_point() -> dict:
    return {"type": "Point", "coordinates": [30, 10]}


@pytest.fixture
def geojson_linestring() -> dict:
    return {
        "type": "LineString",
        "coordinates": [[10, 10], [20, 10], [20, 20]],
    }


@pytest.fixture
def geojson_polygon() -> dict:
    return {
        "type": "Polygon",
        "coordinates": [[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]],
    }


@pytest.fixture
def geojson_holed_polygon() -> dict:
    # this is a lovely square with an hourglass like shape hole
    return {
        "type": "Polygon",
        "coordinates": [
            [[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]],
            [[12, 12], [14, 15], [12, 18], [18, 18], [16, 15], [18, 12], [12, 12]],
        ],
    }


@pytest.fixture
def wkt_point(geojson_point: dict) -> str:
    return from_geojson(json.dumps(geojson_point)).wkt


@pytest.fixture
def wkt_linestring(geojson_linestring: dict) -> str:
    return from_geojson(json.dumps(geojson_linestring)).wkt


@pytest.fixture
def wkt_polygon(geojson_polygon: dict) -> str:
    return from_geojson(json.dumps(geojson_polygon)).wkt


@pytest.fixture
def wkt_holed_polygon(geojson_holed_polygon: dict) -> str:
    return from_geojson(json.dumps(geojson_holed_polygon)).wkt


@pytest.fixture
def basic_options() -> ParsingOptions:
    return (
        ParsingOptionsBuilder()
        .with_keyword_length(8191)
        .with_float_format("{0:.15g}")
        .with_true_value("true")
        .with_true_value("yes")
        .with_true_value("y")
        .with_false_value("false")
        .with_false_value("no")
        .with_false_value("n")
        .with_date_format("%Y-%m-%d")
        .with_date_format("%Y-%m-%dT%H:%M:%S")
        .with_date_format("%Y-%m-%dT%H:%M:%S.%f")
        .with_date_format("%Y-%m-%d %H:%M:%S")
        .with_date_format("%Y-%m-%d %H:%M:%S.%f")
        .with_date_format("%Y-%m-%dT%H:%M:%S%z")
        .with_date_format("%Y-%m-%dT%H:%M:%S.%f%z")
        .with_date_format("%Y-%m-%d %H:%M:%S%z")
        .with_date_format("%Y-%m-%d %H:%M:%S.%f%z")
        .with_geo_hint("lat", "lon")
        .with_geo_hint("latitude", "longitude", "radius")
        .with_geo_hint(
            "decimalLatitude", "decimalLongitude", "coordinateUncertaintyInMeters"
        )
        .build()
    )


@pytest.fixture
def basic_options_builder(basic_options: ParsingOptions) -> ParsingOptionsBuilder:
    return ParsingOptionsBuilder(based_on=basic_options)
