import pytest
from elasticsearch import Elasticsearch

from splitgill.indexing.fields import DocumentField
from splitgill.indexing.templates import DATA_TEMPLATE
from splitgill.manager import SplitgillClient, SplitgillDatabase
from splitgill.model import Record
from splitgill.search import match_query


def test_date_index_template_is_valid(elasticsearch_client: Elasticsearch):
    # this is a simple test, it just confirms that the data index template is valid
    elasticsearch_client.indices.put_index_template(
        name="data-template", body=DATA_TEMPLATE
    )


def test_all_text(splitgill: SplitgillClient):
    database = SplitgillDatabase("test", splitgill)
    records = [Record.new({"a": "banana", "b": "apple", "c": 5.8, "d": True})]
    database.ingest(records, commit=True)
    database.sync()

    queries = ["banana", "apple", "5.8", "true"]
    for query in queries:
        count = database.search().filter(match_query(query)).count()
        assert count == 1


@pytest.fixture
def database_with_geo(splitgill: SplitgillClient):
    records = [
        # a couple of wkt points
        Record("r1", {"a": "POINT (22.8 3.3)", "b": "POINT (11.3 19.5)"}),
        # one geojson point
        Record("r2", {"a": {"coordinates": [14.2, -15.6], "type": "Point"}}),
        # a wkt polygon (centre is 7.15 6.1)
        Record(
            "r3",
            {"a": "POLYGON ((-2.6 14.3, -2.6 -2.1, 16.9 -2.1, 16.9 14.3, -2.6 14.3))"},
        ),
    ]
    database = SplitgillDatabase("test", splitgill)
    database.ingest(records, commit=True)
    database.sync()
    return database


class TestAllPoints:
    def test_simple(self, database_with_geo: SplitgillDatabase):
        # find just r2
        resp = (
            database_with_geo.search()
            .source(DocumentField.ID)
            .filter(
                "geo_bounding_box",
                **{
                    DocumentField.ALL_POINTS: {
                        "top_left": [9, -11],
                        "bottom_right": [18, -18],
                    }
                }
            )
            .execute()
        )
        assert len(resp.hits.hits) == 1
        assert resp.hits.hits[0]._source[DocumentField.ID] == "r2"

    def test_shape_miss(self, database_with_geo: SplitgillDatabase):
        # this will find r1 but not r3 because although the search area contains r3's
        # shape, it does not contain its point (i.e. the centre)
        resp = (
            database_with_geo.search()
            .source(DocumentField.ID)
            .filter(
                "geo_bounding_box",
                **{
                    DocumentField.ALL_POINTS: {
                        "top_left": [7, 23],
                        "bottom_right": [15, 11],
                    }
                }
            )
            .execute()
        )
        assert resp.hits.hits[0]._source[DocumentField.ID] == "r1"

    def test_shape_hit(self, database_with_geo: SplitgillDatabase):
        # this will find r1 and r3 because the search area contains r3's point (i.e.
        # its centre)
        resp = (
            database_with_geo.search()
            .source(DocumentField.ID)
            .filter(
                "geo_bounding_box",
                **{
                    DocumentField.ALL_POINTS: {
                        "top_left": [1.7, 24.1],
                        "bottom_right": [13.8, 2.6],
                    }
                }
            )
            .execute()
        )
        assert sorted(h._source[DocumentField.ID] for h in resp.hits.hits) == [
            "r1",
            "r3",
        ]


class TestAllShapes:
    def test_simple(self, database_with_geo: SplitgillDatabase):
        # find just r2
        resp = (
            database_with_geo.search()
            .source(DocumentField.ID)
            .filter(
                "geo_bounding_box",
                **{
                    DocumentField.ALL_SHAPES: {
                        "top_left": [9, -11],
                        "bottom_right": [18, -18],
                    }
                }
            )
            .execute()
        )
        assert len(resp.hits.hits) == 1
        assert resp.hits.hits[0]._source[DocumentField.ID] == "r2"

    def test_shape(self, database_with_geo: SplitgillDatabase):
        resp = (
            database_with_geo.search()
            .source(DocumentField.ID)
            .filter(
                "geo_bounding_box",
                **{
                    DocumentField.ALL_SHAPES: {
                        "top_left": [7, 23],
                        "bottom_right": [15, 11],
                    }
                }
            )
            .execute()
        )
        assert sorted(h._source[DocumentField.ID] for h in resp.hits.hits) == [
            "r1",
            "r3",
        ]
