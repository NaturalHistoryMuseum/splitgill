from unittest.mock import patch, Mock
from uuid import uuid4

from bson import ObjectId

from splitgill.diffing import diff, prepare
from splitgill.indexing.fields import geo_path
from splitgill.model import MongoRecord, VersionedData, GeoFieldHint, ParsingOptions


def create_mongo_record(version: int, data: dict, *historical_data: VersionedData):
    diffs = {}
    base = data
    for hv, hd in sorted(historical_data, key=lambda h: h.version, reverse=True):
        diffs[str(hv)] = list(diff(base, hd))
        base = hd

    return MongoRecord(ObjectId(), str(uuid4()), version, data, diffs)


class TestMongoRecord:
    def test_is_deleted_true(self):
        record = create_mongo_record(1, {})
        assert record.is_deleted

    def test_is_deleted_false(self):
        record = create_mongo_record(1, {"x": "beans"})
        assert not record.is_deleted

    def test_iter(self):
        data = [
            VersionedData(10, {"a": "1", "b": "2"}),
            VersionedData(7, {"a": "4", "b": "1"}),
            VersionedData(2, {"c": "1"}),
        ]

        record = create_mongo_record(data[0].version, data[0].data, *data[1:])

        for actual, expected in zip(record.iter(), data):
            assert expected == actual

    def test_versions(self):
        data = [
            VersionedData(10, {"a": "1", "b": "2"}),
            VersionedData(7, {"a": "4", "b": "1"}),
            VersionedData(2, {"c": "1"}),
        ]

        record = create_mongo_record(data[0].version, data[0].data, *data[1:])

        assert record.versions == [10, 7, 2]

    def test_is_prepared(self):
        data = {"x": 5, "y": [True, 2, "3"]}
        prepare_spy = Mock(wraps=prepare)

        with patch("splitgill.model.prepare", prepare_spy):
            record = MongoRecord(ObjectId(), str(uuid4()), 10, data)

        assert record.data == {"x": 5, "y": (True, 2, "3")}
        prepare_spy.assert_called_once_with(data)


class TestGeoFieldHint:
    def test_geo_path_with_radius(self):
        assert GeoFieldHint("latitude", "longitude", "radius").path == geo_path(
            "latitude", "longitude", "radius"
        )

    def test_geo_path_without_radius(self):
        assert GeoFieldHint("latitude", "longitude").path == geo_path(
            "latitude", "longitude"
        )

    def test_hash(self):
        hints = set()
        hints.add(GeoFieldHint("latitude", "longitude"))
        hints.add(GeoFieldHint("latitude", "longitude"))
        hints.add(GeoFieldHint("latitude", "longitude", None))
        assert len(hints) == 1
        hints.add(GeoFieldHint("latitude", "longitude", "radius"))
        hints.add(GeoFieldHint("latitude", "longitude", "radius"))
        assert len(hints) == 2


class TestParsingOptions:
    def test_from_to_doc_empty(self):
        options = ParsingOptions(frozenset(), frozenset(), frozenset(), frozenset())
        assert options == ParsingOptions.from_doc(options.to_doc())
