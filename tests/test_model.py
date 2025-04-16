from uuid import uuid4

from bson import ObjectId

from splitgill.diffing import diff
from splitgill.model import (
    MongoRecord,
    VersionedData,
    GeoFieldHint,
    ParsingOptions,
)


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

    def test_get_versions(self):
        data = [
            VersionedData(10, {"a": "1", "b": "2"}),
            VersionedData(7, {"a": "4", "b": "1"}),
            VersionedData(2, {"c": "1"}),
        ]

        record = create_mongo_record(data[0].version, data[0].data, *data[1:])

        assert record.get_versions(desc=True) == [10, 7, 2]
        assert record.get_versions(desc=False) == [2, 7, 10]
        assert record.get_versions() == [2, 7, 10]


class TestGeoFieldHint:
    def test_hash(self):
        hints = set()
        hints.add(GeoFieldHint("latitude", "longitude"))
        hints.add(GeoFieldHint("latitude", "longitude"))
        hints.add(GeoFieldHint("latitude", "longitude", "radius"))
        hints.add(GeoFieldHint("latitude", "longitude", None))
        hints.add(GeoFieldHint("lat", "lon"))
        assert len(hints) == 2

    def test_eq(self):
        # should be equal based on the lat, nothing else
        assert GeoFieldHint("lat", "lon") == GeoFieldHint("lat", "lon")
        assert GeoFieldHint("lat", "lon") == GeoFieldHint("lat", "difflon")
        assert GeoFieldHint("lat", "lon", None) == GeoFieldHint("lat", "lon", "rad")


class TestParsingOptions:
    def test_from_to_doc_empty(self):
        options = ParsingOptions(
            frozenset(), frozenset(), frozenset(), frozenset(), 256, "{0:.15g}"
        )
        assert options == ParsingOptions.from_doc(options.to_doc())
