from unittest.mock import patch, Mock
from uuid import uuid4

from bson import ObjectId

from splitgill.diffing import diff, prepare
from splitgill.model import MongoRecord, VersionedData


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

        for actual, expected in zip(record.__iter__(), data):
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
        data = {"x": 5, "y": [1, 2, 3]}
        prepare_spy = Mock(wraps=prepare)

        with patch('splitgill.model.prepare', prepare_spy):
            record = MongoRecord(ObjectId(), str(uuid4()), 10, data)

        assert record.data == {"x": "5", "y": ("1", "2", "3")}
        prepare_spy.assert_called_once_with(data)
