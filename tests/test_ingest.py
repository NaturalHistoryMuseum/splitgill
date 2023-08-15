from random import randint
from uuid import uuid4

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

from splitgill.diffing import prepare, diff
from splitgill.ingest import generate_ops
from splitgill.model import Record


def create_random_record() -> Record:
    return Record(str(uuid4()), {"x": randint(0, 1000), "y": str(uuid4())})


class TestGenerateOps:
    def test_no_records(self, data_collection: Collection):
        records = []
        version = 1

        ops = list(generate_ops(data_collection, records, version))

        assert len(ops) == 0

    def test_with_all_new_unique_records(self, data_collection: Collection):
        records = [create_random_record() for _ in range(10)]
        version = 1

        ops = list(generate_ops(data_collection, records, version))

        assert len(ops) == len(records)
        assert all(isinstance(op, InsertOne) for op in ops)

        for record, op in zip(records, ops):
            assert op._doc["id"] == record.id
            assert op._doc["version"] == version
            assert op._doc["data"] == prepare(record.data)

    def test_with_all_new_but_some_repeating_records(self, data_collection: Collection):
        records = [
            Record("6", {"x": 81}),
            Record("1", {"x": 5}),
            Record("1", {"x": 6}),
            Record("2", {"x": 5}),
            Record("1", {"x": 7}),
        ]
        version = 1

        ops = list(generate_ops(data_collection, records, version))

        assert len(ops) == 3
        for record, op in zip((records[0], records[4], records[3]), ops):
            assert op._doc["id"] == record.id
            assert op._doc["version"] == version
            assert op._doc["data"] == prepare(record.data)

    def test_update_existing_records(self, data_collection: Collection):
        existing_records = [
            Record("1", {"x": 1}),
            Record("2", {"x": 2}),
            Record("3", {"x": 3}),
        ]
        first_version = 1
        data_collection.bulk_write(
            list(generate_ops(data_collection, existing_records, first_version))
        )
        assert data_collection.count_documents({}) == len(existing_records)

        new_records = [
            # no update to record 1
            Record("1", {"x": 1}),
            # updates to records 2 and 3 though
            Record("2", {"x": 10}),
            Record("3", {"x": 185}),
        ]
        new_version = 4
        ops = list(generate_ops(data_collection, new_records, new_version))

        assert len(ops) == 2
        assert data_collection.count_documents({}) == len(existing_records)

        for record, op, existing in zip(new_records[1:], ops, existing_records[1:]):
            assert isinstance(op, UpdateOne)
            assert op._filter["id"] == record.id
            assert op._doc["$set"] == {
                "data": prepare(record.data),
                "version": new_version,
                f"diffs.{first_version}": tuple(
                    diff(prepare(record.data), prepare(existing.data))
                ),
            }

    def test_old_version(self, data_collection: Collection):
        existing_records = [
            Record("1", {"x": 1}),
            Record("2", {"x": 2}),
        ]
        first_version = 10
        data_collection.bulk_write(
            list(generate_ops(data_collection, existing_records, first_version))
        )
        assert data_collection.count_documents({}) == len(existing_records)

        new_records = [
            Record("1", {"x": 100}),
            Record("2", {"x": 101}),
        ]
        # this version is less than the first version
        new_version = 4
        ops = list(generate_ops(data_collection, new_records, new_version))
        assert len(ops) == 0
