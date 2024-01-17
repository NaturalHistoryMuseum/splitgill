from random import randint
from uuid import uuid4

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

from splitgill.diffing import prepare, diff
from splitgill.ingest import generate_ops, FIND_SIZE
from splitgill.model import Record


def create_random_record() -> Record:
    return Record(str(uuid4()), {"x": randint(0, 1000), "y": str(uuid4())})


def commit_helper(data_collection: Collection, version: int):
    data_collection.update_many({"version": None}, {"$set": {"version": version}})


class TestGenerateOps:
    def test_no_records(self, data_collection: Collection):
        records = []
        ops = list(generate_ops(data_collection, records))
        assert len(ops) == 0

    def test_with_all_new_unique_records(self, data_collection: Collection):
        records = [create_random_record() for _ in range(10)]

        ops = list(generate_ops(data_collection, records))

        assert len(ops) == len(records)
        assert all(isinstance(op, InsertOne) for op in ops)

        for record, op in zip(records, ops):
            assert op._doc["id"] == record.id
            assert op._doc["version"] is None
            assert op._doc["data"] == prepare(record.data)

    def test_with_all_new_but_some_repeating_records(self, data_collection: Collection):
        records = [
            Record("6", {"x": 81}),
            Record("1", {"x": 5}),
            Record("1", {"x": 6}),
            Record("2", {"x": 5}),
            Record("1", {"x": 7}),
        ]

        ops = list(generate_ops(data_collection, records))

        assert len(ops) == 3
        for record, op in zip((records[0], records[4], records[3]), ops):
            assert op._doc["id"] == record.id
            assert op._doc["version"] is None
            assert op._doc["data"] == prepare(record.data)

    def test_update_existing_records(self, data_collection: Collection):
        # add some records
        old_version = 4
        old_records = [
            Record("1", {"x": 1}),
            Record("2", {"x": 2}),
            Record("3", {"x": 3}),
        ]
        data_collection.bulk_write(list(generate_ops(data_collection, old_records)))
        commit_helper(data_collection, old_version)
        assert data_collection.count_documents({}) == len(old_records)

        # update some of them
        new_records = [
            # no update to record 1
            Record("1", {"x": 1}),
            # updates to records 2 and 3 though
            Record("2", {"x": 10}),
            Record("3", {"x": 185}),
        ]
        ops = list(generate_ops(data_collection, new_records))
        data_collection.bulk_write(ops)

        # number of records shouldn't have changed
        assert data_collection.count_documents({}) == len(old_records)
        # all the ops should be UpdateOnes
        assert all(isinstance(op, UpdateOne) for op in ops)
        # there should be 2 changed records
        assert data_collection.count_documents({"version": None}) == 2

        for new_record, old_record in zip(new_records[1:], old_records[1:]):
            # sanity check
            assert new_record.id == old_record.id
            doc = data_collection.find_one({"id": new_record.id})
            assert doc["version"] is None
            assert doc["data"] == prepare(new_record.data)
            # to compare the diff we have to convert the tuples into lists
            assert doc["diffs"][str(old_version)] == [
                [list(diff_op.path), diff_op.ops]
                for diff_op in diff(prepare(new_record.data), prepare(old_record.data))
            ]

    def test_lots_of_records(self, data_collection: Collection):
        records = [create_random_record() for _ in range(FIND_SIZE * 5)]

        ops = list(generate_ops(data_collection, records))

        assert len(ops) == len(records)
        assert all(isinstance(op, InsertOne) for op in ops)

        for record, op in zip(records, ops):
            assert op._doc["id"] == record.id
            assert op._doc["version"] is None
            assert op._doc["data"] == prepare(record.data)

    def test_delete_of_non_existent_record(self, data_collection: Collection):
        records = [Record.new({})]
        ops = list(generate_ops(data_collection, records))
        assert len(ops) == 0

    def test_handling_uncommitted(self, data_collection: Collection):
        # add some data without committing it
        record = Record("1", {"x": 4})
        data_collection.bulk_write(list(generate_ops(data_collection, [record])))
        assert data_collection.count_documents({}) == 1

        # try to update the data with the same data
        assert not list(generate_ops(data_collection, [record]))

        # change the data without committing it
        record.data["x"] = 5
        data_collection.bulk_write(list(generate_ops(data_collection, [record])))
        assert data_collection.count_documents({}) == 1
        assert data_collection.find_one({"id": "1"})["data"] == {"x": 5}
        assert data_collection.find_one({"id": "1"})["version"] is None
        assert "diffs" not in data_collection.find_one({"id": "1"})

        # delete the uncommitted data
        record.data = {}
        data_collection.bulk_write(list(generate_ops(data_collection, [record])))
        assert data_collection.count_documents({}) == 0

    def test_handling_uncommitted_with_diffs(self, data_collection: Collection):
        # add some data and commit it
        record = Record("1", {"x": 4})
        data_collection.bulk_write(list(generate_ops(data_collection, [record])))
        commit_helper(data_collection, 6)

        # change the data without committing it
        record.data["x"] = 5
        data_collection.bulk_write(list(generate_ops(data_collection, [record])))

        # change the data back to the previous version's data
        record.data["x"] = 4
        data_collection.bulk_write(list(generate_ops(data_collection, [record])))
        assert data_collection.count_documents({}) == 1
        assert data_collection.find_one({"id": "1"})["data"] == {"x": 4}
        assert data_collection.find_one({"id": "1"})["version"] == 6
        assert "diffs" not in data_collection.find_one({"id": "1"})
