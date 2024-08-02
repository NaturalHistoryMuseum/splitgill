from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from elasticsearch_dsl import Search, Q
from freezegun import freeze_time

from splitgill.indexing.fields import (
    DocumentField,
    DataField,
    ParsedField,
    ParsedType,
    DataType,
)
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.parser import parse
from splitgill.indexing.syncing import BulkOptions
from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    SplitgillDatabase,
    OPTIONS_COLLECTION_NAME,
    SearchVersion,
)
from splitgill.model import Record
from splitgill.search import create_version_query, number
from splitgill.utils import to_timestamp


class TestSplitgillClient:
    def test_database(self, splitgill: SplitgillClient):
        assert splitgill.get_mongo_database().name == MONGO_DATABASE_NAME

    def test_get_data_collection(self, splitgill: SplitgillClient):
        name = "test"
        assert splitgill.get_data_collection(name).name == f"data-{name}"

    def test_get_options_collection(self, splitgill: SplitgillClient):
        assert splitgill.get_options_collection().name == OPTIONS_COLLECTION_NAME

    def test_get_database(self, splitgill: SplitgillClient):
        name = "test"
        assert (
            splitgill.get_database(name).name == SplitgillDatabase(name, splitgill).name
        )


class TestSplitgillDatabaseGetCommittedVersion:
    def test_no_data_no_options(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_committed_version() is None

    def test_uncommitted_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.ingest(records, commit=False)
        assert database.get_committed_version() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_committed_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.ingest(records, commit=True)
        assert database.get_committed_version() == 1326542401000

    @freeze_time("2012-01-14 12:00:01")
    def test_mixed_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        version = 1326542401000
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.ingest(records, commit=True)
        assert database.get_committed_version() == version
        more_records = [
            # this one is new
            Record.new({"x": 1}),
            # this one is an update to one of the ones above
            Record(records[0].id, {"x": 100}),
        ]
        database.ingest(more_records, commit=False)
        assert database.get_committed_version() == version

    def test_uncommitted_options(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        options = ParsingOptionsBuilder().with_defaults().build()
        database.update_options(options, commit=False)
        assert database.get_committed_version() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_committed_options(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        options = ParsingOptionsBuilder().with_defaults().build()
        database.update_options(options, commit=True)
        assert database.get_committed_version() == 1326542401000

    @freeze_time("2012-01-14 12:00:01")
    def test_mixed_options(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        version = 1326542401000
        options = ParsingOptionsBuilder().with_defaults().build()
        database.update_options(options, commit=True)
        assert database.get_committed_version() == version
        new_options = (
            ParsingOptionsBuilder().with_defaults().with_true_value("aye").build()
        )
        database.update_options(new_options, commit=False)
        assert database.get_committed_version() == version

    def test_mixed_both(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.ingest(records, commit=False)
        options = ParsingOptionsBuilder().with_defaults().build()
        database.update_options(options, commit=False)

        # add the new stuff
        with freeze_time("2012-01-14 12:00:01"):
            version = database.commit()
            assert database.get_committed_version() == version

        # update the records
        with freeze_time("2012-01-14 12:00:05"):
            new_records = [Record.new({"x": 4})]
            database.ingest(new_records, commit=True)
        assert database.get_committed_version() == 1326542405000

        # update the options
        with freeze_time("2012-01-14 12:00:09"):
            new_options = (
                ParsingOptionsBuilder().with_defaults().with_true_value("aye").build()
            )
            database.update_options(new_options, commit=True)
        assert database.get_committed_version() == 1326542409000


class TestGetElasticsearchVersion:
    def test_no_docs(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_elasticsearch_version() is None

    def test_with_docs(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        versions = [
            # latest
            1692119228000,
            1681578428000,
            1673802428000,
            1579108028000,
        ]
        for version in versions:
            # make a bare-bones doc
            doc = {DocumentField.VERSION: version}
            splitgill.elasticsearch.index(
                index=database.indices.latest,
                document=doc,
                refresh=True,
            )
        assert database.get_elasticsearch_version() == versions[0]

    def test_with_deleted_docs(self, splitgill: SplitgillClient):
        # this imitates the scenario where all the records in the database have been
        # deleted and therefore there is no data in the latest index but there is in the
        # old data indices
        database = SplitgillDatabase("test", splitgill)
        versions = [
            # latest
            1692119228000,
            1681578428000,
            1673802428000,
            1579108028000,
        ]
        for version in versions:
            # make a bare-bones doc
            doc = {DocumentField.VERSION: version}
            splitgill.elasticsearch.index(
                # put these in not the latest index
                index=database.indices.get_arc("record-1"),
                document=doc,
                refresh=True,
            )
        assert database.get_elasticsearch_version() == versions[0]


class TestCommit:
    def test_nothing_to_commit(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.commit() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_new_records(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.ingest([Record.new({"x": 5})], commit=False)
        assert database.commit() == 1326542401000

    @freeze_time("2012-01-14 12:00:01")
    def test_new_options(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.update_options(
            ParsingOptionsBuilder().with_defaults().build(), commit=False
        )
        assert database.commit() == 1326542401000

    @freeze_time("2012-01-14 12:00:01")
    def test_both(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.ingest([Record.new({"x": 5})], commit=False)
        database.update_options(
            ParsingOptionsBuilder().with_defaults().build(), commit=False
        )
        assert database.commit() == 1326542401000


class TestIngest:
    def test_no_records(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.ingest([])
        assert database.get_committed_version() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_with_records(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        count = 103
        record_iter = (Record.new({"x": i}) for i in range(count))

        database.ingest(record_iter, commit=True)

        assert database.data_collection.count_documents({}) == count
        assert database.get_committed_version() == 1326542401000

    def test_same_record(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        record = Record("r1", {"x": 5, "y": False, "z": [1, 2, 3]})

        database.ingest([record], commit=True)
        added_record = database.data_collection.find_one({"id": "r1"})
        assert added_record["data"] == record.data
        assert "diffs" not in added_record

        # add the same record again
        database.ingest([record], commit=True)
        added_record_again = database.data_collection.find_one({"id": "r1"})
        assert added_record == added_record_again

    def test_same_record_tuples_and_lists(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        record = Record("r1", {"x": (1, 2, 3)})
        clean_data = {"x": [1, 2, 3]}

        database.ingest([record], commit=True)
        added_record = database.data_collection.find_one({"id": "r1"})
        assert added_record["data"] == clean_data
        assert "diffs" not in added_record

        # add the same record again
        database.ingest([record], commit=True)
        added_record_again = database.data_collection.find_one({"id": "r1"})
        assert added_record == added_record_again

        # add the same record again with a list instead of a tuple this time
        record.data = clean_data
        database.ingest([record], commit=True)
        added_record_again = database.data_collection.find_one({"id": "r1"})
        assert added_record == added_record_again

    @freeze_time("2012-01-14 12:00:01")
    def test_commit_and_is_default(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        database.ingest([Record.new({"x": 10})])

        assert database.get_committed_version() == 1326542401000

    def test_no_commit(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        database.ingest([Record.new({"x": 10})], commit=False)

        assert database.get_committed_version() is None

    def test_no_commit_when_error(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        # force bulk write to error when called
        database.data_collection.bulk_write = MagicMock(side_effect=Exception("oh no!"))

        with pytest.raises(Exception, match="oh no!"):
            database.ingest([Record.new({"x": 10})], commit=True)

        assert database.get_committed_version() is None


class TestSync:
    def test_nothing_to_sync(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        result = database.sync()

        assert not splitgill.elasticsearch.indices.exists(index=database.indices.latest)
        assert result.total == 0

    def test_everything_to_sync_many_workers(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        records = [Record.new({"x": i}) for i in range(1000)]
        database.ingest(records, commit=True)

        # these are silly numbers, but it'll make sure it works at least!
        result = database.sync(BulkOptions(worker_count=9, chunk_size=17))

        assert result.indexed == len(records)
        assert splitgill.elasticsearch.indices.exists(index=database.indices.latest)
        assert not splitgill.elasticsearch.indices.exists(index=database.indices.all)
        assert database.search().count() == len(records)

    def test_one_sync_then_another(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_1_time = datetime(2020, 7, 2, tzinfo=timezone.utc)
        version_1_records = [
            Record("r1", {"x": 5}),
            Record("r2", {"x": 10}),
            Record("r3", {"x": 15}),
            Record("r4", {"x": -1}),
            Record("r5", {"x": 1098}),
        ]
        # add some records at a specific version
        with freeze_time(version_1_time):
            database.ingest(version_1_records, commit=True)
        database.sync()
        assert (
            splitgill.elasticsearch.count(index=database.indices.latest)["count"] == 5
        )
        assert not splitgill.elasticsearch.indices.exists(index=database.indices.all)

        # the next day...
        version_2_time = datetime(2020, 7, 3, tzinfo=timezone.utc)
        version_2_records = [
            # a new record
            Record("another", {"x": 7}),
            # an update to the first record in the version 1 set of records
            Record("r1", {"x": 6}),
        ]
        # update the records
        with freeze_time(version_2_time):
            database.ingest(version_2_records, commit=True)
        database.sync()
        assert database.search().count() == 6
        assert (
            Search(
                using=splitgill.elasticsearch, index=database.indices.get_arc("r1")
            ).count()
            == 1
        )

    def test_sync_with_delete(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_1_time = datetime(2020, 7, 2, tzinfo=timezone.utc)
        version_1_records = [
            Record("r1", {"x": 5}),
            Record("r2", {"x": 10}),
            Record("r3", {"x": 15}),
            Record("r4", {"x": -1}),
            Record("r5", {"x": 1098}),
        ]
        # add some records at a specific version
        with freeze_time(version_1_time):
            database.ingest(version_1_records, commit=True)
        database.sync()
        assert database.search().count() == 5
        assert not splitgill.elasticsearch.indices.exists(index=database.indices.all)

        # the next day...
        version_2_time = datetime(2020, 7, 3, tzinfo=timezone.utc)
        version_2_records = [
            # a new record
            Record("another", {"x": 7}),
            # a delete to the second record in the version 1 set of records
            Record("r2", {}),
        ]
        # update the records
        with freeze_time(version_2_time):
            database.ingest(version_2_records, commit=True)
        database.sync()
        assert database.search().count() == 5
        assert (
            Search(
                using=splitgill.elasticsearch, index=database.indices.get_arc("r2")
            ).count()
            == 1
        )

        # the second record shouldn't be in the latest index
        assert not splitgill.elasticsearch.exists(
            id="r2", index=database.indices.latest
        )
        # but it should be in the old index
        assert splitgill.elasticsearch.exists(
            id=f"r2:{to_timestamp(version_1_time)}",
            index=database.indices.get_arc("r2"),
        )

    def test_sync_delete_non_existent(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        database.ingest(
            [
                Record.new({"x": 5}),
                Record.new({"x": 10}),
                Record.new({"x": 15}),
                # a delete
                Record.new({}),
            ],
            commit=True,
        )

        database.sync()

        assert database.search().count() == 3

    def test_incomplete_is_not_searchable(self, splitgill: SplitgillClient):
        called = 0

        def mock_parse(*args, **kwargs):
            # call the actual parse_for_index function 3 times and then on the 4th go
            # around, raise an exception
            nonlocal called
            called += 1
            if called < 4:
                return parse(*args, **kwargs)
            else:
                raise Exception("Something went wrong... on purpose!")

        with patch("splitgill.indexing.index.parse", side_effect=mock_parse):
            database = SplitgillDatabase("test", splitgill)
            records = [
                Record.new({"x": 5}),
                Record.new({"x": 10}),
                Record.new({"x": 15}),
                Record.new({"x": 8}),
            ]
            database.ingest(records)

            with pytest.raises(Exception, match="Something went wrong... on purpose!"):
                # add them one at a time so that some docs actually get to elasticsearch
                database.sync(BulkOptions(chunk_size=1))

        # an error occurred which should have prevented a refresh from being triggered
        # so the doc count should still be 0
        assert database.search().count() == 0

        # check that the refresh interval has been left as -1
        assert (
            splitgill.elasticsearch.indices.get_settings(index=database.indices.latest)[
                database.indices.latest
            ]["settings"]["index"]["refresh_interval"]
            == "-1"
        )

        # run another sync which doesn't error (we're outside of the patch context)
        database.sync()

        # now a refresh should have been triggered and the doc count should be 4
        assert database.search().count() == 4

        # and the refresh should have been reset (it either won't be in the settings or
        # it will be set to something other than -1, hence the get usage)
        assert (
            splitgill.elasticsearch.indices.get_settings(index=database.indices.latest)[
                database.indices.latest
            ]["settings"]["index"].get("refresh_interval")
            != "-1"
        )

    def test_incomplete_is_not_searchable_until_refresh(
        self, splitgill: SplitgillClient
    ):
        called = 0

        def mock_parse(*args, **kwargs):
            # call the actual parse_for_index function 3 times and then on the 4th go
            # around, raise an exception
            nonlocal called
            called += 1
            if called < 4:
                return parse(*args, **kwargs)
            else:
                raise Exception("Something went wrong... on purpose!")

        with patch("splitgill.indexing.index.parse", side_effect=mock_parse):
            database = SplitgillDatabase("test", splitgill)
            records = [
                Record.new({"x": 5}),
                Record.new({"x": 10}),
                Record.new({"x": 15}),
                Record.new({"x": 8}),
            ]
            database.ingest(records)

            with pytest.raises(Exception, match="Something went wrong... on purpose!"):
                # add them one at a time so that some docs actually get to elasticsearch
                database.sync(
                    BulkOptions(worker_count=1, chunk_size=1, buffer_multiplier=1)
                )

        splitgill.elasticsearch.indices.refresh(index=database.indices.latest)
        # we expect this to be 3, but we can't be sure because the exception we raise
        # might happen before, during, or after each bulk op request has been processed
        # by elasticsearch. It should definitely be less than 4!
        assert database.search().count() < 4

    def test_resync(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 5}),
            Record.new({"x": 10}),
            Record.new({"x": 15}),
            Record.new({"x": -1}),
            Record.new({"x": 1098}),
        ]
        database.ingest(records, commit=True)

        database.sync(resync=False)
        assert database.search().count() == len(records)

        # delete a couple of documents to cause mayhem
        database.search().params(refresh=True).filter(
            "terms", **{DocumentField.ID: [records[1].id, records[4].id]}
        ).delete()
        # check we deleted them
        assert database.search().count() == len(records) - 2

        # sync them back in
        database.sync(resync=True)
        assert database.search().count() == len(records)


def test_search(splitgill: SplitgillClient):
    database = SplitgillDatabase("test", splitgill)

    client = splitgill.elasticsearch
    latest = [database.indices.latest]
    wildcard = [database.indices.wildcard]

    assert database.search()._index == latest
    assert database.search(version=SearchVersion.latest)._index == latest
    assert database.search(version=SearchVersion.latest)._using == client
    assert not database.search(version=SearchVersion.latest).to_dict()

    assert database.search(version=SearchVersion.all)._index == wildcard
    assert database.search(version=SearchVersion.all)._using == client
    assert not database.search(version=SearchVersion.all).to_dict()

    assert database.search(version=5)._index == wildcard
    assert database.search(version=5)._using == client
    assert database.search(version=5).to_dict() == {
        "query": {DataType.BOOL: {"filter": [create_version_query(5).to_dict()]}}
    }


class TestGetFieldsData:
    def test_int(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 5}),
            Record.new({"a": 10}),
            Record.new({"b": 15}),
            Record.new({"b": -1}),
            Record.new({"b": 1098}),
            Record.new({"c": 33}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 3
        assert fields.get_data_field("a") == DataField("a", {DataType.INT: 2})
        assert fields.get_data_field("b") == DataField("b", {DataType.INT: 3})
        assert fields.get_data_field("c") == DataField("c", {DataType.INT: 1})

    def test_float(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 5.4}),
            Record.new({"a": 10.1}),
            Record.new({"b": 15.0}),
            Record.new({"b": -1.8}),
            Record.new({"b": 1098.124235}),
            Record.new({"c": 33.6}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 3
        assert fields.get_data_field("a") == DataField("a", {DataType.FLOAT: 2})
        assert fields.get_data_field("b") == DataField("b", {DataType.FLOAT: 3})
        assert fields.get_data_field("c") == DataField("c", {DataType.FLOAT: 1})

    def test_bool(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": True}),
            Record.new({"a": False}),
            Record.new({"b": True}),
            Record.new({"b": True}),
            Record.new({"b": False}),
            Record.new({"c": False}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 3
        assert fields.get_data_field("a") == DataField("a", {DataType.BOOL: 2})
        assert fields.get_data_field("b") == DataField("b", {DataType.BOOL: 3})
        assert fields.get_data_field("c") == DataField("c", {DataType.BOOL: 1})

    def test_str(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": "beans"}),
            Record.new({"a": "hammers"}),
            Record.new({"b": "eggs"}),
            Record.new({"b": "llamas"}),
            Record.new({"b": "goats"}),
            Record.new({"c": "books"}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 3
        assert fields.get_data_field("a") == DataField("a", {DataType.STR: 2})
        assert fields.get_data_field("b") == DataField("b", {DataType.STR: 3})
        assert fields.get_data_field("c") == DataField("c", {DataType.STR: 1})

    def test_dict(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"topA": {"a": 4}}),
            Record.new({"topA": {"a": 5}}),
            Record.new({"topB": {"a": 6}}),
            Record.new({"topB": {"a": 7}}),
            Record.new({"topB": {"a": 8}}),
            Record.new({"topC": {"a": 9}}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 6
        assert fields.get_data_field("topA") == DataField("topA", {DataType.DICT: 2})
        assert fields.get_data_field("topB") == DataField("topB", {DataType.DICT: 3})
        assert fields.get_data_field("topC") == DataField("topC", {DataType.DICT: 1})
        assert fields.get_data_field("topA.a") == DataField("topA.a", {DataType.INT: 2})
        assert fields.get_data_field("topB.a") == DataField("topB.a", {DataType.INT: 3})
        assert fields.get_data_field("topC.a") == DataField("topC.a", {DataType.INT: 1})

    def test_list(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [1, 2, 3]}),
            Record.new({"a": [1, "beans", 3]}),
            Record.new({"a": [1, False, True]}),
            Record.new({"a": [5.4]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 2
        assert fields.get_data_field("a") == DataField("a", {DataType.LIST: 4})
        assert fields.get_data_field("a.") == DataField(
            "a.",
            {DataType.INT: 3, DataType.FLOAT: 1, DataType.STR: 1, DataType.BOOL: 1},
        )
        assert fields.get_data_field("a.").is_list_member

    def test_mix(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 5}),
            Record.new({"a": 50.1}),
            Record.new({"b": "beans!"}),
            Record.new({"b": [1, 2, 3]}),
            Record.new({"b": {"x": 5.4, "y": True}}),
            Record.new({"b": {"x": "lemonade!", "y": False}}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 5

        assert fields.get_data_field("a") == DataField(
            "a", {DataType.INT: 1, DataType.FLOAT: 1}
        )
        assert fields.get_data_field("b") == DataField(
            "b", {DataType.STR: 1, DataType.LIST: 1, DataType.DICT: 2}
        )
        assert fields.get_data_field(f"b.") == DataField("b.", {DataType.INT: 1})
        assert fields.get_data_field("b.x") == DataField(
            "b.x", {DataType.FLOAT: 1, DataType.STR: 1}
        )
        assert fields.get_data_field("b.y") == DataField("b.y", {DataType.BOOL: 2})

    def test_list_of_dicts(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [{"a": 5}, {"a": 5.4}, {"b": True}]}),
            Record.new({"a": [{"a": "beans"}, {"a": 5.4}, {"b": 3.9}]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 4
        assert fields.get_data_field("a") == DataField("a", {DataType.LIST: 2})
        assert fields.get_data_field("a.") == DataField("a.", {DataType.DICT: 2})
        assert fields.get_data_field("a..a") == DataField(
            "a..a",
            {DataType.INT: 1, DataType.FLOAT: 2, DataType.STR: 1},
        )
        assert fields.get_data_field("a..b") == DataField(
            "a..b", {DataType.FLOAT: 1, DataType.BOOL: 1}
        )

    def test_list_of_lists(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [[1, 2, 3], [4, 5, 6], 9]}),
            Record.new({"a": [[9, 8, 7], [6, 5, 4], "organs"]}),
            Record.new({"a": [[1, 2, 3], [4, 5, 6], [True, False]]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 3
        assert fields.get_data_field("a") == DataField("a", {DataType.LIST: 3})
        assert fields.get_data_field("a.") == DataField(
            "a.", {DataType.LIST: 3, DataType.INT: 1, DataType.STR: 1}
        )
        assert fields.get_data_field("a..") == DataField(
            "a..", {DataType.BOOL: 1, DataType.INT: 3}
        )

    def test_deep_nesting(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        # ew
        records = [Record.new({"a": {"b": [[{"c": [{"d": 5}]}]]}})]
        database.ingest(records, commit=True)
        database.sync()

        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 7
        assert fields.get_data_field("a") == DataField("a", {DataType.DICT: 1})
        assert fields.get_data_field("a.b") == DataField("a.b", {DataType.LIST: 1})
        assert fields.get_data_field("a.b.") == DataField("a.b.", {DataType.LIST: 1})
        assert fields.get_data_field("a.b..") == DataField("a.b..", {DataType.DICT: 1})
        assert fields.get_data_field(f"a.b...c") == DataField(
            "a.b...c", {DataType.LIST: 1}
        )
        assert fields.get_data_field(f"a.b...c.") == DataField(
            "a.b...c.",
            {DataType.DICT: 1},
        )
        assert fields.get_data_field(f"a.b...c..d") == DataField(
            "a.b...c..d",
            {DataType.INT: 1},
        )

    def test_version(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        # add some records with integer values
        version_1_time = datetime(2020, 7, 2, tzinfo=timezone.utc)
        version_1_records = [Record("r1", {"x": 5}), Record("r2", {"x": 10})]
        with freeze_time(version_1_time):
            database.ingest(version_1_records, commit=True)
        database.sync()

        # the next day all the record values become bools, wild stuff
        version_2_time = datetime(2020, 7, 3, tzinfo=timezone.utc)
        version_2_records = [Record("r1", {"x": True}), Record("r2", {"x": False})]
        with freeze_time(version_2_time):
            database.ingest(version_2_records, commit=True)
        database.sync()

        # check the latest version where the values are all bools
        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 1
        assert fields.get_data_field("x") == DataField("x", {DataType.BOOL: 2})

        # then check the old version where the values are ints
        fields = database.get_fields(version=to_timestamp(version_1_time))
        assert len(list(fields.iter_data_fields())) == 1
        assert fields.get_data_field("x") == DataField("x", {DataType.INT: 2})

    def test_with_filter(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 1, "b": True}),
            Record.new({"a": 2, "b": 5.3}),
            Record.new({"a": 3, "b": "beans!"}),
            Record.new({"a": 4, "b": "2014-05-21"}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        # check the baseline
        fields = database.get_fields()
        assert len(list(fields.iter_data_fields())) == 2
        assert fields.get_data_field("a") == DataField("a", {DataType.INT: 4})
        assert fields.get_data_field("b") == DataField(
            "b", {DataType.BOOL: 1, DataType.FLOAT: 1, DataType.STR: 2}
        )

        # now check with some filters
        fields = database.get_fields(query=Q("term", **{number("a"): 1}))
        assert len(list(fields.iter_data_fields())) == 2
        assert fields.get_data_field("a") == DataField("a", {DataType.INT: 1})
        assert fields.get_data_field("b") == DataField("b", {DataType.BOOL: 1})

        fields = database.get_fields(query=Q("term", **{number("a"): 2}))
        assert len(list(fields.iter_data_fields())) == 2
        assert fields.get_data_field("a") == DataField("a", {DataType.INT: 1})
        assert fields.get_data_field("b") == DataField("b", {DataType.FLOAT: 1})

        fields = database.get_fields(query=Q("term", **{number("a"): 3}))
        assert len(list(fields.iter_data_fields())) == 2
        assert fields.get_data_field("a") == DataField("a", {DataType.INT: 1})
        assert fields.get_data_field("b") == DataField("b", {DataType.STR: 1})

        fields = database.get_fields(query=Q("term", **{number("a"): 4}))
        assert len(list(fields.iter_data_fields())) == 2
        assert fields.get_data_field("a") == DataField("a", {DataType.INT: 1})
        assert fields.get_data_field("b") == DataField("b", {DataType.STR: 1})


def pf(
    *path: str, n: int = 0, d: int = 0, b: int = 0, t: int = 0, g: int = 0
) -> ParsedField:
    counts = {
        ParsedType.NUMBER: n,
        ParsedType.DATE: d,
        ParsedType.BOOLEAN: b,
        ParsedType.TEXT: t,
        ParsedType.KEYWORD_CASE_INSENSITIVE: t,
        ParsedType.KEYWORD_CASE_SENSITIVE: t,
        ParsedType.GEO_SHAPE: g,
        ParsedType.GEO_POINT: g,
    }
    counts = {parsed_type: count for parsed_type, count in counts.items() if count > 0}
    return ParsedField(".".join(path), counts)


class TestGetParsedFields:
    def test_int(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 5}),
            Record.new({"a": 10}),
            Record.new({"b": 15}),
            Record.new({"b": -1}),
            Record.new({"b": 1098}),
            Record.new({"c": 33}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 3
        assert pf("a", n=2, t=2) in parsed_fields
        assert pf("b", n=3, t=3) in parsed_fields
        assert pf("c", n=1, t=1) in parsed_fields

    def test_float(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 5.4}),
            Record.new({"a": 10.1}),
            Record.new({"b": 15.0}),
            Record.new({"b": -1.8}),
            Record.new({"b": 1098.124235}),
            Record.new({"c": 33.6}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 3
        assert pf("a", n=2, t=2) in parsed_fields
        assert pf("b", n=3, t=3) in parsed_fields
        assert pf("c", n=1, t=1) in parsed_fields

    def test_date(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": "2010-01-06"}),
            Record.new({"a": "2010-01-06T13:11:47+05:00"}),
            Record.new({"b": "2010-01-06 13:11:47"}),
        ]
        database.ingest(records, commit=True)
        database.update_options(
            ParsingOptionsBuilder()
            .with_date_format("%Y-%m-%d")
            .with_date_format("%Y-%m-%dT%H:%M:%S%z")
            .with_date_format("%Y-%m-%d %H:%M:%S")
            .build()
        )
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 2
        assert pf("a", d=2, t=2) in parsed_fields
        assert pf("b", d=1, t=1) in parsed_fields

    def test_bool(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": True}),
            Record.new({"a": False}),
            Record.new({"b": True}),
            Record.new({"b": True}),
            Record.new({"b": False}),
            Record.new({"c": False}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 3
        assert pf("a", b=2, t=2) in parsed_fields
        assert pf("b", b=3, t=3) in parsed_fields
        assert pf("c", b=1, t=1) in parsed_fields

    def test_str(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": "beans"}),
            Record.new({"a": "hammers"}),
            Record.new({"b": "eggs"}),
            Record.new({"b": "llamas"}),
            Record.new({"b": "goats"}),
            Record.new({"c": "books"}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 3
        assert pf("a", t=2) in parsed_fields
        assert pf("b", t=3) in parsed_fields
        assert pf("c", t=1) in parsed_fields

    def test_dict(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"topA": {"a": 4}}),
            Record.new({"topA": {"a": 5}}),
            Record.new({"topB": {"a": 6}}),
            Record.new({"topB": {"a": 7}}),
            Record.new({"topB": {"a": 8}}),
            Record.new({"topC": {"a": 9}}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 3
        assert pf("topA", "a", n=2, t=2) in parsed_fields
        assert pf("topB", "a", n=3, t=3) in parsed_fields
        assert pf("topC", "a", n=1, t=1) in parsed_fields

    def test_list(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [1, 2, 3]}),
            Record.new({"a": [1, "beans", 3]}),
            Record.new({"a": [1, False, True]}),
            Record.new({"a": [5.4]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 1
        assert pf("a", n=4, t=4, b=1) in parsed_fields

    def test_mix(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 5}),
            Record.new({"a": 50.1}),
            Record.new({"b": "beans!"}),
            Record.new({"b": [1, 2, 3]}),
            Record.new({"b": {"x": 5.4, "y": True}}),
            Record.new({"b": {"x": "lemonade!", "y": False}}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 4
        assert pf("a", n=2, t=2) in parsed_fields
        assert pf("b", n=1, t=2) in parsed_fields
        assert pf("b", "x", n=1, t=2) in parsed_fields
        assert pf("b", "y", b=2, t=2) in parsed_fields

    def test_list_of_dicts(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [{"a": 5}, {"a": 5.4}, {"b": True}]}),
            Record.new({"a": [{"a": "beans"}, {"a": 5.4}, {"b": 3.9}]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 2
        assert pf("a", "a", n=2, t=2) in parsed_fields
        assert pf("a", "b", n=1, b=1, t=2) in parsed_fields

    def test_list_of_lists(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [[1, 2, 3], [4, 5, 6], 9]}),
            Record.new({"a": [[9, 8, 7], [6, 5, 4], "organs"]}),
            Record.new({"a": [[1, 2, 3], [4, 5, 6], [True, False]]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 1
        assert pf("a", n=3, b=1, t=3) in parsed_fields

    def test_deep_nesting(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        # ew
        records = [Record.new({"a": {"b": [[{"c": [{"d": 5}]}]]}})]
        database.ingest(records, commit=True)
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 1
        assert pf("a", "b", "c", "d", n=1, t=1) in parsed_fields

    def test_geo(self, splitgill: SplitgillClient, geojson_point: dict, wkt_point: str):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new(
                {
                    "geojson": geojson_point,
                    "wkt": wkt_point,
                    "lat": 30,
                    "lon": 60,
                    "rad": 100,
                }
            ),
        ]
        database.ingest(records, commit=True)
        database.update_options(
            ParsingOptionsBuilder().with_geo_hint("lat", "lon", "rad").build()
        )
        database.sync()

        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 7
        assert pf("geojson", g=1) in parsed_fields
        assert pf("geojson", "type", t=1) in parsed_fields
        assert pf("geojson", "coordinates", n=1, t=1) in parsed_fields
        assert pf("wkt", g=1, t=1) in parsed_fields
        assert pf("lat", g=1, t=1, n=1) in parsed_fields
        assert pf("lon", t=1, n=1) in parsed_fields
        assert pf("rad", t=1, n=1) in parsed_fields

    def test_version(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        # add some records with integer values
        version_1_time = datetime(2020, 7, 2, tzinfo=timezone.utc)
        version_1_records = [Record("r1", {"x": 5}), Record("r2", {"x": 10})]
        with freeze_time(version_1_time):
            database.ingest(version_1_records, commit=True)
        database.sync()

        # the next day all the record values become bools, wild stuff
        version_2_time = datetime(2020, 7, 3, tzinfo=timezone.utc)
        version_2_records = [Record("r1", {"x": True}), Record("r2", {"x": False})]
        with freeze_time(version_2_time):
            database.ingest(version_2_records, commit=True)
        database.sync()

        # check the latest version where the values are all bools
        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 1
        assert pf("x", b=2, t=2) in parsed_fields

        # then check the old version where the values are ints
        parsed_fields = list(
            database.get_fields(
                version=to_timestamp(version_1_time)
            ).iter_parsed_fields()
        )
        assert len(parsed_fields) == 1
        assert pf("x", n=2, t=2) in parsed_fields

    def test_with_filter(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": 1, "b": True}),
            Record.new({"a": 2, "b": 5.3}),
            Record.new({"a": 3, "b": "beans!"}),
            Record.new({"a": 4, "b": "2014-05-21"}),
        ]
        database.ingest(records, commit=True)
        database.update_options(
            ParsingOptionsBuilder().with_date_format("%Y-%m-%d").build()
        )
        database.sync()

        # check the baseline
        parsed_fields = list(database.get_fields().iter_parsed_fields())
        assert len(parsed_fields) == 2
        assert pf("a", n=4, t=4) in parsed_fields
        assert pf("b", n=1, b=1, d=1, t=4) in parsed_fields

        # now check with some filters
        parsed_fields = list(
            database.get_fields(
                query=Q("term", **{number("a"): 1})
            ).iter_parsed_fields()
        )
        assert len(parsed_fields) == 2
        assert pf("a", n=1, t=1) in parsed_fields
        assert pf("b", b=1, t=1) in parsed_fields

        parsed_fields = list(
            database.get_fields(
                query=Q("term", **{number("a"): 2})
            ).iter_parsed_fields()
        )
        assert len(parsed_fields) == 2
        assert pf("a", n=1, t=1) in parsed_fields
        assert pf("b", n=1, t=1) in parsed_fields

        parsed_fields = list(
            database.get_fields(
                query=Q("term", **{number("a"): 3})
            ).iter_parsed_fields()
        )
        assert len(parsed_fields) == 2
        assert pf("a", n=1, t=1) in parsed_fields
        assert pf("b", t=1) in parsed_fields

        parsed_fields = list(
            database.get_fields(
                query=Q("term", **{number("a"): 4})
            ).iter_parsed_fields()
        )
        assert len(parsed_fields) == 2
        assert pf("a", n=1, t=1) in parsed_fields
        assert pf("b", d=1, t=1) in parsed_fields
