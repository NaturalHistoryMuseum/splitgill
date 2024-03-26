from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from elasticsearch_dsl import Search
from freezegun import freeze_time

from splitgill.indexing import fields
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.parser import parse_for_index
from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    SplitgillDatabase,
    OPTIONS_COLLECTION_NAME,
    SearchVersion,
)
from splitgill.model import Record
from splitgill.search import create_version_query
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
            doc = {fields.VERSION: version}
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
            doc = {fields.VERSION: version}
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
        result = database.sync(worker_count=9, chunk_size=17)

        assert result.indexed == len(records)
        assert splitgill.elasticsearch.indices.exists(index=database.indices.latest)
        assert not splitgill.elasticsearch.indices.exists(index=database.indices.all)
        assert database.search().count() == len(records)

    def test_one_sync_then_another(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_1_time = datetime(2020, 7, 2)
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
        version_2_time = datetime(2020, 7, 3)
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

        version_1_time = datetime(2020, 7, 2)
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
        version_2_time = datetime(2020, 7, 3)
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

        def mock_parse_for_index(*args, **kwargs):
            # call the actual parse_for_index function 3 times and then on the 4th go
            # around, raise an exception
            nonlocal called
            called += 1
            if called < 4:
                return parse_for_index(*args, **kwargs)
            else:
                raise Exception("Something went wrong... on purpose!")

        with patch(
            "splitgill.indexing.index.parse_for_index", side_effect=mock_parse_for_index
        ):
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
                database.sync(chunk_size=1)

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

        def mock_parse_for_index(*args, **kwargs):
            # call the actual parse_for_index function 3 times and then on the 4th go
            # around, raise an exception
            nonlocal called
            called += 1
            if called < 4:
                return parse_for_index(*args, **kwargs)
            else:
                raise Exception("Something went wrong... on purpose!")

        with patch(
            "splitgill.indexing.index.parse_for_index", side_effect=mock_parse_for_index
        ):
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
                database.sync(worker_count=1, chunk_size=1, buffer_multiplier=1)

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
            "terms", **{fields.ID: [records[1].id, records[4].id]}
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
        "query": {"bool": {"filter": [create_version_query(5).to_dict()]}}
    }
