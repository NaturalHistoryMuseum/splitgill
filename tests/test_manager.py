from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from freezegun import freeze_time

from splitgill.indexing import fields
from splitgill.indexing.index import get_data_index_id, create_index_op
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    SplitgillDatabase,
)
from splitgill.model import Record
from splitgill.utils import to_timestamp


class TestSplitgillClient:
    def test_database(self, splitgill: SplitgillClient):
        assert splitgill.get_database().name == MONGO_DATABASE_NAME

    def test_get_data_collection(self, splitgill: SplitgillClient):
        name = "test"
        assert splitgill.get_data_collection(name).name == f"data-{name}"


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
                index=database.latest_index_name,
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
                index=get_data_index_id(database.name, version),
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


class TestGetAllIndices:
    def test_no_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_all_indices() == [database.latest_index_name]

    def test_some_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        date_2015 = datetime(2015, 5, 20, 6, 3, 10)
        date_2021 = datetime(2021, 10, 9, 19, 54, 0)

        # add a record from 2015
        with freeze_time(date_2015):
            database.ingest(
                [
                    Record.new({"x": 6}),
                ],
                commit=True,
            )

        # add a record from 2021
        with freeze_time(date_2021):
            database.ingest(
                [
                    Record.new({"x": 5}),
                ],
                commit=True,
            )

        assert database.get_all_indices() == [
            database.latest_index_name,
            get_data_index_id("test", to_timestamp(date_2021)),
            get_data_index_id("test", to_timestamp(date_2015)),
        ]


class TestSync:
    def test_nothing_to_sync(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.sync()

        assert not splitgill.elasticsearch.indices.exists(
            index=database.latest_index_name
        )

    def test_everything_to_sync_single_threaded(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_time = datetime(2020, 7, 2)

        with freeze_time(version_time):
            database.ingest(
                [
                    Record.new({"x": 5}),
                    Record.new({"x": 10}),
                    Record.new({"x": 15}),
                    Record.new({"x": -1}),
                ],
                commit=True,
            )

        database.sync(parallel=False)

        assert splitgill.elasticsearch.indices.exists(index=database.latest_index_name)
        assert not splitgill.elasticsearch.indices.exists(
            index=get_data_index_id(database.name, to_timestamp(version_time))
        )
        assert (
            splitgill.elasticsearch.count(index=database.latest_index_name)["count"]
            == 4
        )

    def test_everything_to_sync(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_time = datetime(2020, 7, 2)

        with freeze_time(version_time):
            database.ingest(
                [
                    Record.new({"x": 5}),
                    Record.new({"x": 10}),
                    Record.new({"x": 15}),
                    Record.new({"x": -1}),
                ],
                commit=True,
            )

        database.sync()

        assert splitgill.elasticsearch.indices.exists(index=database.latest_index_name)
        assert not splitgill.elasticsearch.indices.exists(
            index=get_data_index_id(database.name, to_timestamp(version_time))
        )
        assert (
            splitgill.elasticsearch.count(index=database.latest_index_name)["count"]
            == 4
        )

    def test_one_sync_then_another(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_1_time = datetime(2020, 7, 2)
        version_1_records = [
            Record.new({"x": 5}),
            Record.new({"x": 10}),
            Record.new({"x": 15}),
            Record.new({"x": -1}),
        ]
        latest_index = database.latest_index_name
        old_2020_index = get_data_index_id(database.name, to_timestamp(version_1_time))

        # add some records at a specific version
        with freeze_time(version_1_time):
            database.ingest(version_1_records, commit=True)

        database.sync()
        assert splitgill.elasticsearch.count(index=latest_index)["count"] == 4
        assert not splitgill.elasticsearch.indices.exists(index=old_2020_index)

        # the next day...
        version_2_time = datetime(2020, 7, 3)
        version_2_records = [
            # a new record
            Record.new({"x": 7}),
            # an update to the first record in the version 1 set of records
            Record(version_1_records[0].id, {"x": 6}),
        ]

        # update the records
        with freeze_time(version_2_time):
            database.ingest(version_2_records, commit=True)

        database.sync()

        assert splitgill.elasticsearch.count(index=latest_index)["count"] == 5
        assert splitgill.elasticsearch.count(index=old_2020_index)["count"] == 1

    def test_sync_with_delete(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        version_1_time = datetime(2020, 7, 2)
        version_1_records = [
            Record.new({"x": 5}),
            Record.new({"x": 10}),
            Record.new({"x": 15}),
            Record.new({"x": -1}),
        ]
        latest_index = database.latest_index_name
        old_2020_index = get_data_index_id(database.name, to_timestamp(version_1_time))

        # add some records at a specific version
        with freeze_time(version_1_time):
            database.ingest(version_1_records, commit=True)

        database.sync()
        assert splitgill.elasticsearch.count(index=latest_index)["count"] == 4
        assert not splitgill.elasticsearch.indices.exists(index=old_2020_index)

        # the next day...
        version_2_time = datetime(2020, 7, 3)
        version_2_records = [
            # a new record
            Record.new({"x": 7}),
            # delete a record!
            Record(version_1_records[2].id, {}),
        ]

        # update the records
        with freeze_time(version_2_time):
            database.ingest(version_2_records, commit=True)

        database.sync()

        assert splitgill.elasticsearch.count(index=latest_index)["count"] == 4
        assert splitgill.elasticsearch.count(index=old_2020_index)["count"] == 1

        # the second record shouldn't be in the latest index
        assert not splitgill.elasticsearch.exists(
            id=version_2_records[1].id, index=latest_index
        )
        # but it should be in the old index
        assert splitgill.elasticsearch.exists(
            id=f"{version_2_records[1].id}:{to_timestamp(version_1_time)}",
            index=old_2020_index,
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

        assert (
            splitgill.elasticsearch.count(index=database.latest_index_name)["count"]
            == 3
        )

    def test_incomplete_is_not_searchable(self, splitgill: SplitgillClient):
        called = 0

        def mock_create_index_op(*args, **kwargs):
            # call the actual create_index_op function 3 times and then on the 4th go
            # around, raise an exception
            nonlocal called
            called += 1
            if called < 4:
                return create_index_op(*args, **kwargs)
            else:
                raise Exception("Something went wrong... on purpose!")

        with patch(
            "splitgill.indexing.index.create_index_op", side_effect=mock_create_index_op
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
        assert (
            splitgill.elasticsearch.count(index=database.latest_index_name)["count"]
            == 0
        )
        # check that the refresh interval has been left as -1
        assert (
            splitgill.elasticsearch.indices.get_settings(
                index=database.latest_index_name
            )[database.latest_index_name]["settings"]["index"]["refresh_interval"]
            == "-1"
        )

        # run another sync which doesn't error (we're outside of the patch context)
        database.sync()

        # now a refresh should have been triggered and the doc count should be 4
        assert (
            splitgill.elasticsearch.count(index=database.latest_index_name)["count"]
            == 4
        )
        # and the refresh should have been reset (it either won't be in the settings or
        # it will be set to something other than -1, hence the get usage)
        assert (
            splitgill.elasticsearch.indices.get_settings(
                index=database.latest_index_name
            )[database.latest_index_name]["settings"]["index"].get("refresh_interval")
            != "-1"
        )

    def test_incomplete_is_not_searchable_until_refresh(
        self, splitgill: SplitgillClient
    ):
        called = 0

        def mock_create_index_op(*args, **kwargs):
            # call the actual create_index_op function 3 times and then on the 4th go
            # around, raise an exception
            nonlocal called
            called += 1
            if called < 4:
                return create_index_op(*args, **kwargs)
            else:
                raise Exception("Something went wrong... on purpose!")

        with patch(
            "splitgill.indexing.index.create_index_op", side_effect=mock_create_index_op
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
                database.sync(chunk_size=1, parallel=False)

        splitgill.elasticsearch.indices.refresh(index=database.latest_index_name)
        doc_count = splitgill.elasticsearch.count(index=database.latest_index_name)
        # check that the number of docs available for search is more than 0 but fewer
        # than 4. Ideally this would be 3 because we allow 3 create_index_op calls to
        # complete and then raise an exception, however, the way that chunks are sent to
        # elasticsearch means that it's not 3, it's actually 2. The _ActionChunker class
        # in the elasticsearch library doesn't send the first op immediately even though
        # the chunk size is 1 and only sends it after creating the next op so it ends up
        # only sending 2 ops even though it's generate 4. As you can see from that
        # explanation this is an elasticsearch library internal and therefore relying on
        # it always being this way is foolish, therefore we just check that more than 1
        # doc has made it and fewer than 4 (even this is a bit dubious but it's pretty
        # solid).
        assert 0 < doc_count["count"] < 4
