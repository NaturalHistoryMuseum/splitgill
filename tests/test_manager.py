from datetime import datetime
from unittest.mock import patch, MagicMock, Mock

import pytest
from freezegun import freeze_time

from splitgill.indexing.fields import RootField, MetaField
from splitgill.indexing.index import get_data_index_id, create_index_op
from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    STATUS_COLLECTION_NAME,
    SplitgillDatabase,
    OPS_SIZE,
)
from splitgill.model import Record, Status
from splitgill.utils import to_timestamp


class TestSplitgillClient:
    def test_database(self, splitgill: SplitgillClient):
        assert splitgill.get_database().name == MONGO_DATABASE_NAME

    def test_get_status_collection(self, splitgill: SplitgillClient):
        assert splitgill.get_status_collection().name == STATUS_COLLECTION_NAME

    def test_get_data_collection(self, splitgill: SplitgillClient):
        name = "test"
        assert splitgill.get_data_collection(name).name == f"data-{name}"


class TestSplitgillDatabaseCommittedVersion:
    def test_no_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.committed_version is None

    def test_data_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.add(records, commit=False)
        assert database.committed_version is None

    @freeze_time("2012-01-14 12:00:01")
    def test_data_with_status(self, splitgill: SplitgillClient):
        version = 1326542401000
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.add(records, commit=True)
        assert database.committed_version == version

    def test_data_with_status_different_to_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]

        with freeze_time("2012-01-14 12:00:01"):
            database.add(records, commit=True)
        assert database.committed_version == 1326542401000

        another_record = Record.new({"x": 199})
        with freeze_time("2016-02-15 12:00:01"):
            database.add([another_record], commit=False)
        # data version shouldn't have changed, even though the version in the data
        # collection will be newer
        assert database.committed_version == 1326542401000


class TestGetMongoVersion:
    def test_no_docs(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_mongo_version() is None

    def test_with_docs(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.data_collection.insert_many(
            [
                {"version": 4},
                {"version": 10000},
                {"version": 4892},
                {"version": 100},
            ]
        )
        assert database.get_mongo_version() == 10000


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
            # make some bare-bones docs
            doc = {
                RootField.META: {MetaField.VERSION: version},
            }
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
            # make some bare-bones docs
            doc = {
                RootField.META: {MetaField.VERSION: version},
            }
            splitgill.elasticsearch.index(
                # put these in not the latest index
                index=get_data_index_id(database.name, version),
                document=doc,
                refresh=True,
            )
        assert database.get_elasticsearch_version() == versions[0]


class TestSplitgillDatabaseGetStatus:
    def test_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_status() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_get(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.add([Record.new({"x": 5})], commit=True)
        status = database.get_status()
        assert status is not None
        assert status.name == database.name
        assert status.version == 1326542401000

    def test_delete_status_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.clear_status()
        assert database.get_status() is None

    def test_delete_status(self, splitgill: SplitgillClient):
        name = "test"
        database = SplitgillDatabase(name, splitgill)
        database.add([Record.new({"x": 5})], commit=True)

        assert database.get_status() is not None
        database.clear_status()
        assert database.get_status() is None


class TestCommit:
    def test_nothing_to_commit(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert not database.commit()
        assert database.get_status() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.add([Record.new({"x": 5})], commit=False)

        assert database.commit()
        assert database.get_status().version == 1326542401000

    def test_update_status(self, splitgill: SplitgillClient):
        name = "test"
        database = SplitgillDatabase(name, splitgill)
        database.add([Record.new({"x": 5})], commit=False)
        database.commit()

        first_status = database.get_status()
        assert first_status is not None

        database.add([Record.new({"x": 5})], commit=False)
        database.commit()

        second_status = database.get_status()
        assert second_status.version > first_status.version


class TestDetermineNextStatus:
    def test_no_status_no_data_version(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        with patch("splitgill.manager.now", MagicMock(return_value=100)):
            assert database.determine_next_version() == 100

    def test_no_status_has_data_version(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        with freeze_time("2012-01-14 12:00:01"):
            database.add([Record.new({"x": 5})], commit=False)
        assert database.determine_next_version() == 1326542401000

    def test_has_status_no_data_version(self, splitgill: SplitgillClient):
        name = "test"
        database = SplitgillDatabase(name, splitgill)
        database.get_status = MagicMock(return_value=Status(name, 100))
        database.clear_status = MagicMock()
        with freeze_time("2012-01-14 12:00:01"):
            assert database.determine_next_version() == 1326542401000
        database.clear_status.assert_called_once()

    def test_has_status_and_data_version_continuation(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        with freeze_time("2012-01-02 12:00:01"):
            database.add([Record.new({"x": 5})], commit=True)

        with freeze_time("2012-01-14 12:00:01"):
            database.add([Record.new({"x": 5})], commit=False)

        assert database.determine_next_version() == 1326542401000

    def test_has_status_and_data_version_new_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        with freeze_time("2012-01-02 12:00:01"):
            database.add([Record.new({"x": 5})], commit=True)

        with freeze_time("2012-01-14 12:00:01"):
            assert database.determine_next_version() == 1326542401000


class TestAdd:
    def test_no_records(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.add([])
        assert database.committed_version is None

    @freeze_time("2012-01-14 12:00:01")
    def test_with_records(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        count = 100
        record_iter = (Record.new({"x": i}) for i in range(count))

        database.add(record_iter)

        assert database.data_collection.count_documents({}) == count
        assert database.committed_version == 1326542401000
        assert database.get_status().version == 1326542401000

    def test_is_batched(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        batches_goal = 5
        count = OPS_SIZE * batches_goal
        record_iter = (Record.new({"x": i}) for i in range(count))

        bulk_write_spy = Mock(wraps=database.data_collection.bulk_write)
        database.data_collection.bulk_write = bulk_write_spy

        database.add(record_iter)

        assert database.data_collection.count_documents({}) == count
        assert bulk_write_spy.call_count == batches_goal

    def test_commit_and_is_default(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        database.add([Record.new({"x": 10})])

        data_version = database.get_mongo_version()
        assert data_version is not None
        assert data_version == database.committed_version

    def test_no_commit(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        database.add([Record.new({"x": 10})], commit=False)

        assert database.get_mongo_version() is not None
        assert database.committed_version is None

    def test_no_commit_when_error(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        # force bulk write to error when called
        database.data_collection.bulk_write = MagicMock(side_effect=Exception("oh no!"))

        with pytest.raises(Exception, match="oh no!"):
            database.add([Record.new({"x": 10})], commit=True)

        assert database.committed_version is None


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
            database.add(
                [
                    Record.new({"x": 6}),
                ],
                commit=True,
            )

        # add a record from 2021
        with freeze_time(date_2021):
            database.add(
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
            database.add(
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
            database.add(
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
            database.add(version_1_records, commit=True)

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
            database.add(version_2_records, commit=True)

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
            database.add(version_1_records, commit=True)

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
            database.add(version_2_records, commit=True)

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

        database.add(
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
            database.add(records)

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
            database.add(records)

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
