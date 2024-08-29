import time
from collections import Counter
from datetime import datetime, timezone
from typing import List
from unittest.mock import patch, MagicMock

import pytest
from elasticsearch_dsl import Search
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
from splitgill.model import Record, ParsingOptions
from splitgill.search import create_version_query, term_query
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

    def test_uncommitted_options(
        self, splitgill: SplitgillClient, basic_options: ParsingOptions
    ):
        database = SplitgillDatabase("test", splitgill)
        database.update_options(basic_options, commit=False)
        assert database.get_committed_version() is None

    @freeze_time("2012-01-14 12:00:01")
    def test_committed_options(
        self, splitgill: SplitgillClient, basic_options: ParsingOptions
    ):
        database = SplitgillDatabase("test", splitgill)
        database.update_options(basic_options, commit=True)
        assert database.get_committed_version() == 1326542401000

    @freeze_time("2012-01-14 12:00:01")
    def test_mixed_options(
        self, splitgill: SplitgillClient, basic_options_builder: ParsingOptionsBuilder
    ):
        database = SplitgillDatabase("test", splitgill)
        version = 1326542401000
        options = basic_options_builder.build()
        database.update_options(options, commit=True)
        assert database.get_committed_version() == version
        new_options = basic_options_builder.with_true_value("aye").build()
        database.update_options(new_options, commit=False)
        assert database.get_committed_version() == version

    def test_mixed_both(
        self, splitgill: SplitgillClient, basic_options_builder: ParsingOptionsBuilder
    ):
        database = SplitgillDatabase("test", splitgill)

        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.ingest(records, commit=False)
        database.update_options(basic_options_builder.build(), commit=False)

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
            new_options = basic_options_builder.with_true_value("aye").build()
            database.update_options(new_options, commit=True)
        assert database.get_committed_version() == 1326542409000


class TestGetElasticsearchVersion:
    def test_no_docs(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_elasticsearch_version() is None

    def test_with_docs(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)

        versions = []
        for _ in range(5):
            versions.append(
                database.ingest([Record.new({"x": 4})], commit=True).version
            )
            # just to ensure the versions are different have a nap. They will be, cause
            # Python slow, but this guarantees it
            time.sleep(0.1)

        database.sync()

        assert database.get_elasticsearch_version() == versions[-1]

    def test_with_deletes(self, splitgill: SplitgillClient):
        # this test is the same as the above scenario but with the last modification to
        # the database being a delete. This is therefore checking that the method under
        # test returns the latest version from es correctly when it is either a new bit
        # of data or a delete (ensuring that we are getting the latest version from both
        # the version and the next fields)

        database = SplitgillDatabase("test", splitgill)

        versions = []
        for _ in range(5):
            versions.append(
                database.ingest([Record.new({"x": 4})], commit=True).version
            )
            # just to ensure the versions are different have a nap. They will be, cause
            # Python slow, but this guarantees it
            time.sleep(0.1)

        # delete a record
        a_record = next(iter(database.iter_records()))
        versions.append(database.ingest([Record(a_record.id, {})], commit=True).version)

        database.sync()

        assert database.get_elasticsearch_version() == versions[-1]


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
    def test_new_options(
        self, splitgill: SplitgillClient, basic_options: ParsingOptions
    ):
        database = SplitgillDatabase("test", splitgill)
        database.update_options(basic_options, commit=False)
        assert database.commit() == 1326542401000

    @freeze_time("2012-01-14 12:00:01")
    def test_both(self, splitgill: SplitgillClient, basic_options: ParsingOptions):
        database = SplitgillDatabase("test", splitgill)
        database.ingest([Record.new({"x": 5})], commit=False)
        database.update_options(basic_options, commit=False)
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
        "query": {"bool": {"filter": [create_version_query(5).to_dict()]}}
    }


def pf(
    path: str,
    count: int,
    n: int = 0,
    d: int = 0,
    b: int = 0,
    t: int = 0,
    g: int = 0,
) -> ParsedField:
    counts = {
        ParsedType.BOOLEAN: b,
        ParsedType.DATE: d,
        ParsedType.GEO_SHAPE: g,
        ParsedType.GEO_POINT: g,
        ParsedType.KEYWORD_CASE_INSENSITIVE: t,
        ParsedType.KEYWORD_CASE_SENSITIVE: t,
        ParsedType.NUMBER: n,
        ParsedType.TEXT: t,
    }
    counts = {parsed_type: count for parsed_type, count in counts.items() if count > 0}
    return ParsedField(path, count=count, type_counts=Counter(counts))


def df(
    path: str,
    count: int,
    s: int = 0,
    i: int = 0,
    f: int = 0,
    b: int = 0,
    l: int = 0,
    d: int = 0,
    n: int = 0,
) -> DataField:
    counts = {
        DataType.NONE: n,
        DataType.STR: s,
        DataType.INT: i,
        DataType.FLOAT: f,
        DataType.BOOL: b,
        DataType.LIST: l,
        DataType.DICT: d,
    }
    counts = {data_type: count for data_type, count in counts.items() if count > 0}
    return DataField(path, count=count, type_counts=Counter(counts))


def check_data_fields(actual: List[DataField], expected: List[DataField]):
    for actual_df, expected_df in zip(actual, expected):
        assert actual_df.path == expected_df.path
        assert actual_df.count == expected_df.count
        assert actual_df.type_counts == expected_df.type_counts


class TestGetFieldsMethods:
    def test_int(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 3
        assert data_fields == [df("b", 3, i=3), df("a", 2, i=2), df("c", 1, i=1)]

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 3
        assert parsed_fields == [
            pf("b", 3, t=3, n=3),
            pf("a", 2, t=2, n=2),
            pf("c", 1, t=1, n=1),
        ]

    def test_float(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 3
        assert data_fields == [df("b", 3, f=3), df("a", 2, f=2), df("c", 1, f=1)]

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 3
        assert parsed_fields == [
            pf("b", 3, t=3, n=3),
            pf("a", 2, t=2, n=2),
            pf("c", 1, t=1, n=1),
        ]

    def test_date(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 2
        assert data_fields == [df("a", 2, s=2), df("b", 1, s=1)]

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 2
        assert parsed_fields == [pf("a", 2, d=2, t=2), pf("b", 1, d=1, t=1)]

    def test_bool(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 3
        assert data_fields == [df("b", 3, b=3), df("a", 2, b=2), df("c", 1, b=1)]

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 3
        assert parsed_fields == [
            pf("b", 3, t=3, b=3),
            pf("a", 2, t=2, b=2),
            pf("c", 1, t=1, b=1),
        ]

    def test_str(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 3
        assert data_fields == [df("b", 3, s=3), df("a", 2, s=2), df("c", 1, s=1)]

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 3
        assert parsed_fields == [
            pf("b", 3, t=3),
            pf("a", 2, t=2),
            pf("c", 1, t=1),
        ]

    def test_dict(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 6
        check_data_fields(
            data_fields,
            [
                df("topB", 3, d=3),
                df("topB.a", 3, i=3),
                df("topA", 2, d=2),
                df("topA.a", 2, i=2),
                df("topC", 1, d=1),
                df("topC.a", 1, i=1),
            ],
        )

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 3
        assert parsed_fields == [
            pf("topB.a", 3, t=3, n=3),
            pf("topA.a", 2, t=2, n=2),
            pf("topC.a", 1, t=1, n=1),
        ]

    def test_list(self, database: SplitgillDatabase):
        records = [
            Record.new({"a": [1, 2, 3]}),
            Record.new({"a": [1, "beans", 3]}),
            Record.new({"a": [1, False, True]}),
            Record.new({"a": [5.4]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        data_fields = database.get_data_fields()
        assert len(data_fields) == 2
        check_data_fields(
            data_fields,
            [
                df("a", 4, l=4),
                df("a.", 4, i=3, f=1, s=1, b=1),
            ],
        )

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 1
        assert parsed_fields == [pf("a", 4, t=4, n=4, b=1)]

    def test_mix(self, database: SplitgillDatabase):
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

        data_fields = database.get_data_fields()
        assert len(data_fields) == 5
        check_data_fields(
            data_fields,
            [
                df("b", 4, s=1, l=1, d=2),
                df("a", 2, i=1, f=1),
                df("b.x", 2, f=1, s=1),
                df("b.y", 2, b=2),
                df("b.", 1, i=1),
            ],
        )

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 4
        assert parsed_fields == [
            pf("a", 2, t=2, n=2),
            pf("b", 2, t=2, n=1),
            pf("b.x", 2, t=2, n=1),
            pf("b.y", 2, t=2, b=2),
        ]

    def test_list_of_dicts(self, database: SplitgillDatabase):
        records = [
            Record.new({"a": [{"a": 5}, {"a": 5.4}, {"b": True}]}),
            Record.new({"a": [{"a": "beans"}, {"a": 5.4}, {"b": 3.9}]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        data_fields = database.get_data_fields()
        assert len(data_fields) == 4
        check_data_fields(
            data_fields,
            [
                df("a", 2, l=2),
                df("a.", 2, d=2),
                df("a..a", 2, i=1, f=2, s=1),
                df("a..b", 2, b=1, f=1),
            ],
        )

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 2
        assert parsed_fields == [
            pf("a.a", 2, t=2, n=2),
            pf("a.b", 2, t=2, b=1, n=1),
        ]

    def test_list_of_lists(self, database: SplitgillDatabase):
        records = [
            Record.new({"a": [[1, 2, 3], [4, 5, 6], 9]}),
            Record.new({"a": [[9, 8, 7], [6, 5, 4], "organs"]}),
            Record.new({"a": [[1, 2, 3], [4, 5, 6], [True, False]]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        data_fields = database.get_data_fields()
        assert len(data_fields) == 3
        check_data_fields(
            data_fields,
            [
                df("a", 3, l=3),
                df("a.", 3, l=3, i=1, s=1),
                df("a..", 3, b=1, i=3),
            ],
        )

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 1
        assert parsed_fields == [pf("a", 3, t=3, n=3, b=1)]

    def test_deep_nesting(self, database: SplitgillDatabase):
        # ew
        records = [Record.new({"a": {"b": [[{"c": [{"d": 5}]}]]}})]
        database.ingest(records, commit=True)
        database.sync()

        data_fields = database.get_data_fields()
        assert len(data_fields) == 7
        check_data_fields(
            data_fields,
            [
                df("a", 1, d=1),
                df("a.b", 1, l=1),
                df("a.b.", 1, l=1),
                df("a.b..", 1, d=1),
                df("a.b...c", 1, l=1),
                df("a.b...c.", 1, d=1),
                df("a.b...c..d", 1, i=1),
            ],
        )
        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 1
        assert parsed_fields == [pf("a.b.c.d", 1, t=1, n=1)]

    def test_version(self, database: SplitgillDatabase):
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
        data_fields = database.get_data_fields()
        assert len(data_fields) == 1
        assert data_fields == [df("x", 2, b=2)]
        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 1
        assert parsed_fields == [pf("x", 2, t=2, b=2)]

        # then check the old version where the values are ints
        data_fields = database.get_data_fields(version=to_timestamp(version_1_time))
        assert len(data_fields) == 1
        assert data_fields == [df("x", 2, i=2)]
        parsed_fields = database.get_parsed_fields(version=to_timestamp(version_1_time))
        assert len(parsed_fields) == 1
        assert parsed_fields == [pf("x", 2, t=2, n=2)]

    def test_with_filter(self, database: SplitgillDatabase):
        records = [
            Record.new({"a": 1, "b": True}),
            Record.new({"a": 2, "b": 5.3}),
            Record.new({"a": 3, "b": "beans!"}),
            Record.new({"a": 4, "b": "armpit"}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        # check the baseline
        data_fields = database.get_data_fields()
        assert len(data_fields) == 2
        assert data_fields == [df("a", 4, i=4), df("b", 4, f=1, b=1, s=2)]
        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 2
        assert parsed_fields == [pf("a", 4, t=4, n=4), pf("b", 4, t=4, b=1, n=1)]

        # now check with some filters
        query = term_query("a", 1)
        data_fields = database.get_data_fields(query=query)
        parsed_fields = database.get_parsed_fields(query=query)
        assert data_fields == [df("a", 1, i=1), df("b", 1, b=1)]
        assert parsed_fields == [pf("a", 1, t=1, n=1), pf("b", 1, t=1, b=1)]

        query = term_query("a", 2)
        data_fields = database.get_data_fields(query=query)
        parsed_fields = database.get_parsed_fields(query=query)
        assert data_fields == [df("a", 1, i=1), df("b", 1, f=1)]
        assert parsed_fields == [pf("a", 1, t=1, n=1), pf("b", 1, t=1, n=1)]

        query = term_query("a", 3)
        data_fields = database.get_data_fields(query=query)
        parsed_fields = database.get_parsed_fields(query=query)
        assert data_fields == [df("a", 1, i=1), df("b", 1, s=1)]
        assert parsed_fields == [pf("a", 1, t=1, n=1), pf("b", 1, t=1)]

        query = term_query("a", 4)
        data_fields = database.get_data_fields(query=query)
        parsed_fields = database.get_parsed_fields(query=query)
        assert data_fields == [df("a", 1, i=1), df("b", 1, s=1)]
        assert parsed_fields == [pf("a", 1, t=1, n=1), pf("b", 1, t=1)]

    def test_geo_in_parsed_fields(
        self, database: SplitgillDatabase, geojson_point: dict, wkt_point: str
    ):
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
        database.ingest(records, commit=False)
        database.update_options(
            ParsingOptionsBuilder().with_geo_hint("lat", "lon", "rad").build(),
            commit=False,
        )
        database.commit()
        database.sync()

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 7
        assert parsed_fields == [
            pf("geojson", 1, g=1),
            pf("geojson.coordinates", 1, n=1, t=1),
            pf("geojson.type", 1, t=1),
            pf("lat", 1, g=1, t=1, n=1),
            pf("lon", 1, t=1, n=1),
            pf("rad", 1, t=1, n=1),
            pf("wkt", 1, g=1, t=1),
        ]

    def test_counts(self, database: SplitgillDatabase):
        records = [
            Record.new({"a": [1, True]}),
            Record.new({"a": [1, 4.5]}),
            Record.new({"a": "beans"}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        data_fields = database.get_data_fields()
        assert len(data_fields) == 2
        check_data_fields(
            data_fields,
            [
                # 3 fields have an "a" field
                df("a", 3, l=2, s=1),
                # 2 fields have lists under "a"
                df("a.", 2, i=2, b=1, f=1),
            ],
        )

        parsed_fields = database.get_parsed_fields()
        assert len(parsed_fields) == 1
        assert parsed_fields == [pf("a", 3, n=2, b=1, t=3)]

    def test_hierarchy(self, database: SplitgillDatabase):
        records = [
            Record.new({"a": "beans"}),
            Record.new({"b": {"c": 4, "d": True}}),
            Record.new({"e": ["beans"]}),
            Record.new({"f": [{"g": 3, "h": {"i": "beans"}}]}),
        ]
        database.ingest(records, commit=True)
        database.sync()

        # these are the data fields we expect
        a = df("a", 1, s=1)
        b = df("b", 1, d=1)
        b_c = df("b.c", 1, i=1)
        b_d = df("b.d", 1, b=1)
        e = df("e", 1, l=1)
        e_ = df("e.", 1, s=1)
        f = df("f", 1, l=1)
        f_ = df("f.", 1, d=1)
        f__g = df("f..g", 1, i=1)
        f__h = df("f..h", 1, d=1)
        f__h_i = df("f..h.i", 1, s=1)

        # these are the relationships we expect
        b.children.append(b_c)
        b.children.append(b_d)
        b_c.parent = b
        b_d.parent = b
        e.children.append(e_)
        e_.parent = e
        f.children.append(f_)
        f_.parent = f
        f_.children.append(f__g)
        f_.children.append(f__h)
        f__g.parent = f
        f__h.parent = f
        f__h.children.append(f__h_i)
        f__h_i.parent = f__h

        data_fields = database.get_data_fields()
        check_data_fields(
            data_fields, [a, b, b_c, b_d, e, e_, f, f_, f__g, f__h, f__h_i]
        )
        assert all(
            field.is_root_field
            for field in [
                data_fields[0],
                data_fields[1],
                data_fields[4],
                data_fields[6],
            ]
        )
        assert a.children == []

        check_data_fields(data_fields[1].children, [b_c, b_d])
        assert all(field.parent.path == b.path for field in data_fields[1].children)

        check_data_fields(data_fields[4].children, [e_])
        assert all(field.parent.path == e.path for field in data_fields[4].children)

        check_data_fields(data_fields[6].children, [f_, f__g, f__h])
        assert all(field.parent.path == f.path for field in data_fields[6].children)

        check_data_fields(data_fields[9].children, [f__h_i])
        assert all(field.parent.path == f__h.path for field in data_fields[9].children)


def test_get_rounded_version(splitgill: SplitgillClient):
    database = splitgill.get_database("test")

    # test with no versions
    assert database.get_rounded_version(8) is None

    # create some versions
    for version in [4, 5, 9]:
        with freeze_time(datetime.fromtimestamp(version / 1000, timezone.utc)):
            database.ingest([Record.new({"a": 4})])
    database.sync()

    # check before the first version
    assert database.get_rounded_version(2) is None
    assert database.get_rounded_version(3) is None
    # then check some other versions
    assert database.get_rounded_version(4) == 4
    assert database.get_rounded_version(5) == 5
    assert database.get_rounded_version(6) == 5
    assert database.get_rounded_version(7) == 5
    assert database.get_rounded_version(8) == 5
    assert database.get_rounded_version(9) == 9
    # check after the latest version
    assert database.get_rounded_version(10) == 9
    assert database.get_rounded_version(18932123) == 9

    # delete all data and check rounded version is correctly found
    with freeze_time(datetime.fromtimestamp(15 / 1000, timezone.utc)):
        database.ingest(
            [Record.delete(record.id) for record in database.iter_records()]
        )
    database.sync()
    assert database.get_rounded_version(15) == 15
    assert database.get_rounded_version(20) == 15


def test_get_versions(splitgill: SplitgillClient):
    database = splitgill.get_database("test")

    assert database.get_versions() == []

    record_id = "test-1"
    versions = [
        (4, {"a": 1}),
        (5, {"a": 7}),
        # delete
        (7, {}),
        # it's back! :O
        (9, {"a": 4}),
        # lastly, delete the record to check next is also being considered
        (15, {}),
    ]
    for version, data in versions:
        with freeze_time(datetime.fromtimestamp(version / 1000, timezone.utc)):
            database.ingest([Record(record_id, data)])

    # no sync has occurred yet so no versions available
    assert database.get_versions() == []

    database.sync()

    # now there are versions, including the deleted version
    assert database.get_versions() == [4, 5, 7, 9, 15]
