import json
from collections import defaultdict
from datetime import datetime, timezone
from itertools import chain
from typing import Dict, Optional
from unittest.mock import patch

from elasticsearch_dsl import Search
from freezegun import freeze_time

from splitgill.indexing.fields import DocumentField, DATA_ID_FIELD
from splitgill.indexing.index import (
    generate_index_ops,
    IndexNames,
    BulkOp,
    IndexOp,
    DeleteOp,
    ArcStatus,
)
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.parser import parse
from splitgill.manager import SplitgillClient, SplitgillDatabase, SearchVersion
from splitgill.model import Record, ParsingOptions
from splitgill.search import term_query, id_query


def test_index_names():
    indices = IndexNames("test")
    assert indices.name == "test"
    assert indices.base == "data-test"
    assert indices.latest == "data-test-latest"
    assert indices.arc_base == "data-test-arc"
    assert indices.wildcard == "data-test-*"
    assert indices.arc_wildcard == "data-test-arc-*"
    assert indices.get_arc(0) == "data-test-arc-0"
    assert indices.get_arc(4289) == "data-test-arc-4289"


def setup_scenario(
    splitgill: SplitgillClient,
    records: Dict[str, Dict[int, dict]],
    options: Dict[int, ParsingOptions],
    database_name="test",
) -> SplitgillDatabase:
    database = SplitgillDatabase(database_name, splitgill)

    versioned_data = defaultdict(list)
    for record_id, record_data in records.items():
        for version, data in record_data.items():
            versioned_data[version].append(Record(record_id, data))
    versions = set(chain(versioned_data, options))

    for version in sorted(versions):
        records_to_add = versioned_data.get(version)
        if records_to_add:
            database.ingest(records_to_add, commit=False)

        options_to_update = options.get(version)
        if options_to_update:
            database.update_options(options_to_update, commit=False)

        with freeze_time(datetime.fromtimestamp(version / 1000, tz=timezone.utc)):
            database.commit()

    return database


def check_delete_op(index_names: IndexNames, op: BulkOp, record_id: str):
    assert isinstance(op, DeleteOp)
    assert op.index == index_names.latest
    assert op.doc_id == record_id


def check_op(
    index_names: IndexNames,
    op: BulkOp,
    record_id: str,
    version: int,
    data: dict,
    options: ParsingOptions,
    next_version: Optional[int] = None,
):
    assert isinstance(op, IndexOp)
    if next_version is not None:
        assert op.document[DocumentField.NEXT] == next_version
        assert op.document[DocumentField.VERSIONS]["lt"] == next_version
        assert op.index == index_names.get_arc(0)
    else:
        assert DocumentField.NEXT not in op.document
        assert op.doc_id == record_id
        assert op.index == index_names.latest

    assert op.document[DocumentField.ID] == record_id
    assert op.document[DocumentField.VERSION] == version
    assert op.document[DocumentField.VERSIONS]["gte"] == version

    # copy the data and add the record ID for checks
    data = data.copy()
    data[DATA_ID_FIELD] = record_id
    parsed_data = parse(data, options)
    assert op.document[DocumentField.DATA] == parsed_data.parsed
    assert op.document[DocumentField.DATA_TYPES] == parsed_data.data_types
    assert op.document[DocumentField.PARSED_TYPES] == parsed_data.parsed_types


class TestGenerateIndexOps:
    def test_after_beyond_data_version(
        self, splitgill: SplitgillClient, basic_options: ParsingOptions
    ):
        # this shouldn't happen, but might as well check it!
        database = setup_scenario(
            splitgill,
            records={"r1": {10: {"x": 5}}},
            options={8: basic_options},
        )

        after = 11
        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                after,
            )
        )
        assert not ops

    def test_after_beyond_options_version(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        # this shouldn't happen, but might as well check it!
        database = setup_scenario(
            splitgill,
            records={"r1": {10: {"x": 5}}},
            options={
                8: builder.with_keyword_length(256).build(),
                12: builder.with_keyword_length(4).build(),
            },
        )

        after = 13
        assert not list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                after,
            )
        )

    def test_mix(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            4: {"x": 3.8},
            8: {"x": 1.4},
            9: {"x": 9.6},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
            10: builder.with_float_format("{0:.10f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                None,
            )
        )
        assert len(ops) == 7
        check_op(database.indices, ops[0], "r1", 10, data[9], options[10])
        check_op(
            database.indices, ops[1], "r1", 9, data[9], options[7], next_version=10
        )
        check_op(database.indices, ops[2], "r1", 8, data[8], options[7], next_version=9)
        check_op(database.indices, ops[3], "r1", 7, data[4], options[7], next_version=8)
        check_op(database.indices, ops[4], "r1", 5, data[4], options[5], next_version=7)
        check_op(database.indices, ops[5], "r1", 4, data[4], options[1], next_version=5)
        check_op(database.indices, ops[6], "r1", 2, data[2], options[1], next_version=4)

    def test_delete(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            4: {},
            8: {"x": 1.4},
            9: {},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
            10: builder.with_float_format("{0:.10f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                None,
            )
        )
        # we're expecting this set of pairs:
        # 2|1, 4|1(D), 4|5(D), 4|7(D), 8|7, 9|7(D), 9|10(D)
        # so the deletes are ignored (except for the next versions they set) and that
        # means we only get 3 ops
        assert len(ops) == 3
        check_delete_op(database.indices, ops[0], "r1")
        check_op(database.indices, ops[1], "r1", 8, data[8], options[7], next_version=9)
        check_op(database.indices, ops[2], "r1", 2, data[2], options[1], next_version=4)

    def test_after_between_versions(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            4: {"x": 2.7},
            8: {"x": 1.4},
            9: {"x": 0.1},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
            10: builder.with_float_format("{0:.10f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        # set after to 6 as we have no data nor options versions at 6
        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                6,
            )
        )
        assert len(ops) == 5
        check_op(database.indices, ops[0], "r1", 10, data[9], options[10])
        check_op(
            database.indices, ops[1], "r1", 9, data[9], options[7], next_version=10
        )
        check_op(database.indices, ops[2], "r1", 8, data[8], options[7], next_version=9)
        check_op(database.indices, ops[3], "r1", 7, data[4], options[7], next_version=8)
        check_op(database.indices, ops[4], "r1", 5, data[4], options[5], next_version=7)

    def test_after_at_both_versions(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            5: {"x": 2.7},
            8: {"x": 1.4},
            9: {"x": 0.1},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
            10: builder.with_float_format("{0:.10f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        # set after to 5 which we have a version of data and options at
        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                5,
            )
        )
        assert len(ops) == 5
        check_op(database.indices, ops[0], "r1", 10, data[9], options[10])
        check_op(
            database.indices, ops[1], "r1", 9, data[9], options[7], next_version=10
        )
        check_op(database.indices, ops[2], "r1", 8, data[8], options[7], next_version=9)
        check_op(database.indices, ops[3], "r1", 7, data[5], options[7], next_version=8)
        check_op(database.indices, ops[4], "r1", 5, data[5], options[5], next_version=7)

    def test_after_new_data(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            4: {"x": 2.7},
            8: {"x": 1.4},
            9: {"x": 0.1},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        # set after to 8, this should just mean version 9 of the data is found as new
        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                8,
            )
        )
        # should get 2 ops, one to update the latest index and one pushing the old
        # latest down to the non-latest data indices
        assert len(ops) == 2
        check_op(database.indices, ops[0], "r1", 9, data[9], options[7])
        check_op(database.indices, ops[1], "r1", 8, data[8], options[7], next_version=9)

    def test_after_new_options(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            4: {"x": 2.7},
            8: {"x": 1.4},
            9: {"x": 0.1},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
            10: builder.with_float_format("{0:.3f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        # set after to 9, this should just mean version 10 of the options is found as
        # new
        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                9,
            )
        )
        # should get 2 ops, one to update the latest index and one pushing the old
        # latest down to the non-latest data indices
        assert len(ops) == 2
        check_op(database.indices, ops[0], "r1", 10, data[9], options[10])
        check_op(
            database.indices, ops[1], "r1", 9, data[9], options[7], next_version=10
        )

    def test_after_new_both(self, splitgill: SplitgillClient):
        builder = ParsingOptionsBuilder()
        data = {
            2: {"x": 5.4},
            4: {"x": 2.7},
            8: {"x": 1.4},
            9: {"x": 0.1},
        }
        options = {
            1: builder.with_float_format("{0:.4f}").build(),
            5: builder.with_float_format("{0:.2f}").build(),
            7: builder.with_float_format("{0:.6f}").build(),
            9: builder.with_float_format("{0:.3f}").build(),
        }
        database = setup_scenario(splitgill, {"r1": data}, options)

        # set after to 8, this should just mean version 10 of the options and data is
        # found as new
        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                8,
            )
        )
        # should get 2 ops, one to update the latest index and one pushing the old
        # latest down to the non-latest data indices
        assert len(ops) == 2
        check_op(database.indices, ops[0], "r1", 9, data[9], options[9])
        check_op(database.indices, ops[1], "r1", 8, data[8], options[7], next_version=9)

    def test_just_latest(
        self, splitgill: SplitgillClient, basic_options: ParsingOptions
    ):
        data = {1: {"x": 5.4}}
        options = {1: basic_options}
        database = setup_scenario(splitgill, {"r1": data}, options)

        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                None,
            )
        )
        assert len(ops) == 1
        check_op(database.indices, ops[0], "r1", 1, data[1], options[1])

    def test_meta_geo(self, splitgill: SplitgillClient):
        builder = (
            ParsingOptionsBuilder()
            .with_keyword_length(256)
            .with_float_format("{0:.15g}")
        )
        data = {
            1: {
                "x": "beans",
                "lat": 4,
                "lon": 10,
                "location": {"type": "Point", "coordinates": [100.4, 0.1]},
            }
        }
        options = {1: builder.with_geo_hint("lat", "lon").build()}

        database = setup_scenario(splitgill, {"r1": data}, options)

        ops = list(
            generate_index_ops(
                database.indices,
                ArcStatus(0, 0),
                database.iter_records(),
                database.get_options(),
                None,
            )
        )

        assert len(ops) == 1
        check_op(database.indices, ops[0], "r1", 1, data[1], options[1])

    def test_arcs_from_nothing(self, splitgill: SplitgillClient):
        options = (
            ParsingOptionsBuilder()
            .with_keyword_length(256)
            .with_float_format("{0:.15g}")
        ).build()

        records = 1000
        versions = 5
        arc_max_size = 50

        data = {
            f"r-{i}": {v: {"x": f"value at {v}"} for v in range(1, versions + 1)}
            for i in range(records)
        }

        database = setup_scenario(splitgill, data, {1: options})

        # sync with the arc max size we want
        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", arc_max_size):
            database.sync()

        # get the indices used
        indices = database.get_current_indices()
        # this is what we expect should have been used
        expected = [
            database.indices.get_arc(i)
            # just to explain this range, it's calculating the number of records that
            # are headed for an arc index based on the size of the arcs. We do
            # (versions - 1) because the latest versions will go into the latest index,
            # not an arc index
            for i in range(int((records * (versions - 1)) / arc_max_size))
        ]
        expected.append(database.indices.latest)

        assert indices == sorted(expected)
        assert database.search().count() == records
        # the latest arc might not be full
        assert splitgill.elasticsearch.count(index=indices[0])["count"] <= arc_max_size
        # all the other arcs will be though
        for index in indices[1:-1]:
            assert splitgill.elasticsearch.count(index=index)["count"] == arc_max_size

        assert (
            database.search(version=SearchVersion.all).filter(id_query("r-123")).count()
            == versions
        )

    def test_arcs_size_change_bigger(self, splitgill: SplitgillClient):
        database = splitgill.get_database("test")
        record_id = "r-1"
        # goes to arc-0
        database.ingest([Record(record_id, {"a": 4})], commit=True)
        # goes to arc-0
        database.ingest([Record(record_id, {"a": 7})], commit=True)
        # goes to arc-1
        database.ingest([Record(record_id, {"a": 9})], commit=True)
        # goes to arc-1
        database.ingest([Record(record_id, {"a": 3})], commit=True)
        # goes to latest, then after second sync goes to arc-1
        database.ingest([Record(record_id, {"a": 8})], commit=True)

        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", 2):
            database.sync()

        # goes to arc-1
        database.ingest([Record(record_id, {"a": 2})], commit=True)
        # goes to arc-2
        database.ingest([Record(record_id, {"a": 1})], commit=True)
        # goes to arc-2
        database.ingest([Record(record_id, {"a": 0})], commit=True)
        # goes to arc-2
        database.ingest([Record(record_id, {"a": 4})], commit=True)
        # goes to latest
        database.ingest([Record(record_id, {"a": 9})], commit=True)

        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", 4):
            database.sync()

        search = Search(using=splitgill.elasticsearch)
        assert search.index(database.indices.latest).count() == 1
        assert search.index(database.indices.get_arc(0)).count() == 2
        assert search.index(database.indices.get_arc(1)).count() == 4
        assert search.index(database.indices.get_arc(2)).count() == 3

    def test_arcs_size_change_smaller(self, splitgill: SplitgillClient):
        database = splitgill.get_database("test")
        record_id = "r-1"
        # goes to arc-0
        database.ingest([Record(record_id, {"a": 4})], commit=True)
        # goes to arc-0
        database.ingest([Record(record_id, {"a": 7})], commit=True)
        # goes to arc-0
        database.ingest([Record(record_id, {"a": 9})], commit=True)
        # goes to arc-0
        database.ingest([Record(record_id, {"a": 3})], commit=True)
        # goes to latest, then after second sync goes to arc-1
        database.ingest([Record(record_id, {"a": 8})], commit=True)

        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", 5):
            database.sync()

        # goes to arc-1
        database.ingest([Record(record_id, {"a": 2})], commit=True)
        # goes to arc-2
        database.ingest([Record(record_id, {"a": 1})], commit=True)
        # goes to arc-2
        database.ingest([Record(record_id, {"a": 0})], commit=True)
        # goes to arc-3
        database.ingest([Record(record_id, {"a": 4})], commit=True)
        # goes to latest
        database.ingest([Record(record_id, {"a": 9})], commit=True)

        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", 2):
            database.sync()

        search = Search(using=splitgill.elasticsearch)
        assert search.index(database.indices.latest).count() == 1
        assert search.index(database.indices.get_arc(0)).count() == 4
        assert search.index(database.indices.get_arc(1)).count() == 2
        assert search.index(database.indices.get_arc(2)).count() == 2
        assert search.index(database.indices.get_arc(3)).count() == 1

    def test_arcs_size_1(self, splitgill: SplitgillClient):
        # an arc size of 1 would never actually be used, but it is a helpful edge case
        # to test to make sure the logic is sound
        database = splitgill.get_database("test")
        record_id = "r-1"
        database.ingest([Record(record_id, {"a": 4})], commit=True)
        database.ingest([Record(record_id, {"a": 7})], commit=True)
        database.ingest([Record(record_id, {"a": 9})], commit=True)
        database.ingest([Record(record_id, {"a": 3})], commit=True)
        database.ingest([Record(record_id, {"a": 8})], commit=True)
        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", 1):
            database.sync()
        database.ingest([Record(record_id, {"a": 2})], commit=True)
        database.ingest([Record(record_id, {"a": 1})], commit=True)
        database.ingest([Record(record_id, {"a": 0})], commit=True)
        database.ingest([Record(record_id, {"a": 4})], commit=True)
        database.ingest([Record(record_id, {"a": 9})], commit=True)
        with patch("splitgill.indexing.index.MAX_DOCS_PER_ARC", 1):
            database.sync()

        search = Search(using=splitgill.elasticsearch)
        assert search.index(database.indices.latest).count() == 1
        for arc_index in range(9):
            assert search.index(database.indices.get_arc(arc_index)).count() == 1


def test_delete_op():
    op = DeleteOp("test-index", "record-1")
    assert op.serialise() == json.dumps(
        {"delete": {"_index": "test-index", "_id": "record-1"}}, separators=(",", ":")
    )


def test_index_op():
    op = IndexOp("test-index", "record-1", {"x": "beans", "y": "beans", "z": 4.689221})
    metadata = json.dumps(
        {"index": {"_index": "test-index", "_id": "record-1"}}, separators=(",", ":")
    )
    data = json.dumps(
        {"x": "beans", "y": "beans", "z": 4.689221}, separators=(",", ":")
    )
    assert op.serialise() == f"{metadata}\n{data}"
