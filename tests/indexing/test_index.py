from datetime import datetime

from bson import ObjectId
from cytoolz.itertoolz import sliding_window

from splitgill.diffing import diff
from splitgill.indexing.fields import RootField, MetaField
from splitgill.indexing.index import (
    get_data_index_id,
    get_config_index_id,
    get_latest_index_id,
    create_index_op,
    generate_index_ops,
)
from splitgill.indexing.parser import parse_for_index
from splitgill.manager import SplitgillClient
from splitgill.model import MongoRecord, VersionedData
from splitgill.utils import to_timestamp


def test_get_data_index_id():
    assert get_data_index_id("test", 1691340859000) == "data-2023-test"
    assert get_data_index_id("test", 1659804859000) == "data-2022-test"
    assert get_data_index_id("test", 1617692059000) == "data-2021-test"
    assert get_data_index_id("test", 1577840461000) == "data-2020-test"


def test_get_config_index_id():
    assert get_config_index_id("test") == "config-test"


def test_get_latest_index_id():
    assert get_latest_index_id("test") == "data-latest-test"


class TestCreateIndexOp:
    def test_latest_op(self):
        index_name = get_latest_index_id("test")
        data = {"x": "beans"}
        record_id = "xyz"
        version = 1691359001000
        parsed_data = parse_for_index(data)

        op = create_index_op(index_name, record_id, data, version, next_version=None)

        assert op["_op_type"] == "index"
        assert op["_index"] == index_name
        assert op["_id"] == record_id
        assert op[RootField.ID.value] == record_id
        assert op[RootField.DATA.value] == data
        assert op[RootField.PARSED.value] == parsed_data.parsed
        assert op[RootField.GEO.value] == parsed_data.geo
        assert op[RootField.ARRAYS.value] == parsed_data.arrays
        assert op[RootField.META.value] == {
            MetaField.VERSION.value: version,
            MetaField.VERSIONS.value: {
                "gte": version,
                # no lt
            },
        }

    def test_old_op(self):
        data = {"x": "beans"}
        record_id = "xyz"
        version = 1691359001000
        next_version = 1692568619000
        index_name = get_data_index_id("test", version)
        parsed_data = parse_for_index(data)

        op = create_index_op(
            index_name, record_id, data, version, next_version=next_version
        )

        assert op["_op_type"] == "index"
        assert op["_index"] == index_name
        assert op["_id"] == f"{record_id}:{version}"
        assert op[RootField.ID.value] == record_id
        assert op[RootField.DATA.value] == data
        assert op[RootField.PARSED.value] == parsed_data.parsed
        assert op[RootField.GEO.value] == parsed_data.geo
        assert op[RootField.ARRAYS.value] == parsed_data.arrays
        assert op[RootField.META.value] == {
            MetaField.VERSION.value: version,
            MetaField.VERSIONS.value: {"gte": version, "lt": next_version},
            MetaField.NEXT_VERSION.value: next_version,
        }


class TestGenerateIndexOps:
    def test_updates(self, splitgill: SplitgillClient):
        # make a bunch of data at different versions
        data = [
            VersionedData(to_timestamp(datetime(year, 1, 1)), {"x": f"{year} beans"})
            for year in range(2015, 2024)
        ]

        record = MongoRecord(
            _id=ObjectId(),
            id="record-1",
            version=data[-1].version,
            data=data[-1].data,
            diffs={
                str(old.version): list(diff(new.data, old.data))
                for old, new in sliding_window(2, data)
            },
        )

        # test all data
        ops = list(generate_index_ops("test", [record], 0))
        assert len(ops) == len(data)
        # check the first op, this will always be the op to update the latest index
        assert ops[0] == create_index_op(
            get_latest_index_id("test"), record.id, data[-1].data, data[-1].version
        )
        # check the other ops which update the old indices
        for i, op in enumerate(reversed(ops[1:])):
            assert op == create_index_op(
                get_data_index_id("test", data[i].version),
                record.id,
                data[i].data,
                data[i].version,
                data[i + 1].version,
            )

        # now test not all the data
        ops = list(generate_index_ops("test", [record], data[4].version + 1))
        assert len(ops) == len(data) - 4
        # first op out should still be the latest index replace op
        assert ops[0] == create_index_op(
            get_latest_index_id("test"), record.id, data[-1].data, data[-1].version
        )
        # the last op out should be one adding the previous latest version to the old
        # indices. To explain a bit more, because we've passed data[4].version as the
        # current version in elasticsearch, this implies that data[4] is the current
        # latest version. This means it is not present in the old indices and therefore
        # when it is replaced as the latest version in the latest index, it's data needs
        # to be shunted to the old indices. This last op does that.
        assert ops[-1] == create_index_op(
            get_data_index_id("test", data[4].version),
            record.id,
            data[4].data,
            data[4].version,
            data[5].version,
        )

    def test_some_deletes(self, splitgill: SplitgillClient):
        data = [
            VersionedData(to_timestamp(datetime(2015, 1, 1)), {"x": f"{2015} beans"}),
            VersionedData(to_timestamp(datetime(2016, 1, 1)), {"x": f"{2016} beans"}),
            # delete
            VersionedData(to_timestamp(datetime(2017, 1, 1)), {}),
            VersionedData(to_timestamp(datetime(2018, 1, 1)), {"x": f"{2018} beans"}),
            # delete
            VersionedData(to_timestamp(datetime(2019, 1, 1)), {}),
        ]

        record = MongoRecord(
            _id=ObjectId(),
            id="record-1",
            version=data[-1].version,
            data=data[-1].data,
            diffs={
                str(old.version): list(diff(new.data, old.data))
                for old, new in sliding_window(2, data)
            },
        )

        ops = list(generate_index_ops("test", [record], 0))

        assert len(ops) == 4
        # first op should be the delete from the latest index
        assert ops[0] == {
            "_op_type": "delete",
            "_index": get_latest_index_id("test"),
            "_id": record.id,
        }
        # second op should be to index the previous version into the a data index
        assert ops[1] == create_index_op(
            get_data_index_id("test", data[-2].version),
            record.id,
            data[-2].data,
            data[-2].version,
            data[-1].version,
        )
        # third op should the 2016 record as the 2017 is a delete
        assert ops[2] == create_index_op(
            get_data_index_id("test", data[1].version),
            record.id,
            data[1].data,
            data[1].version,
            data[2].version,
        )
        # fourth op should the 2015 record
        assert ops[3] == create_index_op(
            get_data_index_id("test", data[0].version),
            record.id,
            data[0].data,
            data[0].version,
            data[1].version,
        )
