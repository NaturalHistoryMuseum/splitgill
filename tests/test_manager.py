import pytest
from bson import ObjectId
from freezegun import freeze_time

from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    STATUS_COLLECTION_NAME,
    SplitgillDatabase,
)
from splitgill.model import Record, Status


class TestSplitgillClient:
    def test_database(self, splitgill: SplitgillClient):
        assert splitgill.get_database().name == MONGO_DATABASE_NAME

    def test_get_status_collection(self, splitgill: SplitgillClient):
        assert splitgill.get_status_collection().name == STATUS_COLLECTION_NAME

    def test_get_data_collection(self, splitgill: SplitgillClient):
        name = "test"
        assert splitgill.get_data_collection(name).name == f"data-{name}"

    def test_get_config_collection(self, splitgill: SplitgillClient):
        name = "test"
        assert splitgill.get_config_collection(name).name == f"config-{name}"


class TestSplitgillDatabaseDataVersion:
    def test_no_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.data_version is None

    def test_data_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]
        database.add(records, commit=False)
        assert database.data_version is None

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
        assert database.data_version == version

    def test_data_with_status_different_to_data(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"x": 4}),
            Record.new({"x": 89}),
            Record.new({"x": 5}),
        ]

        with freeze_time("2012-01-14 12:00:01"):
            database.add(records, commit=True)
        assert database.data_version == 1326542401000

        another_record = Record.new({"x": 199})
        with freeze_time("2016-02-15 12:00:01"):
            database.add([another_record], commit=False)
        # data version shouldn't have changed, even though the version in the data
        # collection will be newer
        assert database.data_version == 1326542401000


class TestSplitgillDatabaseStatusCrud:
    def test_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        assert database.get_status() is None

    def test_set_status_first_time(self, splitgill: SplitgillClient):
        name = "test"
        database = SplitgillDatabase(name, splitgill)
        status = Status(name, 1000)

        database.set_status(status)

        status_in_db = database.get_status()
        assert status_in_db.name == status.name
        assert status_in_db.m_version == status.m_version
        assert status_in_db.e_version == status.e_version
        assert status_in_db._id is not None

    def test_set_status_twice(self, splitgill: SplitgillClient):
        name = "test"
        database = SplitgillDatabase(name, splitgill)

        status = Status(name, 1000)
        database.set_status(status)
        status_in_db = database.get_status()
        assert status_in_db.name == status.name
        assert status_in_db.m_version == status.m_version
        assert status_in_db.e_version == status.e_version
        assert status_in_db._id is not None

        status = Status(name, 1001)
        database.set_status(status)
        status_in_db = database.get_status()
        assert status_in_db.name == status.name
        assert status_in_db.m_version == status.m_version
        assert status_in_db.e_version == status.e_version
        assert status_in_db._id is not None

    def test_set_status_incorrect_name(self, splitgill: SplitgillClient):
        name = "test"
        other_name = "not_test"
        database = SplitgillDatabase(name, splitgill)

        status = Status(other_name, 100)
        with pytest.raises(AssertionError):
            database.set_status(status)

    def test_delete_status_no_status(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        database.clear_status()
        assert database.get_status() is None

    def test_delete_status(self, splitgill: SplitgillClient):
        name = "test"
        database = SplitgillDatabase(name, splitgill)
        database.set_status(Status(name, 100))

        assert database.get_status() is not None
        database.clear_status()
        assert database.get_status() is None
