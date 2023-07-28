from unittest.mock import patch, MagicMock

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
        assert status.m_version == 1326542401000

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
        assert database.get_status().m_version == 1326542401000

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
        assert second_status.m_version > first_status.m_version


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
