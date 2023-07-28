from freezegun import freeze_time

from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    STATUS_COLLECTION_NAME,
    SplitgillDatabase,
)
from splitgill.model import Record


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
