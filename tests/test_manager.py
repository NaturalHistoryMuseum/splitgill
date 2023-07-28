from splitgill.manager import (
    SplitgillConnection,
    MONGO_DATABASE_NAME,
    STATUS_COLLECTION_NAME,
)


class TestSplitgillConnection:
    def test_database_property(self, splitgill: SplitgillConnection):
        assert splitgill.database.name == MONGO_DATABASE_NAME

    def test_get_status_collection(self, splitgill: SplitgillConnection):
        assert splitgill.get_status_collection().name == STATUS_COLLECTION_NAME

    def test_get_data_collection(self, splitgill: SplitgillConnection):
        name = "test"
        assert splitgill.get_data_collection(name).name == f"data-{name}"

    def test_get_config_collection(self, splitgill: SplitgillConnection):
        name = "test"
        assert splitgill.get_config_collection(name).name == f"config-{name}"
