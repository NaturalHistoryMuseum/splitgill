from splitgill.manager import (
    SplitgillClient,
    MONGO_DATABASE_NAME,
    STATUS_COLLECTION_NAME,
)


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
