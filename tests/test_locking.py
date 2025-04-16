import platform
from datetime import datetime

import pytest
from freezegun import freeze_time
from pymongo.collection import Collection

from splitgill.locking import LockManager, AlreadyLocked


class TestLockManager:
    def test_is_locked(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        assert lock_manager.acquire("test")
        assert lock_manager.is_locked("test")
        assert not lock_manager.is_locked("a different lock")
        lock_manager.release("test")
        assert not lock_manager.is_locked("test")

    def test_acquired_ok(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        assert lock_manager.acquire("test")
        lock_manager.release("test")
        assert not lock_manager.is_locked("test")

    @freeze_time("2024-03-20 21:27:23")
    def test_get_metadata(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        data = {"under_test": True, "number": 4, "banana": [1, 2, 3]}
        lock_manager.acquire("test", **data)
        metadata = lock_manager.get_metadata("test")
        # check there is an _id and also remove it so that we can check the metadata
        # without it
        assert metadata.pop("_id", False)
        assert metadata == {
            "lock_id": "test",
            "locked_by": platform.node(),
            "locked_at": datetime(2024, 3, 20, 21, 27, 23),
            "data": data,
        }
        lock_manager.release("test")
        assert not lock_manager.is_locked("test")

    def test_acquired_double_lock(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        assert lock_manager.acquire("test")
        assert not lock_manager.acquire("test")
        lock_manager.release("test")
        assert not lock_manager.is_locked("test")

    def test_acquired_double_lock_raise(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        assert lock_manager.acquire("test")
        with pytest.raises(AlreadyLocked):
            assert lock_manager.acquire("test", raise_on_fail=True)
        lock_manager.release("test")
        assert not lock_manager.is_locked("test")

    def test_lock(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        with lock_manager.lock("test", wow="pop!"):
            assert lock_manager.is_locked("test")
            assert lock_manager.get_metadata("test")["data"]["wow"] == "pop!"
        assert not lock_manager.is_locked("test")

    def test_lock_already_locked(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        assert lock_manager.acquire("test")
        with pytest.raises(AlreadyLocked):
            with lock_manager.lock("test"):
                pass
        lock_manager.release("test")
        assert not lock_manager.is_locked("test")

    def test_lock_interrupted(self, mongo_collection: Collection):
        lock_manager = LockManager(mongo_collection)
        with pytest.raises(Exception, match="oh no!"):
            with lock_manager.lock("test"):
                raise Exception("oh no!")
        assert not lock_manager.is_locked("test")
