import platform
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError


class AlreadyLocked(Exception):
    """
    Exception that will be thrown when a lock is already acquired.
    """

    def __init__(self, lock_id: str):
        super().__init__(f"Lock '{lock_id}' is already locked")
        self.lock_id = lock_id


class LockManager:
    """
    Class for managing locks.
    """

    def __init__(self, lock_collection: Collection):
        """
        :param lock_collection: the collection to use for lock data
        """
        self.lock_collection = lock_collection
        # does nothing if this already exists
        self.lock_collection.create_index("lock_id", unique=True)

    def acquire(self, lock_id: str, raise_on_fail: bool = False, **kwargs) -> bool:
        """
        Acquire the lock with the given lock_id. If the lock can't be acquired, False is
        returned, if it can True is returned. If raise_on_fail is set to True, a
        AlreadyLocked exception is raised if the lock can't be acquired.

        Any additional keyword arguments provided are stored in the lock collection with
        the core lock metadata.

        :param lock_id: the ID of the lock to acquire
        :param raise_on_fail: if True, raises an AlreadyLocked exception if the lock
                              can't be acquired. Default: False.
        :return: True if the lock was acquired, False if not
        """
        try:
            doc = {
                "lock_id": lock_id,
                "locked_at": datetime.now(timezone.utc),
                "locked_by": platform.node(),
            }
            if kwargs:
                doc["data"] = kwargs
            self.lock_collection.insert_one(doc)
        except DuplicateKeyError:
            if raise_on_fail:
                raise AlreadyLocked(lock_id)
            return False
        return True

    def release(self, lock_id: str):
        """
        Release the lock with the given lock_id. If the lock_id isn't locked, does
        nothing.

        :param lock_id: the ID of the lock to release
        """
        self.lock_collection.delete_one({"lock_id": lock_id})

    def is_locked(self, lock_id: str) -> bool:
        """
        Check if the given lock_id is locked or not.

        :param lock_id: the ID of the lock to check
        :return: True if the lock is currently acquired, False if not
        """
        return self.get_metadata(lock_id) is not None

    def get_metadata(self, lock_id: str) -> Optional[dict]:
        """
        Returns the doc stored in the lock collection for the given lock ID, if there is
        one.

        :param lock_id:
        :return:
        """
        return self.lock_collection.find_one({"lock_id": lock_id})

    @contextmanager
    def lock(self, lock_id: str, **kwargs):
        """
        Context manager to safely acquire and release a lock with the given lock ID. If
        the lock is already acquired, raises an AlreadyLocked exception.

        Any additional keyword arguments provided are stored in the lock collection with
        the core lock metadata.

        :param lock_id: ID of the lock to acquire and release
        """
        self.acquire(lock_id, raise_on_fail=True, **kwargs)
        try:
            yield
        finally:
            self.release(lock_id)
