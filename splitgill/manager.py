from dataclasses import dataclass, asdict
from typing import Optional, Iterable

from elasticsearch import Elasticsearch
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.ingest import generate_ops, get_version
from splitgill.model import Record, Status
from splitgill.utils import now, partition

MONGO_DATABASE_NAME = "sg"
STATUS_COLLECTION_NAME = "status"
OPS_SIZE = 500


@dataclass
class SplitgillClient:
    """
    Splitgill client class which holds a mongo connection, an elasticsearch connection
    and any other general information Splitgill needs to manage the databases.
    """

    mongo: MongoClient
    elasticsearch: Elasticsearch

    def get_database(self) -> Database:
        """
        Returns the MongoDB database in use.

        :return: a pymongo Database object
        """
        return self.mongo.get_database(MONGO_DATABASE_NAME)

    def get_status_collection(self) -> Collection:
        """
        Returns the status collection.

        :return: a pymongo Collection object
        """
        return self.get_database().get_collection(STATUS_COLLECTION_NAME)

    def get_data_collection(self, name: str) -> Collection:
        """
        Returns the data collection for the given Splitgill database.

        :param name: the name of the Splitgill database
        :return: a pymongo Collection object
        """
        return self.get_database().get_collection(f"data-{name}")

    def get_config_collection(self, name: str) -> Collection:
        """
        Returns the config collection for the given Splitgill database.

        :param name: the name of the Splitgill database
        :return: a pymongo Collection object
        """
        return self.get_database().get_collection(f"config-{name}")


class SplitgillDatabase:
    """
    Represents a single set of data to be managed by Splitgill.

    Under the hood, this data will exist in several MongoDB collections and
    Elasticsearch indices, but this object provides an abstraction layer to access all
    of that from one object.
    """

    def __init__(self, name: str, client: SplitgillClient):
        """
        :param name: the name of the database, needs to be a valid MongoDB collection
                     name and a valid Elasticsearch index name
        :param client: a SplitgillClient object
        """
        self.name = name
        self._client = client
        self.data_collection = self._client.get_data_collection(self.name)
        self.status_collection = self._client.get_status_collection()

    @property
    def data_version(self) -> Optional[int]:
        status = self.get_status()
        return status.m_version if status else None

    def get_status(self) -> Optional[Status]:
        doc = self.status_collection.find_one({"name": self.name})
        return Status(**doc) if doc else None

    def set_status(self, status: Status):
        self.status_collection.update_one(
            {"name": self.name}, asdict(status), upsert=True
        )

    def clear_status(self):
        self.status_collection.delete_one({"name": self.name})

    def commit(self):
        # get the latest version in the data collection
        version = get_version(self.data_collection)
        if version is None:
            # nothing to commit
            return

        # either update the existing status, or create a new one
        status = self.get_status()
        if status is None:
            status = Status(name=self.name, m_version=version)
        else:
            if version > status.m_version:
                raise Exception("oh no")
            status.m_version = version

        # write the new status
        self.set_status(status)

    def _determine_add_version(self) -> int:
        status_version = self.data_version
        data_version = get_version(self.data_collection)

        # no data or status so generate a new version
        if status_version is None and data_version is None:
            return now()

        # no status version, but there is data so use the latest data version
        elif status_version is None and data_version is not None:
            return data_version

        # have a status version but no data version which is a weird state to be in,
        # clean it up and return a new version
        elif status_version is not None and data_version is None:
            self.clear_status()
            return now()

        # have a status version and a data version
        else:
            if data_version > status_version:
                # data version is newer, so data has been added without a commit. In
                # this case allow a continuation of adding data to this version by
                # returning the data version
                return data_version
            else:
                # otherwise data is behind status, so we should return a new version
                return now()

    def add(self, records: Iterable[Record], commit=True):
        version = self._determine_add_version()

        # this does nothing if the indexes already exist
        self.data_collection.create_indexes(
            [IndexModel([("id", ASCENDING)]), IndexModel([("version", DESCENDING)])]
        )

        for ops in partition(
            generate_ops(self.data_collection, records, version), OPS_SIZE
        ):
            self.data_collection.bulk_write(ops)

        if commit:
            self.commit()
