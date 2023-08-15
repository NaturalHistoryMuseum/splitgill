from collections import deque
from dataclasses import dataclass
from typing import Optional, Iterable

from cytoolz.dicttoolz import get_in
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.indexing.fields import MetaField
from splitgill.indexing.index import generate_index_ops, get_latest_index_id
from splitgill.indexing.templates import DATA_TEMPLATE
from splitgill.ingest import generate_ops
from splitgill.model import Record, Status, MongoRecord
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
        self.latest_index_name = get_latest_index_id(self.name)

    @property
    def committed_version(self) -> Optional[int]:
        """
        Returns the currently committed data version, if there is one available. This
        version is the number stored in the main database status object, not
        (necessarily) the latest version in the actual data collection for this
        database.

        :return: a version timestamp or None
        """
        status = self.get_status()
        return status.version if status else None

    def get_mongo_version(self) -> Optional[int]:
        """
        Returns the latest version found in the data collection. If no records exist in
        the collection, None is returned.

        :return: the max version or None
        """
        last = next(
            self.data_collection.find().sort("version", DESCENDING).limit(1), None
        )
        if last is None:
            return None
        return last["version"]

    def get_elasticsearch_version(self) -> Optional[int]:
        """
        Returns the latest version found in the Elasticsearch indices for this database.
        If no records exist in any index, None is returned.

        :return: the max version or None
        """
        result = self._client.elasticsearch.search(
            aggs={"max_version": {"max": {"field": MetaField.VERSION.path()}}},
            size=0,
            # search all data indices for this database
            index=f"data-*-{self.name}",
        )

        version = get_in(("aggregations", "max_version", "value"), result, None)
        if version is None:
            return None

        # elasticsearch does max aggs using the double type apparently, so we need to
        # convert it back to an int to avoid returning a float and causing confusion
        return int(version)

    def get_status(self) -> Optional[Status]:
        """
        Return the current status for this database.

        :return: a Status object or None if no status is set currently
        """
        doc = self.status_collection.find_one({"name": self.name})
        return Status(**doc) if doc else None

    def clear_status(self):
        """
        Clears the status for this database by deleting the doc.
        """
        self.status_collection.delete_one({"name": self.name})

    def commit(self) -> bool:
        """
        Updates the status of this database with the latest version in the data
        collection. After this, no more records at that latest version can be added to
        the database, they must all be newer.

        :return: True if the status was updated, False if not
        """
        # get the latest version in the data collection
        version = self.get_mongo_version()
        if version is None:
            # nothing to commit
            return False

        status = self.get_status()
        if status is None:
            # make a new status object
            status = Status(name=self.name, version=version)
        else:
            # update the existing status object
            status.version = version

        # replace (or insert) the current status
        self.status_collection.replace_one(
            {"name": self.name}, status.to_doc(), upsert=True
        )
        return True

    def determine_next_version(self) -> int:
        """
        Figure out what version should be used with the next record(s) added to this
        database. If a transaction is in progress and changes haven't been committed
        then the latest version from the data collection will be returned, but if there
        is no current add in progress then the current time as a UNIX epoch is returned.

        :return: a version timestamp to use for adding
        """
        committed_version = self.committed_version
        data_version = self.get_mongo_version()

        # no data or status so generate a new version
        if committed_version is None and data_version is None:
            return now()

        # no status version, but there is data so use the latest data version
        elif committed_version is None and data_version is not None:
            return data_version

        # have a status version but no data version which is a weird state to be in,
        # clean it up and return a new version
        elif committed_version is not None and data_version is None:
            self.clear_status()
            return now()

        # have a status version and a data version
        else:
            if data_version > committed_version:
                # data version is newer, so data has been added without a commit. In
                # this case allow a continuation of adding data to this version by
                # returning the data version
                return data_version
            else:
                # otherwise data is behind status, so we should return a new version
                return now()

    def add(self, records: Iterable[Record], commit=True):
        """
        Adds the given records to the database. This only adds the records to the
        MongoDB data collection, it doesn't trigger the indexing of this new data into
        the Elasticsearch cluster.

        Use the commit keyword argument to either close the transaction after writing
        these records or leave it open. By default, the transaction is committed before
        the method returns.

        If an error occurs, the transaction will not be committed, but the changes will
        not be rolled back.

        :param records: the records to add. These will be added in batches, so it is
                        safe to pass a very large stream of records
        :param commit: whether to commit the new version to the status after writing the
                       records. Default: True.
        """
        # TODO: return some stats about the add
        # TODO: locking?
        version = self.determine_next_version()

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

    def sync(self):
        """
        Synchronise the data in MongoDB with the data in Elasticsearch by updating the
        latest and old data indices as required.

        To find the data that needs to be updated, the current version of the data in
        MongoDB is compared to the current version of the data in Elasticsearch, and the
        two are synced (assuming MongoDB's version is <= Elasticsearch).
        """
        # ensure the template exists
        self._client.elasticsearch.indices.put_index_template(
            name="data-template", body=DATA_TEMPLATE
        )

        since = self.committed_version
        # set up a stream of all the all records with a version newer than since
        docs = (
            MongoRecord(**doc)
            for doc in self.data_collection.find({"version": {"$gte": since}})
        )

        # we don't care about the results so just throw them away into a 0-sized deque
        deque(
            parallel_bulk(
                self._client.elasticsearch,
                generate_index_ops(self.name, docs, since),
                raise_on_error=True,
            ),
            maxlen=0,
        )
