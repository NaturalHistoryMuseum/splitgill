from collections import deque
from dataclasses import dataclass
from typing import Optional, Iterable, List

from cytoolz.dicttoolz import get_in
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.indexing.fields import MetaField
from splitgill.indexing.index import (
    generate_index_ops,
    get_latest_index_id,
    get_data_index_id,
)
from splitgill.indexing.options import ParsingOptionsRange
from splitgill.indexing.templates import DATA_TEMPLATE
from splitgill.ingest import generate_ops
from splitgill.model import Record, Status, MongoRecord, ParsingOptions
from splitgill.utils import now, partition

MONGO_DATABASE_NAME = "sg"
STATUS_COLLECTION_NAME = "status"
OPTIONS_COLLECTION_NAME = "options"
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

    def get_options_collection(self) -> Collection:
        """
        Returns the options collection.

        :return: a pymongo Collection object
        """
        return self.get_database().get_collection(OPTIONS_COLLECTION_NAME)

    def get_data_collection(self, name: str) -> Collection:
        """
        Returns the data collection for the given Splitgill database.

        :param name: the name of the Splitgill database
        :return: a pymongo Collection object
        """
        return self.get_database().get_collection(f"data-{name}")


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
        self.options_collection = self._client.get_options_collection()
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
        Returns the latest version found in the data collection or options collection
        for this database. If no records or options exist, None is returned.

        :return: the max version or None
        """
        sort = [("version", DESCENDING)]
        last_data = self.data_collection.find_one({}, sort=sort)
        last_options = self.options_collection.find_one({"name": self.name}, sort=sort)
        if last_data is None and last_options is None:
            return None
        return max(
            last["version"] for last in (last_data, last_options) if last is not None
        )

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
            index=f"data-{self.name}-*",
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
        Updates the status of this database with whichever is higher - the latest
        version in the data collection or the latest version for this database in the
        options collection. After this, no more records at that latest version can be
        added to the database, nor a new options update, they must all be newer.

        :return: True if the status was updated, False if not
        """
        # get the latest data/options version
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
        Figure out what version should be used with the next record(s) or options added
        to this database. If a transaction is in progress and changes haven't been
        committed then the latest version from the data collection or options collection
        for this database will be returned, but if there is no current change in
        progress then the current time as a UNIX epoch is returned.

        :return: a version timestamp to use for adding
        """
        committed_version = self.committed_version
        version = self.get_mongo_version()

        if committed_version is None:
            if version is None:
                # no versions found at all, generate a new version
                return now()
            else:
                # use the latest data/options version
                return version

        # have a status version but no data/options version which is a weird state to
        # be in, clean it up and return a new version
        if version is None:
            self.clear_status()
            return now()

        # have a status version and a data/options version
        if version > committed_version:
            # data/options version is newer, so data/options has been added without a
            # commit. In this case allow a continuation of adding data to this
            # version by returning the data/options version
            return version
        else:
            # otherwise data/options is behind status, so we should return a new
            # version
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

    def update_options(self, options: ParsingOptions, commit=True) -> bool:
        """
        Update the parsing options for this database.

        :param options: the new parsing options
        :param commit: whether to commit the new version to the status after writing the
                       config. Default: True.
        :return: a bool indicating whether the new options were different from the
                 currently saved options. Returns True if the options were different and
                 therefore updated, False if not.
        """
        latest_options = self.get_options().latest

        # if the options are the same as the latest existing ones, don't update
        if latest_options == options:
            return False

        # either the options are completely new or they differ from the existing
        # options, write a fresh entry
        new_doc = {
            "name": self.name,
            "version": self.determine_next_version(),
            "options": options.to_doc(),
        }
        self.options_collection.insert_one(new_doc)

        if commit:
            self.commit()

        return True

    def get_options(self) -> ParsingOptionsRange:
        """
        Retrieve all the parsing options ever configured for this database. The options
        are returned in a wrapping class to provide easy access by version.

        :return: a ParsingOptionsRange object
        """
        return ParsingOptionsRange(
            {
                doc["version"]: ParsingOptions.from_doc(doc["options"])
                for doc in self.options_collection.find({"name": self.name})
            }
        )

    def get_all_indices(self) -> List[str]:
        """
        Returns a list of all index possible index names for the data in this database.
        Some of these indices may not actually be in use in Elasticsearch depending on
        the sync state, but this list contains all the possible names given the range of
        versions in the current data in MongoDB.

        :return: a list of index names, in age order (latest, then decreasing years)
        """
        indices = [self.latest_index_name]
        indices.extend(
            # sort in descending order
            sorted(
                {
                    get_data_index_id(self.name, version)
                    for version in self.data_collection.distinct("version")
                },
                reverse=True,
            )
        )
        return indices

    def sync(self, parallel: bool = True, chunk_size: int = 500):
        """
        Synchronise the data in MongoDB with the data in Elasticsearch by updating the
        latest and old data indices as required.

        To find the data that needs to be updated, the current version of the data in
        MongoDB is compared to the current version of the data in Elasticsearch, and the
        two are synced (assuming MongoDB's version is <= Elasticsearch).

        While the data is being indexed, refreshing is paused on this database's indexes
        and only resumed once all the data has been indexed. If an error occurs during
        indexing then the refresh interval is not reset meaning the updates that have
        made it to Elasticsearch will not impact searches until a refresh is triggered,
        or the refresh interval is reset. This kinda makes this function transactional.
        If no errors occur, the refresh interval is reset (along with the replica count)
        and a refresh is called. This means that if this function returns successfully,
        the data updated by it will be immediately available for searches.

        :param parallel: send the data to Elasticsearch using multiple threads if True,
                         otherwise use a single thread if False
        :param chunk_size: the number of docs to send to Elasticsearch in each bulk
                           request
        """
        # we're gonna use this all over the place so cache it into a variable
        client = self._client.elasticsearch
        # choose which bulk function we're using based on the parallel parameter
        bulk_function = parallel_bulk if parallel else streaming_bulk

        # ensure the data template exists
        client.indices.put_index_template(name="data-template", body=DATA_TEMPLATE)

        # grab a list of all the indices we may update during this operation
        indices = self.get_all_indices()

        # create all the indices so we can apply optimal indexing settings to them and
        # refresh them at the end to make the new data visible
        for index in indices:
            if not client.indices.exists(index=index):
                client.indices.create(index=index)

        # set up a generator of all the records with a version newer than since
        since = self.committed_version
        docs = (
            MongoRecord(**doc)
            for doc in self.data_collection.find({"version": {"$gte": since}})
        )

        # apply optimal indexing settings to all the indices we may update
        client.indices.put_settings(
            body={"index": {"refresh_interval": -1, "number_of_replicas": 0}},
            index=indices,
        )

        # we don't care about the results so just throw them away into a 0-sized
        # deque (errors will be raised directly)
        deque(
            bulk_function(
                client,
                generate_index_ops(self.name, docs, since, self.get_options()),
                raise_on_error=True,
                chunk_size=chunk_size,
            ),
            maxlen=0,
        )

        # refresh all indices to make the changes visible all at once
        client.indices.refresh(index=indices)

        # reset the settings we changed (None forces them to revert to defaults)
        client.indices.put_settings(
            body={"index": {"refresh_interval": None, "number_of_replicas": None}},
            index=indices,
        )

        # do a bit of a tidy up by deleting any indexes without docs
        for index in indices:
            if not any(client.search(index=index, size=1)["hits"]["hits"]):
                client.indices.delete(index=index)
