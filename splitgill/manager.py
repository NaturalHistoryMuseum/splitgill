from bisect import bisect_right
from enum import Enum
from typing import Optional, Iterable, List, Dict, Union

from cytoolz.dicttoolz import get_in
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A
from elasticsearch_dsl.query import Query
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.indexing.fields import (
    DocumentField,
    DataField,
    get_type_counts,
    ParsedField,
)
from splitgill.indexing.index import IndexNames, generate_index_ops
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.syncing import write_ops, WriteResult, BulkOptions
from splitgill.indexing.templates import DATA_TEMPLATE
from splitgill.ingest import generate_ops, generate_rollback_ops
from splitgill.locking import LockManager
from splitgill.model import Record, MongoRecord, ParsingOptions, IngestResult
from splitgill.search import create_version_query
from splitgill.utils import partition, now

MONGO_DATABASE_NAME = "sg"
OPTIONS_COLLECTION_NAME = "options"
LOCKS_COLLECTION_NAME = "locks"


class SplitgillClient:
    """
    Splitgill client class which holds a mongo connection, an elasticsearch connection
    and any other general information Splitgill needs to manage the databases.
    """

    def __init__(self, mongo: MongoClient, elasticsearch: Elasticsearch):
        self.mongo = mongo
        self.elasticsearch = elasticsearch
        self.lock_manager = LockManager(self.get_lock_collection())

    def get_database(self, name: str) -> "SplitgillDatabase":
        """
        Returns a SplitgillDatabase object.

        :param name: the name of the database
        :return: a new SplitgillDatabase object
        """
        return SplitgillDatabase(name, self)

    def get_mongo_database(self) -> Database:
        """
        Returns the MongoDB database in use.

        :return: a pymongo Database object
        """
        return self.mongo.get_database(MONGO_DATABASE_NAME)

    def get_options_collection(self) -> Collection:
        """
        Returns the options collection.

        :return: a pymongo Collection object
        """
        return self.get_mongo_database().get_collection(OPTIONS_COLLECTION_NAME)

    def get_data_collection(self, name: str) -> Collection:
        """
        Returns the data collection for the given Splitgill database.

        :param name: the name of the Splitgill database
        :return: a pymongo Collection object
        """
        return self.get_mongo_database().get_collection(f"data-{name}")

    def get_lock_collection(self) -> Collection:
        """
        Returns the locks collection.

        :return: a pymongo Collection object
        """
        return self.get_mongo_database().get_collection(LOCKS_COLLECTION_NAME)


class SearchVersion(Enum):
    """
    Indicator for the SplitgillDatabase.search method as to which version of the data
    you would like to search.
    """

    # searches the latest data
    latest = "latest"
    # searches all data
    all = "all"


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
        # Mongo collection objects
        self.data_collection = self._client.get_data_collection(self.name)
        self.options_collection = self._client.get_options_collection()
        # index names
        self.indices = IndexNames(self.name)
        self.locker = self._client.lock_manager

    def get_committed_version(self) -> Optional[int]:
        """
        Returns the latest committed version of the data or config (whichever is
        higher). If no records or options exist, or if neither has any committed values,
        None is returned.

        :return: the max version or None
        """
        sort = [("version", DESCENDING)]
        last_data = self.data_collection.find_one(sort=sort)
        last_options = self.options_collection.find_one({"name": self.name}, sort=sort)

        last_data_version = last_data.get("version") if last_data is not None else None
        last_options_version = (
            last_options.get("version") if last_options is not None else None
        )
        # there's no committed data or options
        if last_data_version is None and last_options_version is None:
            return None

        return max(
            last
            for last in (last_data_version, last_options_version)
            if last is not None
        )

    def get_elasticsearch_version(self) -> Optional[int]:
        """
        Returns the latest version found in the Elasticsearch indices for this database.
        If no records exist in any index, None is returned. This method checks both the
        maximum value in the version field and the next field. Checking the next field
        accounts for updates that only include deletions.

        :return: the max version or None
        """
        version = None
        for field in (DocumentField.VERSION, DocumentField.NEXT):
            result = self._client.elasticsearch.search(
                aggs={"max_version": {"max": {"field": field}}},
                size=0,
                # search all indices so that we catch deletes which won't have a
                # document in latest
                index=self.indices.wildcard,
            )
            value = get_in(("aggregations", "max_version", "value"), result, None)
            if value is not None and (version is None or value > version):
                version = value

        # elasticsearch does max aggs using the double type apparently, so we need to
        # convert it back to an int to avoid returning a float and causing confusion
        return int(version) if version is not None else None

    def get_rounded_version(self, version: int) -> Optional[int]:
        """
        Given a target version, rounds the version down to the nearest available
        version. This in effect returns the version of the data that is application to
        the given target version.

        If the target version is below the earliest version or this database's indexed
        data, or, no indexed versions are available, None is returned.

        :param version: the target version
        :return: a version or None
        """
        versions = self.get_versions()
        if not versions or version < versions[0]:
            return None

        return versions[bisect_right(versions, version) - 1]

    def has_data(self) -> bool:
        """
        Returns True if there is at least one committed record in this database,
        otherwise returns False. Note that this ignored options.

        :return: True if there is data, False if not
        """
        return self.data_collection.find_one({"version": {"$ne": None}}) is not None

    def has_options(self) -> bool:
        """
        Returns True if there is at least one committed options in this database,
        otherwise returns False. Note that this ignored data.

        :return: True if there is options, False if not
        """
        return self.options_collection.find_one({"version": {"$ne": None}}) is not None

    def commit(self) -> Optional[int]:
        """
        Commits the currently uncommitted data and options changes for this database.
        All new data/options will be given the same version which is the current time.
        If no changes were made, None is returned, otherwise the new version is
        returned.

        If a commit is already ongoing this will raise an AlreadyLocked exception.

        :return: the new version or None if no uncommitted changes were found
        """
        # todo: global now?
        # todo: transaction/rollback? Can't do this without replicasets so who knows?
        with self.locker.lock(self.name, stage="commit"):
            if not self.has_uncommitted_data() and not self.has_uncommitted_options():
                # nothing to commit, so nothing to do
                return None

            if not self.has_options() and not self.has_uncommitted_options():
                # no existing options and no options to be committed, so create some
                # basic parsing options to use
                options = ParsingOptionsBuilder().build()
                self.update_options(options, commit=False)

            version = now()

            # update the uncommitted data and options in a transaction
            for collection in [self.data_collection, self.options_collection]:
                collection.update_many(
                    filter={"version": None}, update={"$set": {"version": version}}
                )
            return version

    def ingest(
        self,
        records: Iterable[Record],
        commit=True,
        modified_field: Optional[str] = None,
    ) -> IngestResult:
        """
        Ingests the given records to the database. This only adds the records to the
        MongoDB data collection, it doesn't trigger the indexing of this new data into
        the Elasticsearch cluster. All data will be added with a None version unless the
        commit parameter is True in which case a version will be assigned.

        Use the commit keyword argument to either close the "transaction" after writing
        these records or leave it open. By default, the "transaction" is committed
        before the method returns, and the version is set then.

        If an error occurs, the "transaction" will not be committed, but the changes
        will not be rolled back.

        :param records: the records to add. These will be added in batches, so it is
                        safe to pass a very large stream of records
        :param commit: whether to commit the data added with a new version after writing
                       the records. Default: True.
        :param modified_field: a field name which, if the only changes in the record
                               data are in this field means the changes will be ignored.
                               As you can probably guess from the name, the root reason
                               for this parameter existing is to avoid committing a new
                               version of a record when all that has happened is the
                               record has been touched and the modified field's date
                               value updated even though the rest of the record remains
                               the same. Default: None, meaning all fields are checked
                               for changes.
        :return: returns a IngestResult object
        """
        # this does nothing if the indexes already exist
        self.data_collection.create_indexes(
            [IndexModel([("id", ASCENDING)]), IndexModel([("version", DESCENDING)])]
        )

        result = IngestResult()
        # this is used for the find size and the bulk ops partition size which both need
        # to be the same to ensure we can handle duplicates in the record stream
        size = 200

        for ops in partition(
            generate_ops(self.data_collection, records, modified_field, size), size
        ):
            bulk_result = self.data_collection.bulk_write(ops)
            result.update(bulk_result)

        if commit:
            result.version = self.commit()

        return result

    def update_options(self, options: ParsingOptions, commit=True) -> Optional[int]:
        """
        Update the parsing options for this database.

        :param options: the new parsing options
        :param commit: whether to commit the new config added with a new version after
                       writing the config. Default: True.
        :return: returns the new version if a commit happened, otherwise None. If a
                 commit was requested but nothing was changed, None is returned.
        """
        # get the latest options that have been committed (get_options ignores
        # uncommitted options)
        all_options = self.get_options()
        if all_options:
            latest_options = all_options[max(all_options)]
        else:
            latest_options = None

        if self.has_uncommitted_options():
            self.rollback_options()

        # if the options are the same as the latest existing ones, don't update
        if latest_options == options:
            return None

        # either the options are completely new or they differ from the existing
        # options, write a fresh entry
        new_doc = {
            "name": self.name,
            # version = None to indicate this is an uncommitted change
            "version": None,
            "options": options.to_doc(),
        }
        self.options_collection.insert_one(new_doc)

        if commit:
            return self.commit()
        return None

    def rollback_options(self) -> int:
        """
        Remove any uncommitted option changes.

        There should only ever be one, but this deletes them all ensuring everything is
        clean and tidy.

        :return: the number of documents deleted
        """
        return self.options_collection.delete_many({"version": None}).deleted_count

    def rollback_records(self):
        """
        Remove any uncommitted data changes.

        This method has to interrogate every uncommitted record in the data collection
        to perform the rollback and therefore, depending on how much uncommitted data
        there is, may take a bit of time to run.
        """
        if self.has_uncommitted_data():
            for ops in partition(generate_rollback_ops(self.data_collection), 200):
                self.data_collection.bulk_write(ops)

    def has_uncommitted_data(self) -> bool:
        """
        Check if there are any uncommitted records stored against this database.

        :return: returns True if there are any uncommitted records, False if not
        """
        return self.data_collection.find_one({"version": None}) is not None

    def has_uncommitted_options(self) -> bool:
        """
        Check if there are any uncommitted options stored against this database.

        :return: returns True if there are any uncommitted options, False if not
        """
        return self.options_collection.find_one({"version": None}) is not None

    def get_options(self, include_uncommitted=False) -> Dict[int, ParsingOptions]:
        """
        Retrieve all the parsing options configured for this database in a dict mapping
        int versions to ParsingOptions objects. Use the include_uncommitted parameter to
        indicate whether to include the uncommitted options or not.

        :return: a dict of versions and options
        """
        return {
            doc["version"]: ParsingOptions.from_doc(doc["options"])
            for doc in self.options_collection.find({"name": self.name})
            if include_uncommitted or doc["version"] is not None
        }

    def iter_records(self, **find_kwargs) -> Iterable[MongoRecord]:
        """
        Yields MongoRecord objects matching the given find kwargs. As you can probably
        guess, the find_kwargs argument is just passed directly to PyMongo's find
        method.

        :param find_kwargs: args to pass to the data collection's find method
        :return: yields matching MongoRecord objects
        """
        yield from (
            MongoRecord(**doc) for doc in self.data_collection.find(**find_kwargs)
        )

    def sync(
        self, bulk_options: Optional[BulkOptions] = None, resync: bool = False
    ) -> WriteResult:
        """
        Synchronise the data/options in MongoDB with the data in Elasticsearch by
        updating the latest and old data indices as required.

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

        :param bulk_options: options determining how the bulk operations are sent to
                             Elasticsearch
        :param resync: whether to resync all records with Elasticsearch regardless of
                       the currently synced version. This won't delete any data first
                       and just replaces documents in Elasticsearch as needed.
        :return: a WriteResult object
        """
        if not self.has_data():
            return WriteResult()

        all_options = self.get_options(include_uncommitted=False)
        last_sync = self.get_elasticsearch_version() if not resync else None
        if last_sync is None:
            # elasticsearch has nothing so find all committed records
            find_filter = {"version": {"$ne": None}}
        else:
            committed_version = self.get_committed_version()
            if last_sync >= committed_version:
                # elasticsearch and mongo are in sync (use >= rather than == just in
                # case some bizarro-ness has occurred)
                return WriteResult()
            if any(version > last_sync for version in all_options):
                # there's an options change ahead, this means we need to check all
                # records again, so filter out committed records only
                find_filter = {"version": {"$ne": None}}
            else:
                # find all the updated records that haven't had their updates synced yet
                find_filter = {"version": {"$gt": last_sync}}

        client = self._client.elasticsearch

        client.indices.put_index_template(name="data-template", body=DATA_TEMPLATE)
        for index in self.indices.all:
            if not client.indices.exists(index=index):
                client.indices.create(index=index)

        # apply optimal indexing settings to all the indices we may update
        client.indices.put_settings(
            body={"index": {"refresh_interval": -1, "number_of_replicas": 0}},
            index=self.indices.all,
        )

        result = write_ops(
            client,
            generate_index_ops(
                self.indices,
                self.iter_records(filter=find_filter),
                all_options,
                last_sync,
            ),
            bulk_options,
        )

        # refresh all indices to make the changes visible all at once
        client.indices.refresh(index=self.indices.all)

        # reset the settings we changed (None forces them to revert to defaults)
        client.indices.put_settings(
            body={"index": {"refresh_interval": None, "number_of_replicas": None}},
            index=self.indices.all,
        )

        # do a bit of a tidy up by deleting any indexes without docs
        for index in self.indices.all:
            if not any(client.search(index=index, size=1)["hits"]["hits"]):
                client.indices.delete(index=index)

        return result

    def search(
        self, version: Union[SearchVersion, int] = SearchVersion.latest
    ) -> Search:
        """
        Creates a Search DSL object to use on this database's indexed data. This Search
        object will be setup with the appropriate index and version filter depending on
        the given version parameter, and the Elasticsearch client object in use on this
        database.

        If a version number is passed as the version parameter, it will be checked
        against the latest version available in Elasticsearch. If it is below the latest
        version available in Elasticsearch, all indices will be searched and a term
        filter will be used to get the right data. if the version is equal to or above
        the latest version available in Elasticsearch, the latest index will be searched
        and no version term filter will be used. This is for Elasticsearch performance
        and caching.

        :param version: the version to search at, this should either be a SearchVersion
                        enum option or an int. SearchVersion.latest will result in a
                        search on the latest index with no version filter thus searching
                        the latest data. SearchVersion.all will result in a search on
                        all indices using a wildcard and no version filter. Passing an
                        int version will search at the given timestamp. The default is
                        SearchVersion.latest.
        :return: a Search DSL object
        """
        search = Search(using=self._client.elasticsearch)

        if isinstance(version, int):
            current_version = self.get_elasticsearch_version()
            if current_version is not None and current_version <= version:
                # the version requested is above the latest version, use the latest
                # index instead of a filter, it'll be faster and more easily cachable
                # for elasticsearch
                search = search.index(self.indices.latest)
            else:
                search = search.index(self.indices.wildcard)
                search = search.filter(create_version_query(version))
        else:
            if version == SearchVersion.latest:
                search = search.index(self.indices.latest)
            elif version == SearchVersion.all:
                search = search.index(self.indices.wildcard)

        return search

    def get_versions(self) -> List[int]:
        """
        Returns a list of the available versions that have been indexed into
        Elasticsearch for this database. The versions are in ascending order and will be
        retrieved from both the version and next document fields to ensure we capture
        all versions.

        :return: the available versions in ascending order
        """
        versions = set()

        # get all versions present in the version and next document fields
        for field in (DocumentField.VERSION, DocumentField.NEXT):
            after = None
            while True:
                search = self.search(version=SearchVersion.all)[:0]
                search.aggs.bucket(
                    "versions",
                    "composite",
                    size=50,
                    sources={"version": A("terms", field=field, order="asc")},
                )
                if after is not None:
                    search.aggs["versions"].after = after
                result = search.execute().aggs.to_dict()
                buckets = get_in(("versions", "buckets"), result, [])
                after = get_in(("versions", "after_key"), result, None)
                if not buckets:
                    break
                versions.update(bucket["key"]["version"] for bucket in buckets)

        return sorted(versions)

    def get_data_fields(
        self, version: Optional[int] = None, query: Optional[Query] = None
    ) -> List[DataField]:
        """
        Retrieves the available data fields for this database, optionally at the given
        version with the given query.

        :param version: the version to find data fields at, if None, the latest data is
                        searched
        :param query: the query to filter records with before finding the data fields,
                      if None, all record data is considered
        :return: a list of DataField objects with the most frequent field first
        """
        search = self.search(version if version is not None else SearchVersion.latest)
        if query is not None:
            search = search.filter(query)
        paths_and_counts = get_type_counts(DocumentField.DATA_TYPES, search)

        fields: Dict[str, DataField] = {}

        # create the basic field objects and add type counts
        for path_and_type, count in paths_and_counts:
            path, raw_types = path_and_type.rsplit(".", 1)
            if path not in fields:
                fields[path] = DataField(path)
            fields[path].add(raw_types, count)

        # go through each field and link it with other fields to create hierarchy
        for field in fields.values():
            if not field.is_container:
                continue
            target_dot_count = field.path.count(".") + 1
            for child in fields.values():
                if child.path.count(".") == target_dot_count and child.path.startswith(
                    f"{field.path}."
                ):
                    field.children.append(child)
                    child.parent = field

        # return the data fields as a list in a specific order. Note that because we're
        # using multiple orderings in different directions, we do multiple sorts in the
        # reverse order of the order we want them applied.
        # descending depth (so fields closest to the root first)
        data_fields = sorted(
            fields.values(), key=lambda f: f.path.count("."), reverse=True
        )
        # ascending alphabetical order
        data_fields.sort(key=lambda f: f.path)
        # descending frequency (so most frequent fields first)
        data_fields.sort(key=lambda f: f.count, reverse=True)
        return data_fields

    def get_parsed_fields(
        self, version: Optional[int] = None, query: Optional[Query] = None
    ) -> List[ParsedField]:
        """
        Retrieves the available parsed fields for this database, optionally at the given
        version with the given query.

        :param version: the version to find parsed fields at, if None, the latest data
                        is searched
        :param query: the query to filter records with before finding the parsed fields,
                      if None, all record data is considered
        :return: a list of ParsedField objects with the most frequent field first
        """
        search = self.search(version if version is not None else SearchVersion.latest)
        if query is not None:
            search = search.filter(query)
        paths_and_counts = get_type_counts(DocumentField.PARSED_TYPES, search)

        fields: Dict[str, ParsedField] = {}

        # create the basic field objects and add type counts
        for path_and_type, count in paths_and_counts:
            path, raw_types = path_and_type.rsplit(".", 1)
            if path not in fields:
                fields[path] = ParsedField(path)
            fields[path].add(raw_types, count)

        # return the parsed fields as a list in a specific order. Note that because
        # we're using multiple orderings in different directions, we do multiple sorts
        # in the reverse order of the order we want them applied.
        # descending depth (so fields closest to the root first)
        parsed_fields = sorted(
            fields.values(), key=lambda f: f.path.count("."), reverse=True
        )
        # ascending alphabetical order
        parsed_fields.sort(key=lambda f: f.path)
        # descending frequency (so most frequent fields first)
        parsed_fields.sort(key=lambda f: f.count, reverse=True)
        return parsed_fields
