from collections import deque
from dataclasses import asdict
from typing import Optional, Iterable, List, Dict

from cytoolz.dicttoolz import get_in
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, streaming_bulk
from elasticsearch_dsl import Search, A
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.indexing import fields
from splitgill.indexing.index import (
    generate_index_ops,
    get_latest_index_id,
    get_data_index_id,
    get_index_wildcard,
)
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.templates import DATA_TEMPLATE
from splitgill.ingest import generate_ops, generate_rollback_ops
from splitgill.model import Record, MongoRecord, ParsingOptions, IngestResult
from splitgill.profiles import Profile, build_profile
from splitgill.utils import partition, now

MONGO_DATABASE_NAME = "sg"
OPTIONS_COLLECTION_NAME = "options"
PROFILES_INDEX_NAME = "profiles"


class SplitgillClient:
    """
    Splitgill client class which holds a mongo connection, an elasticsearch connection
    and any other general information Splitgill needs to manage the databases.
    """

    def __init__(self, mongo: MongoClient, elasticsearch: Elasticsearch):
        self.mongo = mongo
        self.elasticsearch = elasticsearch
        self.profile_manager = ProfileManager(elasticsearch)

    def get_database(self) -> Database:
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
        self.latest_index_name = get_latest_index_id(self.name)

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
        If no records exist in any index, None is returned.

        :return: the max version or None
        """
        result = self._client.elasticsearch.search(
            aggs={"max_version": {"max": {"field": fields.VERSION}}},
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

        :return: the new version or None if no uncommitted changes were found
        """
        # TODO: locking?
        # TODO: global now?
        # TODO: transaction/rollback? Can't do this without replicasets so who knows?
        if not self.has_uncommitted_data() and not self.has_uncommitted_options():
            # nothing to commit, so nothing to do
            return None

        if not self.has_options() and not self.has_uncommitted_options():
            # no existing options and no options to be committed, create a default
            self.update_options(ParsingOptionsBuilder().build(), commit=False)

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

        :param parallel: send the data to Elasticsearch using multiple threads if True,
                         otherwise use a single thread if False
        :param chunk_size: the number of docs to send to Elasticsearch in each bulk
                           request
        """
        if not self.has_data():
            return

        all_options = self.get_options(include_uncommitted=False)
        last_sync = self.get_elasticsearch_version()
        if last_sync is None:
            # elasticsearch has nothing so find all committed records
            find_filter = {"version": {"$ne": None}}
        else:
            committed_version = self.get_committed_version()
            if last_sync >= committed_version:
                # elasticsearch and mongo are in sync (use >= rather than == just in case
                # some bizarro-ness has occurred)
                return
            if any(version > last_sync for version in all_options):
                # there's an options change ahead, this means we need to check all
                # records again, so filter out committed records only
                find_filter = {"version": {"$ne": None}}
            else:
                # find all the updated records that haven't had their updates synced yet
                find_filter = {"version": {"$gt": last_sync}}

        client = self._client.elasticsearch
        bulk_function = parallel_bulk if parallel else streaming_bulk

        client.indices.put_index_template(name="data-template", body=DATA_TEMPLATE)
        indices = self.get_all_indices()
        for index in indices:
            if not client.indices.exists(index=index):
                client.indices.create(index=index)

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
                generate_index_ops(
                    self.name,
                    self.iter_records(filter=find_filter),
                    all_options,
                    last_sync,
                ),
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

        self._client.profile_manager.update_profiles(self)

    def search(self, latest: bool) -> Search:
        """
        Creates a Search DSL object to use on this database's indexed data. This Search
        object will be setup with the appropriate index depending on the given latest
        parameter and the Elasticsearch client object in use on this database.

        :param latest: whether to search the latest data or all data, this impacts the
                       indexes that are added to the returned Search object
        :return: a Search DSL object
        """
        # TODO: add more precise versioning options (latest, none, a specific version)
        if latest:
            index = self.latest_index_name
        else:
            index = get_index_wildcard(self.name)
        return Search(using=self._client.elasticsearch, index=index)

    def get_available_versions(self) -> List[int]:
        """
        Returns a list of the available versions that have been indexed into
        Elasticsearch for this database. The versions are in ascending order.

        :return: the available versions in ascending order
        """
        versions = set()
        after = None
        while True:
            search = self.search(latest=False)[:0]
            search.aggs.bucket(
                "versions",
                "composite",
                size=50,
                sources={"version": A("terms", field=fields.VERSION, order="asc")},
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

    def get_profile(self, version: int) -> Optional[Profile]:
        """
        Given a version, gets the data profile that applies, if there is one available.

        :param version: the data version to get the profile for
        :return: a Profile object or None
        """
        return self._client.profile_manager.get_profile(self.name, version)

    def update_profiles(self, rebuild: bool = False):
        """
        Force an update of the profiles for this database, optionally rebuilding them.

        :param rebuild: whether to rebuild the profiles completely
        """
        self._client.profile_manager.update_profiles(self, rebuild)


class ProfileManager:
    """
    Class that manages all database profiles in Elasticsearch.
    """

    def __init__(self, elasticsearch: Elasticsearch):
        """
        :param elasticsearch: an Elasticsearch client
        """
        self._elasticsearch = elasticsearch

    def _index_exists(self) -> bool:
        """
        Check if the profiles index exists.

        :return: True if the profiles index exists, False if not
        """
        return self._elasticsearch.indices.exists(index=PROFILES_INDEX_NAME)

    def _create_index(self):
        """
        If the profiles index doesn't exist, create it.
        """
        if self._index_exists():
            return
        self._elasticsearch.indices.create(
            index=PROFILES_INDEX_NAME,
            mappings={
                "properties": {
                    "name": {"type": "keyword"},
                    "version": {"type": "long"},
                    "total": {"type": "long"},
                    "changes": {"type": "long"},
                    "fields": {
                        "properties": {
                            "name": {"type": "keyword"},
                            "path": {"type": "keyword"},
                            "count": {"type": "long"},
                            "boolean_count": {"type": "long"},
                            "date_count": {"type": "long"},
                            "number_count": {"type": "long"},
                            "array_count": {"type": "long"},
                            "is_value": {"type": "boolean"},
                            "is_parent": {"type": "boolean"},
                        }
                    },
                },
            },
        )

    def get_profile_versions(self, name: str) -> List[int]:
        """
        Returns a list of the profile versions that are currently available for the
        given database name. The list of versions is sorted in ascending order.

        :param name: the database name
        :return: the versions in ascending order
        """
        return [profile.version for profile in self.get_profiles(name)]

    def get_profile(self, name: str, version: int) -> Optional[Profile]:
        """
        Returns the profile that applies to the given version of the database with the
        given name. If no profile applies then None is returned. Each profile has a
        version and is applicable from that version (inclusive) until the next version
        (exclusive) that is available supersedes it. If no version supersedes then the
        version is the latest.

        :param name: the database name
        :param version: the version to get a profile for
        :return: a profile if one can be found, otherwise None
        """
        candidate = None
        for profile in self.get_profiles(name):
            if version >= profile.version:
                candidate = profile
            else:
                break
        return candidate

    def get_profiles(self, name: str) -> List[Profile]:
        """
        Return a list of all the profiles available for this database, sorted in
        ascending version order.

        :param name: the name of the database
        :return: a list of Profile objects
        """
        if not self._index_exists():
            return []
        search = Search(using=self._elasticsearch, index=PROFILES_INDEX_NAME).filter(
            "term", name=name
        )
        profiles = [Profile.from_dict(hit.to_dict()) for hit in search.scan()]
        return sorted(profiles, key=lambda profile: profile.version)

    def update_profiles(self, database: SplitgillDatabase, rebuild: bool = False):
        """
        Updates the profiles for the given database. This will find all the available
        versions of the database, check if any versions don't have a profile, and then
        create those versions as needed. If all versions have profiles, nothing happens.

        If the rebuild option is True, all the profiles are deleted and recreated. This
        may take a bit of time depending on the number of versions and size of the
        database.

        :param database: the database to profile
        :param rebuild: whether to rebuild all the profiles for all versions
        """
        if rebuild and self._index_exists():
            # delete all the profiles
            self._elasticsearch.delete_by_query(
                index=PROFILES_INDEX_NAME,
                refresh=True,
                query=Search().filter("term", name=database.name).to_dict(),
            )

        profiled_versions = set(self.get_profile_versions(database.name))
        for version in database.get_available_versions():
            if version in profiled_versions:
                continue
            # no profile available for this version, build it
            profile = build_profile(self._elasticsearch, database.name, version)

            # create a doc from the profile and then add some extras
            doc = {**asdict(profile), "name": database.name, "version": version}

            # make sure the index exists
            self._create_index()

            # add to the profiles index
            self._elasticsearch.create(
                index=PROFILES_INDEX_NAME,
                id=f"{database.name}-{version}",
                document=doc,
                refresh=True,
            )
