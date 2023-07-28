from typing import Iterable, Union, Optional

from pymongo import InsertOne, UpdateOne, DESCENDING
from pymongo.collection import Collection

from splitgill.diffing import prepare, diff
from splitgill.model import Record, MongoRecord
from splitgill.utils import partition

MongoBulkOp = Union[InsertOne, UpdateOne]

FIND_SIZE = 100


def get_version(collection: Collection) -> Optional[int]:
    """
    Returns the latest version found in the data collection. If no records exist in the
    collection, None is returned.

    :param collection: a data collection
    :return: the max version or None
    """
    last = next(collection.find().sort("version", DESCENDING).limit(1), None)
    if last is None:
        return None
    return last["version"]


def generate_ops(
    collection: Collection, records: Iterable[Record], version: int
) -> Iterable[MongoBulkOp]:
    """
    Yields MongoDB bulk operations to either insert or update records in the given
    collection. This function will yield InsertOne ops for new records and UpdateOne ops
    for records that already exist but need a new version added.

    :param collection: the data collection containing any existing records
    :param records: the records to generate insert/update ops for
    :param version: the version of the records being inserted/updated
    :return: yields InsertOne or UpdateOne ops
    """
    for chunk in partition(records, FIND_SIZE):
        records = {record.id: record for record in chunk}
        docs = (
            MongoRecord(**doc)
            for doc in collection.find({"id": {"$in": list(records.keys())}})
        )
        existing = {doc.id: doc for doc in docs}

        for record_id, record in records.items():
            new_data = prepare(record.data)

            if record_id not in existing:
                # add a new record and carry on to the next
                yield InsertOne({"id": record_id, "data": new_data, "version": version})
                continue

            existing_record = existing[record_id]

            # this probably isn't necessary but better safe than sorry
            if version <= existing_record.version:
                # ignore this record, it's older than the one in the collection
                continue

            changes = tuple(diff(new_data, existing_record.data))
            if changes:
                # the existing record has been updated, yield the op necessary to
                # update it in mongo
                yield UpdateOne(
                    {"id": record.id},
                    {
                        "$set": {
                            # set new latest data
                            "data": new_data,
                            # set new current version
                            "version": version,
                            # add diff at previous version
                            f"diffs.{existing_record.version}": changes,
                        },
                    },
                )
