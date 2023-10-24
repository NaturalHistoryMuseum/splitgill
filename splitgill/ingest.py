from typing import Iterable, Union

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

from splitgill.diffing import prepare, diff
from splitgill.model import Record, MongoRecord
from splitgill.utils import partition

MongoBulkOp = Union[InsertOne, UpdateOne]

FIND_SIZE = 100


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
        records_by_id = {record.id: record for record in chunk}
        docs = (
            MongoRecord(**doc)
            for doc in collection.find({"id": {"$in": list(records_by_id.keys())}})
        )
        existing = {doc.id: doc for doc in docs}

        for record_id, record in records_by_id.items():
            # this is a delete of a non-existent record, do nothing
            if not record.data and record_id not in existing:
                continue

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
