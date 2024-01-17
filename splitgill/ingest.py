from itertools import islice
from typing import Iterable, Union, Optional

from pymongo import InsertOne, UpdateOne, DeleteOne
from pymongo.collection import Collection

from splitgill.diffing import prepare, diff
from splitgill.model import Record, MongoRecord
from splitgill.utils import partition

MongoBulkOp = Union[InsertOne, UpdateOne, DeleteOne]

FIND_SIZE = 100


def generate_ops(
    data_collection: Collection, records: Iterable[Record]
) -> Iterable[MongoBulkOp]:
    """
    Yields MongoDB bulk operations to insert or modify records in the given collection
    with the record data in the records iterable. All data is added with the version set
    to None.

    New records will be added with an InsertOne operation. If the new data is an empty
    dict (i.e. a delete) then the record is ignored and no operation is yielded.

    Existing committed records where the record data has changed will be diffed and be
    updated through an UpdateOne operation.

    Existing uncommitted records with no previous versions where the data has changed
    will be updated directly through an UpdateOne operation which just replaces the old
    data with the new data. If the data is the same then nothing happens and the record
    is ignored.

    Existing uncommitted records with no previous versions where the new data is empty
    (i.e. a delete) will be removed using a DeleteOne operation.

    Existing uncommitted records with previous versions will have the new data compared
    to the previous version and then be updated accordingly. If the new data is
    different, then an UpdateOne operation is yielded to update the data and replace the
    previous version's diff. If the new data simply rolls the record back to the
    previous version's data, then the previous diff is deleted and the data is restored
    back to this previous version using an UpdateOne operation.

    :param data_collection: the data collection containing any existing records
    :param records: the records to generate insert/update ops for
    :return: yields bulk Mongo ops
    """
    # TODO: refactor this, it's a bit messy
    for chunk in partition(records, FIND_SIZE):
        records_by_id = {record.id: record for record in chunk}
        # find if any of the records to be added/updated already exist in the collection
        existing = {
            doc["id"]: MongoRecord(**doc)
            for doc in data_collection.find({"id": {"$in": list(records_by_id)}})
        }

        for record_id, record in records_by_id.items():
            # a delete of a non-existent record, ignore
            if record.is_delete and record_id not in existing:
                continue

            new_data = prepare(record.data)

            if record_id not in existing:
                # the record is new, insert and carry on to the next
                yield InsertOne({"id": record_id, "data": new_data, "version": None})
                continue

            existing_record = existing[record_id]

            # check if there is uncommitted data already on the record and handle
            revert_update_op = None
            if existing_record.version is None:
                if not existing_record.diffs:
                    if not record.data:
                        # the uncommitted record is being deleted, so delete it!
                        yield DeleteOne({"id": record.id})
                    elif existing_record.data != new_data:
                        # the current record has one uncommitted version of the data and
                        # no previous versions, just replace its data with the new data
                        yield UpdateOne({"id": record.id}, {"$set": {"data": new_data}})
                    # no diff needs to be generated, so move on
                    continue
                else:
                    # revert the local object version back to the previous version of
                    # the record's data and stash the UpdateOp to update Mongo in case
                    # we need to use it lower down
                    revert_update_op = revert_record(existing_record)

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
                            # set version to None to indicate the change is uncommitted
                            "version": None,
                            # add diff at previous version
                            f"diffs.{existing_record.version}": changes,
                        },
                    },
                )
            elif revert_update_op is not None:
                # if there are no changes between the new data and the reverted version
                # of the data, yield the update op we created earlier to update Mongo
                # back to this state
                yield revert_update_op


def generate_rollback_ops(data_collection: Collection) -> Iterable[MongoBulkOp]:
    """
    Given a data collection, rollback any uncommitted changes. Depending on the state of
    each record, this will either completely delete the record if it was new, or it will
    revert the uncommitted changes and return the record back to its previous version.

    :param data_collection: the data collection to operate on
    :return: yields bulk Mongo ops
    """
    for doc in data_collection.find({"version": None}):
        record = MongoRecord(**doc)
        if not record.diffs:
            # the record is just uncommitted data and nothing else, just delete it
            yield DeleteOne({"id": record.id})
        else:
            # there is uncommitted data on this record, roll it back and then update
            op = revert_record(record)
            if op is not None:
                yield op


def revert_record(record: MongoRecord) -> Optional[UpdateOne]:
    """
    Revert the given record's data to the previous version. Note that this method only
    modifies the internal state of the MongoRecord object passed in, it will not update
    MongoDB itself, but it will return an UpdateOne object which can be used to update
    the state of this record in Mongo.

    If there are no previous versions of the record's data then nothing happens and None
    is returned as there is no previous version to revert back to.

    If the current version of the record is not None (i.e. it is committed) then None is
    returned as you shouldn't be reverting committed data, that breaks Splitgill!

    :return: an UpdateOne object if there was a previous version to revert to and
             therefore the revert was completed, None if not
    """
    if not record.diffs or record.version is not None:
        return None

    record.version, record.data = next(islice(record.iter(), 1, None), None)
    del record.diffs[str(record.version)]
    return UpdateOne(
        {"id": record.id},
        {
            "$set": {
                # update the data and the version
                "data": record.data,
                "version": record.version,
            },
            # delete the entry from the diffs, or delete the diffs completely if the
            # version we just reverted back to was the only previous version
            "$unset": {"diffs" if not record.diffs else f"diffs.{record.version}": ""},
        },
    )
