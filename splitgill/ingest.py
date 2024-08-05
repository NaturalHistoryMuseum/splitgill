from itertools import islice
from typing import Iterable, Union, Optional

from pymongo import InsertOne, UpdateOne, DeleteOne
from pymongo.collection import Collection

from splitgill.diffing import prepare_data, diff
from splitgill.model import Record, MongoRecord
from splitgill.utils import partition

MongoBulkOp = Union[InsertOne, UpdateOne, DeleteOne]


def generate_ops(
    data_collection: Collection,
    records: Iterable[Record],
    modified_field: Optional[str] = None,
    find_size: int = 100,
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
    :param modified_field: optional field containing a modified date. If this parameter
                           is specified, the check to see if there are any changes
                           between the old and new versions of the data will ignore this
                           field (if there are other fields that have changed, then a
                           full diff is generated with these fields included). Defaults
                           to None, indicating no modified field should be used.
    :param find_size: the number of records look up at a time. This corresponds directly
                      to the size of the $in query ID list. Defaults to 100.
    :return: yields bulk Mongo ops
    """
    # todo: refactor this, it's a bit messy
    for chunk in partition(records, find_size):
        records_by_id = {record.id: record for record in chunk}
        # find if any of the records to be added/updated already exist in the collection
        existing = {
            doc["id"]: MongoRecord(**doc)
            for doc in data_collection.find({"id": {"$in": list(records_by_id)}})
        }

        # shortcut if no records exist
        if not existing:
            yield from (
                InsertOne(
                    {
                        "id": record.id,
                        "data": prepare_data(record.data),
                        "version": None,
                    }
                )
                for record in records_by_id.values()
                if not record.is_delete
            )
            continue

        for record_id, record in records_by_id.items():
            # a delete of a non-existent record, ignore
            if record.is_delete and record_id not in existing:
                continue

            # prepare the record's data, we will use this as both the record's new data
            # that we actually store in Mongo and also to diff against any existing data
            new_data = prepare_data(record.data)

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
                    elif any(diff(new_data, existing_record.data)):
                        # the current record has one uncommitted version of the data and
                        # no previous versions, just replace its data with the new data
                        yield UpdateOne({"id": record.id}, {"$set": {"data": new_data}})
                    # the existing and new data are the same, nothing to do
                    continue
                else:
                    # revert the local object version back to the previous version of
                    # the record's data and stash the UpdateOp
                    revert_update_op = revert_record(existing_record)

            if (
                modified_field is not None
                and modified_field in new_data
                and modified_field in existing_record.data
            ):
                # pop the modified values
                new_value = new_data.pop(modified_field)
                existing_value = existing_record.data.pop(modified_field)
                # check if there are any other changes
                other_changes = any(diff(new_data, existing_record.data))
                # put the values back
                new_data[modified_field] = new_value
                existing_record.data[modified_field] = existing_value
                if other_changes:
                    # generate a full diff
                    changes = list(diff(new_data, existing_record.data))
                else:
                    # indicate that there are no changes between the new and old data
                    changes = []
            else:
                changes = list(diff(new_data, existing_record.data))

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
