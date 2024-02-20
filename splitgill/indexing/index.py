from datetime import datetime
from functools import lru_cache
from typing import Optional, Iterable

from splitgill.indexing import fields
from splitgill.indexing.options import ParsingOptionsRange
from splitgill.indexing.parser import parse_for_index
from splitgill.model import MongoRecord, ParsingOptions


@lru_cache
def get_data_index_id(name: str, version: int) -> str:
    """
    Given a name and a version, return the name of the index non-latest data for this
    version should be added to.

    :param name: the Splitgill database name
    :param version: the version
    :return: the name of the index to use for data at this version
    """
    return f"data-{name}-{datetime.fromtimestamp(version / 1000).year}"


def get_latest_index_id(name: str) -> str:
    """
    Given a name, return the name of the latest data index for this database.

    :param name: the Splitgill database name
    :return: the name of the latest data index for this database
    """
    return f"data-{name}-latest"


def get_index_wildcard(name: str) -> str:
    """
    Given a database name, return a wildcard that covers all indices for the database.

    :param name: the Splitgill database name
    :return: the wildcard name
    """
    return f"data-{name}-*"


def create_index_op(
    index_name: str,
    record_id: str,
    data: dict,
    version: int,
    options: ParsingOptions,
    next_version: Optional[int] = None,
) -> dict:
    """
    Creates an index op for Elasticsearch to process (is just a dict).

    :param index_name: the name of the index this op will operate on
    :param record_id: the record's id
    :param data: the record's data
    :param version: the record's version
    :param options: the options for creating the indexed view of the data
    :param next_version: the next version of this record's data (i.e. when this version
                         expires). Defaults to None which means that the version passed
                         is the latest version.
    :return: an Elasticsearch index op as a dict
    """
    parsed_data = parse_for_index(data, options)
    op = {
        "_op_type": "index",
        "_index": index_name,
        fields.ID: record_id,
        fields.VERSION: version,
        fields.VERSIONS: {
            "gte": version,
        },
        fields.DATA: data,
        fields.PARSED: parsed_data.parsed,
        fields.GEO: parsed_data.geo,
        fields.LISTS: parsed_data.lists,
    }

    if parsed_data.geo:
        # create a collection using the individual geo GeoJSON values
        op[fields.GEO_ALL] = {
            "type": "GeometryCollection",
            "geometries": list(parsed_data.geo.values()),
        }

    if next_version is None:
        op["_id"] = record_id
    else:
        op["_id"] = f"{record_id}:{version}"
        op[fields.NEXT] = next_version
        op[fields.VERSIONS]["lt"] = next_version

    return op


def generate_index_ops(
    name: str,
    records: Iterable[MongoRecord],
    current: Optional[int],
    options: ParsingOptionsRange,
) -> Iterable[dict]:
    """
    Yields operations to run against Elasticsearch in order to update the indices in the
    cluster for the given database name with the data in the given records. The current
    parameter should hold the value of the current version in the cluster for this
    database. Ops will be yielded that update the cluster from this current version to
    the latest version available for each record.

    :param name: the name of the database
    :param records: the records to update from
    :param current: the latest version in Elasticsearch, None if there isn't any data in
                    elasticsearch
    :param options: ParsingOptionsRange object so that we can get the right parsing
                    options for indexing
    :return: yields ops as dicts
    """
    latest_index = get_latest_index_id(name)
    # only check the version of the record against the current version of elasticsearch
    # if there is something to compare against
    do_version_check = current is not None

    for record in records:
        if do_version_check and record.version < current:
            # nothing to do for this record, move on
            continue

        next_version = None
        # only the first time through the loop is the data the latest data and this is
        # always true, so set this here to True and then at the end of the loop we'll
        # reset this to False ensuring it is only True one time through
        is_latest = True

        for version, data in record.iter():
            if not data:
                # we only need to do something with a delete if it's the current latest
                # data, otherwise we can ignore it
                if is_latest:
                    # delete the data from the latest index
                    yield {
                        "_op_type": "delete",
                        "_index": latest_index,
                        "_id": record.id,
                    }
            else:
                index = latest_index if is_latest else get_data_index_id(name, version)
                yield create_index_op(
                    index, record.id, data, version, options.get(version), next_version
                )

            # if the version is below the current version in Elasticsearch, we're done
            if do_version_check and version < current:
                break

            # update the next version to the version we just handled
            next_version = version
            # the data is the latest only the first time through the loop, so set False
            is_latest = False
