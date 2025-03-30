import abc
import math
from dataclasses import dataclass
from typing import Optional, Iterable, Dict, NamedTuple

from orjson import dumps, OPT_NON_STR_KEYS

from splitgill.indexing.fields import DocumentField
from splitgill.indexing.parser import parse
from splitgill.model import MongoRecord, ParsingOptions


# the maximum number of documents to store in an arc index before creating a new one
MAX_DOCS_PER_ARC = 2_000_000


class ArcStatus(NamedTuple):
    """
    Named tuple representing the status of the archive indices in a database by
    detailing the index of the last arc index and the number documents in it.

    The last arc is the arc which has most recently been used.
    """

    index: int
    count: int


class IndexNames:
    """
    A class holding index names and index wildcard patterns for the various index names
    used to store the data from the given named database in Elasticsearch.

    For each database, an index called data-{name}-latest is likely to exist* and then,
    depending on how many versions of the data there are, a series of archive
    indices called data-{name}-arc-{index}. The highest indexed archive index holds the
    most recent data which isn't latest, while the lowest holds the oldest.

    This means you can use:

        - data-{name}-* for all data in the database with {name}
        - data-{name}-latest for the latest data in the database with {name}
        - data-{name}-arc-* for all archived data in the database with {name}
        - data-* for all data in all databases
        - data-*-latest for all latest data
        - data-*-arc for all archived data

    This class creates these names and stores them in attributes for easy and consistent
    access.
    """

    def __init__(self, name: str):
        self.name = name
        # the base name of all indices for this database
        self.base = f"data-{name}"
        # the latest index name
        self.latest = f"{self.base}-latest"
        # the archive indices base name
        self.arc_base = f"{self.base}-arc"
        # wildcard name to catch all data indices (so both latest and all arcs)
        self.wildcard = f"{self.base}-*"
        # wildcard name to catch all arc indices
        self.arc_wildcard = f"{self.arc_base}-*"

    def get_arc(self, index: int) -> str:
        """
        Creates the name of the archive database with the given index and returns it.

        :param index: the archive index
        :return: the index name
        """
        return f"{self.arc_base}-{index}"


@dataclass
class BulkOp(abc.ABC):
    """
    Abstract class representing a bulk Elasticsearch operation.
    """

    index: str
    # if doc_id is set to None, Elasticsearch will create an ID for the document
    doc_id: Optional[str]

    def serialise(self) -> str:
        """
        Serialise the op ready to send to Elasticsearch.

        :return: a utf-8 encoded str
        """
        ...


@dataclass
class IndexOp(BulkOp):
    """
    An index bulk operation.
    """

    document: dict

    def serialise(self) -> str:
        # index ops are 2 lines, first the action metadata and then the document
        return (
            dumps({"index": {"_index": self.index, "_id": self.doc_id}})
            + b"\n"
            # we have to use OPT_NON_STR_KEYS because we're using StrEnums and orjson
            # doesn't work with them :(
            + dumps(self.document, option=OPT_NON_STR_KEYS)
        ).decode("utf-8")


@dataclass
class DeleteOp(BulkOp):
    """
    An delete bulk operation.
    """

    def serialise(self) -> str:
        # delete ops are only one line of JSON, for speed build it directly as a str
        return f'{{"delete":{{"_index":"{self.index}","_id":"{self.doc_id}"}}}}'


def generate_index_ops(
    indices: IndexNames,
    arc_status: ArcStatus,
    records: Iterable[MongoRecord],
    all_options: Dict[int, ParsingOptions],
    after: Optional[int],
) -> Iterable[BulkOp]:
    """
    Yield bulk index operations to run on Elasticsearch to update the indices of the
    given database name with the data in the given records using the given options. The
    after parameter specifies the version from which the index operations should begin
    (exclusive). Typically, therefore, after = the latest version in elasticsearch for
    this database.

    If after is None, all versions are considered and operations yielded.

    Each data change will always result in a new version in the index, however, options
    changes on the same data can result in no index change if the options don't impact
    the data in question (e.g. a geo hint change but the data has no geo data).

    The bulk ops are yielded in reverse version order for each record with the op on the
    latest index coming first and then the other index's ops following.

    :param indices: an IndexNames object for the database
    :param arc_status: the current arc status
    :param records: the records to update from
    :param after: the exclusive start version to produce index operations from, None if
                  all versions should be indexed
    :param all_options: dict of versions to ParsingOptions objects, this should be all
                        parsing option versions, not just the ones that apply after the
                        after parameter (if it's even provided)
    :return: yields BulkOp objects
    """
    # initialise these based off the ArcStatus we are provided
    arc_index, arc_count = arc_status
    # pre-sort the options in reverse version order
    sorted_options = [
        (option_version, all_options[option_version])
        for option_version in sorted(all_options, reverse=True)
    ]
    # cache the latest option version
    latest_option_version = max(all_options)
    # and cache the latest index name
    latest_index = indices.latest
    # if after is not provided, using -inf ensures that all versions will be yielded
    if after is None:
        after = -math.inf

    for record in records:
        if record.version <= after and latest_option_version <= after:
            # nothing to do for this record
            continue

        # create an iter for the record data and the options, both of these go backwards
        data_iter = iter(record.iter())
        options_iter = iter(sorted_options)
        # these iters have to have at least one element so this is safe
        data_version, data = next(data_iter)
        options_version, options = next(options_iter)
        version = max(data_version, options_version)
        next_version = None
        last_parsed_data = None

        while True:
            if not data:
                last_parsed_data = None
                # this is a delete! If this is the latest version then we delete the
                # record's document in the latest index, otherwise do nothing
                if next_version is None:
                    yield DeleteOp(latest_index, record.id)
            else:
                parsed_data = parse(data, options)
                # only yield an op if there is a change. Every data version should
                # trigger an op to be yielded, but options versions can result in the
                # same parsed data if the underlying data was the same between the
                # versions and the options change didn't impact any of fields present in
                # the data (e.g. changing a float string format when there are no
                # floats)
                if parsed_data != last_parsed_data:
                    last_parsed_data = parsed_data
                    document = {
                        DocumentField.ID: record.id,
                        DocumentField.VERSION: version,
                        DocumentField.VERSIONS: {"gte": version},
                        DocumentField.DATA: parsed_data.parsed,
                        DocumentField.DATA_TYPES: parsed_data.data_types,
                        DocumentField.PARSED_TYPES: parsed_data.parsed_types,
                    }
                    if next_version is None:
                        index_name = latest_index
                        doc_id = record.id
                    else:
                        # set the doc's ID to None to force Elasticsearch to create it,
                        # this is an ingestion speed optimisation
                        doc_id = None

                        # add some stuff to the document
                        document[DocumentField.NEXT] = next_version
                        document[DocumentField.VERSIONS]["lt"] = next_version

                        # figure out which arc we need to put this document in
                        if arc_count >= MAX_DOCS_PER_ARC:
                            arc_index += 1
                            arc_count = 0
                        index_name = indices.get_arc(arc_index)
                        arc_count += 1

                    yield IndexOp(index_name, doc_id, document)

            # update state variables
            if version == data_version:
                next_data_item = next(data_iter, None)
                if next_data_item is None:
                    # there's no more data left, break the loop
                    break
                else:
                    data_version, data = next_data_item
            if version == options_version:
                # because you have to have an option version at <= the first data
                # version, this is ok
                options_version, options = next(
                    options_iter, (options_version, options)
                )
            next_version = version
            version = max(data_version, options_version)

            # we've run out of data/options
            if version == next_version:
                break
            # this looks a bit weird, but it's a sneaky way to ensure we correctly
            # update the latest doc as well as then shunting the old latest doc into the
            # other data indices. It's the same as checking if version <= after and then
            # doing one more loop
            if next_version <= after:
                break
