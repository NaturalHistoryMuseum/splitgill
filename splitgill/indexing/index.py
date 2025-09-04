import abc
import math
from dataclasses import dataclass
from itertools import chain
from typing import Any, Dict, Iterable, NamedTuple, Optional

from orjson import OPT_NON_STR_KEYS, dumps

from splitgill.indexing.fields import DocumentField
from splitgill.indexing.parser import ParsedData, parse
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


@dataclass
class RecordVersion:
    """
    A version of a record.

    The version is valid until either the next version replaces it (referenced by the
    next property) or it is deleted (represented by the deleted_at) property.
    """

    record_id: str
    version: int
    parsed: ParsedData
    # pointer to the next RecordVersion
    next: Optional["RecordVersion"] = None
    # if this version has been deleted, this is set with the version it was deleted at
    deleted_at: Optional[int] = None

    @property
    def version_end(self) -> Optional[int]:
        """
        Property which returns the version where this version becomes invalid. This
        could be either the version it was deleted at, the version of the next version,
        or if neither of these are set, None, implying there is no end to this version
        and that this is the latest data for the record.

        :return: a version or None
        """
        if self.deleted_at is not None:
            return self.deleted_at
        if self.next is not None:
            return self.next.version
        return None

    def create_doc(self) -> dict:
        """
        Creates the document to be indexed in Elasticsearch for this version and returns
        it.

        :return: a dict document
        """
        doc = {
            DocumentField.ID: self.record_id,
            DocumentField.VERSION: self.version,
            DocumentField.VERSIONS: {"gte": self.version},
            DocumentField.DATA: self.parsed.parsed,
            DocumentField.DATA_TYPES: self.parsed.data_types,
            DocumentField.PARSED_TYPES: self.parsed.parsed_types,
        }
        if self.version_end is not None:
            doc[DocumentField.NEXT] = self.version_end
            doc[DocumentField.VERSIONS]["lt"] = self.version_end
        return doc

    def __eq__(self, other: Any) -> bool:
        """
        Determines if two record versions are equal. Two versions are deemed equal if
        they have the same parsed data and neither version has been deleted.

        :param other: any object
        :return: True if the two record versions are equal, False if not, and returns
            NotImplemented if the other parameter is not a RecordVersion
        """
        if isinstance(other, RecordVersion):
            return (
                self.deleted_at is None
                and other.deleted_at is None
                and other.parsed == self.parsed
            )
        return NotImplemented


@dataclass
class RecordVersions:
    """
    A linked list representing all the versions of the record that should be included in
    Elasticsearch.
    """

    head: RecordVersion
    last: RecordVersion

    @classmethod
    def build(
        cls, record: MongoRecord, all_options: Dict[int, ParsingOptions]
    ) -> "RecordVersions":
        """
        Build a new RecordVersion object using the data in the given record and all the
        available options.

        :param record: the record
        :param all_options: all available options with versions as keys
        :return: a new RecordVersion object
        """
        all_data = dict(record.iter())
        first_data_version = min(all_data)
        versions = sorted(set(chain(all_options, all_data)))

        # init the data and options with the first versions
        data = all_data[min(all_data)]
        options = all_options[min(all_options)]

        record_versions = None
        for version in versions:
            # if the first options version is before the first data version we should
            # ignore it - if there's no data then what are the options going to act on?
            if version < first_data_version:
                continue

            data = all_data.get(version, data)
            options = all_options.get(version, options)
            if not data:
                # only set the deleted_at if the version isn't already deleted
                if record_versions.last.deleted_at is None:
                    record_versions.last.deleted_at = version
                continue

            doc = RecordVersion(record.id, version, parse(data, options))

            if not record_versions:
                record_versions = RecordVersions(doc, doc)
            elif doc != record_versions.last:
                record_versions.last.next = doc
                record_versions.last = doc

        return record_versions

    def __iter__(self) -> Iterable[RecordVersion]:
        doc = self.head
        while True:
            yield doc
            if doc.next:
                doc = doc.next
            else:
                break


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

    The bulk ops are yielded in ascending version order for each record.

    :param indices: an IndexNames object for the database
    :param arc_status: the current arc status
    :param records: the records to update from
    :param after: the exclusive start version to produce index operations from, None if
        all versions should be indexed
    :param all_options: dict of versions to ParsingOptions objects, this should be all
        parsing option versions, not just the ones that apply after the after parameter
        (if it's even provided)
    :return: yields BulkOp objects
    """
    arc_index, arc_count = arc_status
    latest_index = indices.latest
    # if after is not provided, using -inf ensures that all versions will be yielded
    if after is None:
        after = -math.inf

    for record in records:
        record_versions = RecordVersions.build(record, all_options)

        for rv in record_versions:
            # this is the latest version, just yield the appropriate op and carry on
            if rv.version > after and rv.next is None and rv.deleted_at is None:
                yield IndexOp(latest_index, record.id, rv.create_doc())
                continue

            if rv.version <= after:
                if rv.version_end is not None and rv.version_end > after:
                    # getting here means this version of the record is already in
                    # Elasticsearch and was the previous latest version at last sync
                    if rv.next is None:
                        yield DeleteOp(latest_index, record.id)
                else:
                    continue

            # figure out which arc we need to put this document in
            if arc_count >= MAX_DOCS_PER_ARC:
                arc_index += 1
                arc_count = 0
            arc_count += 1
            yield IndexOp(indices.get_arc(arc_index), None, rv.create_doc())
