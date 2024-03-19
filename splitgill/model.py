from dataclasses import dataclass, field, astuple
from functools import cached_property
from itertools import chain
from typing import Dict, Iterable, NamedTuple, List, Optional, FrozenSet
from uuid import uuid4

from bson import ObjectId
from pymongo.results import BulkWriteResult

from splitgill.diffing import patch, DiffOp
from splitgill.indexing import fields


@dataclass
class Record:
    """
    A record before it becomes managed by Splitgill.
    """

    id: str
    data: dict

    @property
    def is_delete(self) -> bool:
        """
        Returns True if this record is a delete request, otherwise False. A delete
        request is a record with empty data ({}).

        :return: True if this is a delete, False if not
        """
        return not self.data

    @staticmethod
    def new(data: dict) -> "Record":
        return Record(str(uuid4()), data)

    @staticmethod
    def delete(record_id: str) -> "Record":
        return Record(record_id, {})


VersionedData = NamedTuple("VersionedData", version=Optional[int], data=dict)


@dataclass
class MongoRecord:
    """
    A record retrieved from MongoDB.
    """

    _id: ObjectId
    id: str
    version: Optional[int]
    data: dict
    # you'd expect the keys to be ints but MongoDB doesn't allow non-string keys
    diffs: Dict[str, List[DiffOp]] = field(default_factory=dict)

    @property
    def is_deleted(self) -> bool:
        """
        A record is deleted if its current data is an empty dict.

        :return: True if this record has been deleted, False if not
        """
        return not self.data

    @property
    def is_uncommitted(self) -> bool:
        """
        A record is uncommitted if its current version is None.

        :return: True if this record has been deleted, False if not
        """
        return self.version is None

    @property
    def has_history(self) -> bool:
        """
        A record has history if it has any diffs.

        :return: True if this record has previous versions, False if not
        """
        return bool(self.diffs)

    def get_versions(self, desc=False) -> List[int]:
        """
        Returns a list of the record's versions in ascending order. If desc is True, the
        versions are returned in descending order. If the current version is None, it is
        not included.

        :return: the record's versions
        """
        versions = map(int, self.diffs)
        if self.version is not None:
            versions = chain(versions, (self.version,))
        return sorted(versions, reverse=desc)

    def iter(self) -> Iterable[VersionedData]:
        """
        Yields the versions and data of this record. These are yielded as (int, dict)
        VersionedData named tuples. The tuples are yielded in reverse order, starting
        with the latest data and working back to the first version.

        :return: VersionedData (version: int, data: dict) named tuples in descending
                 version order
        """
        yield VersionedData(self.version, self.data)
        base = self.data
        for version in sorted(map(int, self.diffs), reverse=True):
            data = patch(base, self.diffs[str(version)])
            # convert the string versions to ints on the way out the door
            yield VersionedData(version, data)
            base = data


# use frozen to get a free hash method and as these objects have no reason to be mutable
@dataclass(frozen=True)
class GeoFieldHint:
    """
    Class holding the fields representing the fields in a record which describe its
    latitude/longitude location and an optional uncertainty radius.
    """

    lat_field: str
    lon_field: str
    radius_field: Optional[str] = None

    @cached_property
    def path(self) -> str:
        return fields.geo_make_name(self.lat_field, self.lon_field, self.radius_field)


# set frozen=True to make the objects immutable and provide hashing (which we need for
# parser.parse_str's lru_cache)
@dataclass(frozen=True)
class ParsingOptions:
    """
    Holds options for parsing.

    The objects created using this class are immutable. You can instantiate them
    directly, but it's better to use The ParsingOptionBuilder defined below.
    """

    # lowercase string values which should be parsed as True
    true_values: FrozenSet[str]
    # lowercase string values which should be parsed as False
    false_values: FrozenSet[str]
    # date format strings to test candidates against using datetime.strptime
    date_formats: FrozenSet[str]
    # GeoFieldHint objects which can be used to test if a record contains any geographic
    # coordinate data
    geo_hints: FrozenSet[GeoFieldHint]
    # the maximum length of keyword strings (both case-sensitive and -insensitive).
    # Strings will be truncated to this length before indexing them in either keyword
    # field. Defaults to the maximum Elasticsearch allows.
    keyword_length: int = 2147483647
    # the format to use to convert a float to a string for indexing. The string will
    # have format() called on it with the float value passed as the only parameter,
    # therefore the format string should use 0 to reference it. The default format uses
    # 15 significant digits. This roughly matches how a float is actually stored in
    # elasticsearch and therefore gives a somewhat sensible representative idea to users
    # of what the number actually is and how it can be searched. This format will
    # produce string representations of numbers in scientific notation if it decides it
    # needs to.
    float_format: str = "{0:.15g}"

    def to_doc(self) -> dict:
        return {
            "true_values": list(self.true_values),
            "false_values": list(self.false_values),
            "date_formats": list(self.date_formats),
            "geo_hints": [astuple(hint) for hint in self.geo_hints],
            "keyword_length": self.keyword_length,
            "float_format": self.float_format,
        }

    @classmethod
    def from_doc(cls, doc: dict) -> "ParsingOptions":
        return ParsingOptions(
            frozenset(doc["true_values"]),
            frozenset(doc["false_values"]),
            frozenset(doc["date_formats"]),
            frozenset(GeoFieldHint(*params) for params in doc["geo_hints"]),
            doc["keyword_length"],
            doc["float_format"],
        )


@dataclass
class IngestResult:
    """
    A dataclass containing information about the new data ingested into MongoDB.
    """

    # the version the new data was added at (if the data was not committed or no new
    # data was added, then this will be None)
    version: Optional[int] = None
    # the number of insert operations performed
    inserted: int = 0
    # the number of update operations performed
    updated: int = 0
    # the number of delete operations performed
    deleted: int = 0

    @property
    def was_committed(self) -> bool:
        """
        Returns True if the data was committed, False if not. This is determined by
        whether a version is available.

        :return: True if the data was committed, False if not
        """
        return self.version is not None

    def update(self, bulk_result: BulkWriteResult):
        """
        Update the counts with the counts in the bulk result object.

        :param bulk_result: a BulkWriteResult object
        """
        self.inserted += bulk_result.inserted_count
        self.updated += bulk_result.upserted_count
        self.deleted += bulk_result.deleted_count
