from dataclasses import dataclass, field, asdict, astuple
from functools import cached_property
from typing import Dict, Iterable, NamedTuple, List, Optional, FrozenSet
from uuid import uuid4

from bson import ObjectId

from splitgill.diffing import patch, DiffOp, prepare
from splitgill.indexing.fields import geo_path


@dataclass
class Record:
    """
    A record before it becomes managed by Splitgill.
    """

    id: str
    data: dict

    @staticmethod
    def new(data: dict) -> "Record":
        return Record(str(uuid4()), data)


VersionedData = NamedTuple("VersionedData", version=int, data=dict)


@dataclass
class MongoRecord:
    """
    A record retrieved from MongoDB.
    """

    _id: ObjectId
    id: str
    version: int
    data: dict
    # you'd expect the keys to be ints but MongoDB doesn't allow non-string keys
    diffs: Dict[str, List[DiffOp]] = field(default_factory=dict)

    def __post_init__(self):
        # to be safe, we need to prepare the data we have stored in MongoDB so that any
        # patching or diffing that uses it can reliably use it
        self.data = prepare(self.data)

    @property
    def is_deleted(self) -> bool:
        """
        A record is deleted if its current data is an empty dict.

        :return: True if this record has been deleted, False if not
        """
        return not bool(self.data)

    @property
    def versions(self) -> List[int]:
        """
        Returns a list of the record's versions in descending order.

        :return: the record's versions
        """
        versions = sorted(map(int, self.diffs.keys()), reverse=True)
        versions.insert(0, self.version)
        return versions

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
        for version_str, diff in sorted(
            self.diffs.items(), key=lambda item: int(item[0]), reverse=True
        ):
            data = patch(base, diff)
            # convert the string versions to ints on the way out the door
            yield VersionedData(int(version_str), data)
            base = data


@dataclass
class Status:
    name: str
    version: int
    _id: Optional[ObjectId] = None

    def to_doc(self) -> dict:
        """
        Converts this object to a dict ready for storage in MongoDB. If this object
        hasn't been stored in the MongoDB yet, then the _id field is not included, if it
        has, then the _id is included.

        :return: a dict
        """
        doc = asdict(self)
        # if the _id is None then this status object hasn't been saved, remove it from
        # the dict to avoid attempting to write a doc with _id=None which MongoDB will
        # allow
        if self._id is None:
            del doc["_id"]
        return doc


@dataclass
class GeoFieldHint:
    lat_field: str
    lon_field: str
    radius_field: Optional[str] = None

    @cached_property
    def path(self) -> str:
        return geo_path(self.lat_field, self.lon_field, self.radius_field)

    def __hash__(self) -> int:
        return hash(self.path)


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

    def to_doc(self) -> dict:
        return {
            "true_values": list(self.true_values),
            "false_values": list(self.false_values),
            "date_formats": list(self.date_formats),
            "geo_hints": [astuple(hint) for hint in self.geo_hints],
        }

    @classmethod
    def from_doc(cls, doc: dict) -> "ParsingOptions":
        return ParsingOptions(
            frozenset(doc["true_values"]),
            frozenset(doc["false_values"]),
            frozenset(doc["date_formats"]),
            frozenset(GeoFieldHint(*params) for params in doc["geo_hints"]),
        )
