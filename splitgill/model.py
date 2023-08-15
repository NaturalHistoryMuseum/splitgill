from dataclasses import dataclass, field, asdict
from typing import Dict, Iterable, NamedTuple, List, Optional
from uuid import uuid4

from bson import ObjectId

from splitgill.diffing import patch, DiffOp, prepare


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
