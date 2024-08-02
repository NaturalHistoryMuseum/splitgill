import dataclasses
from enum import auto
from typing import Optional, Dict, List, Iterable, TypeVar, Generic

from strenum import LowercaseStrEnum, StrEnum


def is_field_valid(name: str) -> bool:
    """
    Defines a valid field in user provided data.

    :param name: the field name
    :return: True if the field is valid, False otherwise
    """
    # todo: should we disallow fields that have a __ in them for elasticsearch-dsl?
    # ^ is used in parsed type field names, and we use . in the parsed and data type
    # fields. Additionally, using dots in names is probably going to result in confusing
    # outcomes for users given elasticsearch will interpret them as flattened objects
    # and expand them out
    return name and "^" not in name and "." not in name


class DocumentField(LowercaseStrEnum):
    """
    Enum representing the fields used in the indexed documents.
    """

    # the record ID
    ID = auto()
    # the version of this record
    VERSION = auto()
    # the next version of this record (if available)
    NEXT = auto()
    # the range of versions this record is valid for. The lower bound is the same value
    # as the version field and the upper bound is the same value as the next field
    VERSIONS = auto()
    # the record's data, not indexed
    DATA = auto()
    # the parsed version of the data for searching
    PARSED = auto()
    # type information about the fields found in the data
    DATA_TYPES = auto()
    # type information about the fields found in the parsed data
    PARSED_TYPES = auto()
    # a text field into which all data is added to support "search everything" searches
    ALL_TEXT = auto()
    # ALL_POINTS and ALL_SHAPES are geo fields into which all geo data is added to
    # support "search everything" geo searches. ALL_SHAPES gets all data from GEO_SHAPE
    # parsed fields and ALL_POINTS gets all data from GEO_POINT parsed fields. If you're
    # doing a search, you probably want to use the ALL_SHAPES field but if you're
    # mapping the results, you'll need to aggregate on the ALL_POINTS value as you
    # aren't allowed to use geo aggregations on the geo shape data type unless you used
    # a paid Elasticsearch version (yes this is annoying).
    ALL_POINTS = auto()
    ALL_SHAPES = auto()


class ParsedType(StrEnum):
    """
    Enum representing the possible parsed data types a value can be indexed as.

    It's generally recommended to not use these directly, but to use the convenience
    functions defined later in this module or in the search module.
    """

    # the number field
    NUMBER = "^n"
    # the date field
    DATE = "^d"
    # the boolean field
    BOOLEAN = "^b"
    # the text field
    TEXT = "^t"
    # the keyword case-insensitive field
    KEYWORD_CASE_INSENSITIVE = "^ki"
    # the keyword case-sensitive field
    KEYWORD_CASE_SENSITIVE = "^ks"
    # the geo point field (shape centroid, will always be a point)
    GEO_POINT = "^gp"
    # the geo shape field (full shape, could be point, linestring, or polygon)
    GEO_SHAPE = "^gs"

    def path_to(self, field: str, full: bool = True) -> str:
        """
        Creates and returns the parsed path to the field indexed with this type.

        :param field: the name (including dots if needed) of the field
        :param full: whether to prepend the parsed field name to the path or not
                     (default: True)
        :return: the path
        """
        return parsed_path(field, self, full)


def parsed_path(
    field: str, parsed_type: Optional[ParsedType] = None, full: bool = True
) -> str:
    """
    Creates and returns the parsed path to the field indexed with the given parsed type.
    Optionally, the full path is created and therefore the result includes the "parsed"
    prefix. If no parsed_type is provided (i.e. parsed_type=None, the default), then the
    root path to the field in the parsed object is returned.

    :param field: the name (including dots if needed) of the field
    :param parsed_type: the parsed type (default: None)
    :param full: whether to prepend the parsed field name to the path or not (default:
                 True)
    :return: the path
    """
    if parsed_type is not None:
        path = f"{field}.{parsed_type}"
    else:
        path = field

    if full:
        return f"{DocumentField.PARSED}.{path}"
    else:
        return path


class DataType(LowercaseStrEnum):
    """
    Enum representing the types of data Splitgill indexes as user data.

    This should match the output of diffing.prepare_data.
    """

    NULL = "nonetype"
    STR = auto()
    INT = auto()
    FLOAT = auto()
    BOOL = auto()
    LIST = auto()
    DICT = auto()


# type hint for Field
T = TypeVar("T")


@dataclasses.dataclass
class Field(Generic[T]):
    """
    Base class for information about fields.

    This class is designed to
    """

    path: str
    counts: Dict[T, int] = dataclasses.field(default_factory=dict)

    def add_type(self, field_type: T, count: int):
        """
        Adds the given type to this field's data with the given count.

        :param field_type: the type
        :param count: the number of records which have this field with values of the
                      given type
        """
        self.counts[field_type] = count

    def is_types(self, *types: T) -> bool:
        """
        Checks if this field has any records with the given types.

        :param types: the types to check
        :return: True if at least one of the passed types has a record count >0 , False
                 if not
        """
        return any(self.count(t) > 0 for t in types)

    def count(self, field_type: T) -> int:
        """
        Returns the number of records which have this field as this type. If the type is
        not represented in this field, 0 is returned.

        :param field_type: the type
        :return: an integer >= 0
        """
        return self.counts.get(field_type, 0)

    @property
    def depth(self) -> int:
        """
        Returns the depth of the field in the record structure. Fields at the root of
        the record will have a depth of 0. As an example, the field "a.b.c" has depth 2.

        :return: the depth of the field in the record
        """
        return self.path.count(".")

    @property
    def name(self) -> str:
        """
        The name of the field.

        :return: the name
        """
        return self.path.rsplit(".", 1)[-1]

    @property
    def types(self) -> List[T]:
        """
        Returns a list of types this field is represented as.

        :return: a list of types
        """
        return list(self.counts.keys())


class ParsedField(Field[ParsedType]):
    """
    Class representing fields in the parsed and therefore searchable data.
    """

    pass


class DataField(Field[DataType]):
    """
    Class representing fields in the source data and therefore not searchable.
    """

    @property
    def is_list_member(self) -> bool:
        """
        Checks if this field is a direct member of a list.

        :return: True or False
        """
        return "" in self.path.split(".")


class FieldInfo:
    """
    Class representing the information about the available fields.

    This class contains information about both the parsed fields and the source data
    fields.
    """

    def __init__(self):
        self.data_fields: Dict[str, DataField] = {}
        self.parsed_fields: Dict[str, ParsedField] = {}

    def add_data_type(self, full_path: str, count: int):
        """
        Add a data type and its record count to this object.

        :param full_path: the full path including the field and the data type
        :param count: the number of records which contain this field/data type combo
        """
        path, data_type = full_path.rsplit(".", 1)
        if path not in self.data_fields:
            self.data_fields[path] = DataField(path)
        self.data_fields[path].add_type(DataType(data_type), count)

    def add_parsed_type(self, full_path: str, count: int):
        """
        Add a parsed type and its record count to this object.

        :param full_path: the full path including the field and the parsed type
        :param count: the number of records which contain this field/parsed type combo
        """
        path, parsed_type = full_path.rsplit(".", 1)
        if path not in self.parsed_fields:
            self.parsed_fields[path] = ParsedField(path)
        self.parsed_fields[path].add_type(ParsedType(parsed_type), count)

    def iter_data_fields(self) -> Iterable[DataField]:
        """
        Yields all the data fields in this object.

        :return: yields DataField objects
        """
        yield from self.data_fields.values()

    def iter_parsed_fields(self) -> Iterable[ParsedField]:
        """
        Yields all the parsed fields in this object.

        :return: yields ParsedField objects
        """
        yield from self.parsed_fields.values()

    def get_data_field(self, path: str) -> DataField:
        """
        Retrieves the DataField with the given path. If the path is not found, returns
        None.

        :return: None or a DataField object
        """
        return self.data_fields.get(path)

    def get_parsed_field(self, path: str) -> ParsedField:
        """
        Retrieves the ParsedField with the given path. If the path is not found, returns
        None.

        :return: None or a ParsedField object
        """
        return self.parsed_fields.get(path)

    def get_data_field_children(
        self, parent: Optional[DataField] = None
    ) -> List[DataField]:
        """
        Retrieves the children of the given DataField. If no parent is given, returns
        all root fields.

        :param parent: the parent DataField or None to get the root fields
        :return: a list of DataField objects
        """
        if parent is None:
            # return the root fields
            return [field for field in self.data_fields.values() if field.depth == 0]
        else:
            # sanity check
            if not parent.is_types(DataType.LIST, DataType.DICT):
                return []
            # find the kids
            return [
                field
                for field in self.data_fields.values()
                if field.depth == parent.depth + 1
                and field.path.startswith(f"{parent.path}.")
            ]
