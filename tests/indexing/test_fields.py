import pytest

from splitgill.indexing.fields import (
    ParsedType,
    parsed_path,
    is_field_valid,
    DocumentField,
    FieldInfo,
    DataType,
)


def test_is_field_valid():
    assert is_field_valid("egg")
    assert is_field_valid("_egg")
    assert not is_field_valid("")
    assert not is_field_valid("egg^beans")
    assert not is_field_valid("egg.beans")


@pytest.mark.parametrize("parsed_type", ParsedType)
def test_parsed_path(parsed_type: ParsedType):
    field = "a.field.in.the.record"

    full = f"{DocumentField.PARSED}.{field}.{parsed_type}"
    relative = f"{field}.{parsed_type}"

    assert parsed_path(field, parsed_type=parsed_type, full=True) == full
    assert parsed_type.path_to(field, full=True) == full

    assert parsed_path(field, parsed_type=parsed_type, full=False) == relative
    assert parsed_type.path_to(field, full=False) == relative


def test_parse_path_no_parsed_type():
    assert parsed_path("a.field.in.the.record", None, False) == "a.field.in.the.record"


class TestFieldInfo:
    def test_get_data_field_children(self):
        fields = FieldInfo()
        fields.add_data_type(f"a.{DataType.DICT}", 4)
        fields.add_data_type(f"a.b.{DataType.INT}", 2)
        fields.add_data_type(f"a.c.{DataType.DICT}", 1)
        fields.add_data_type(f"a.c.d.{DataType.BOOL}", 10)

        a = fields.get_data_field("a")
        b = fields.get_data_field("a.b")
        c = fields.get_data_field("a.c")
        d = fields.get_data_field("a.c.d")

        assert fields.get_data_field_children() == [a]
        assert fields.get_data_field_children(a) == [b, c]
        assert fields.get_data_field_children(b) == []
        assert fields.get_data_field_children(c) == [d]
        assert fields.get_data_field_children(d) == []
