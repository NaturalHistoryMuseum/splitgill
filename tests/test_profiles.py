from datetime import datetime
from typing import List, Optional

from freezegun import freeze_time

from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.manager import SplitgillDatabase, SplitgillClient
from splitgill.model import Record
from splitgill.profiles import Field, Profile


def add_data(
    database: SplitgillDatabase, version: datetime, records: List[Record]
) -> int:
    """
    Utility function to add the given records to the given database at the given version
    with default options. The records are added and indexed.

    :param database: the database to add the records to
    :param version: the version to add them at (uses freeze_time)
    :param records: the records to add
    :return: the version the records were added at as a timestamp
    """
    database.add(records, commit=False)
    database.update_options(
        # just use this date format to keep things under control in terms of what gets
        # parsed as a date and what doesn't
        ParsingOptionsBuilder().with_date_format("%Y-%m-%dT%H:%M:%S.%f").build(),
        commit=False,
    )
    # commit the records and options at a specific version
    with freeze_time(version):
        version = database.commit()
    # sync the index, this will also update the profile
    database.sync(parallel=False)
    # return the new version
    return version


class TestBuildProfile:
    def test_profile_singles(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": "a string of data"}),
            Record.new({"b": 50}),
            Record.new({"c": True}),
            Record.new({"d": datetime.now()}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=4,
            fields=[
                Field("a", "a", count=1),
                Field("b", "b", count=1, number_count=1),
                Field("c", "c", count=1, boolean_count=1),
                Field("d", "d", count=1, date_count=1),
            ],
        )

    def test_profile_one_mixed(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": "a string of data"}),
            Record.new({"a": 50}),
            Record.new({"a": True}),
            Record.new({"a": datetime.now()}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=1,
            fields=[
                Field("a", "a", count=4, boolean_count=1, date_count=1, number_count=1),
            ],
        )

    def test_profile_single_type_lists(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": ["a string of data", "another!", "and another?"]}),
            Record.new({"b": [50, 4.1, 10000004, 0, -109.2]}),
            Record.new({"c": [True, False, True]}),
            Record.new({"d": [datetime.now()]}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=4,
            fields=[
                Field("a", "a", count=1, lists_count=1),
                Field("b", "b", count=1, number_count=1, lists_count=1),
                Field("c", "c", count=1, boolean_count=1, lists_count=1),
                # note that this is still counted as a list even though it's only got
                # one value (the parser doesn't unpack it basically)
                Field("d", "d", count=1, date_count=1, lists_count=1),
            ],
        )

    def test_profile_mixed(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": ["beans", False, 4.2, datetime.now()]}),
            Record.new({"a": [True, 4, False, "large", datetime.now()]}),
            Record.new({"b": ["bark", "woof", "cat", False, datetime.now(), 4.883]}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=2,
            fields=[
                Field(
                    "a",
                    "a",
                    count=2,
                    boolean_count=2,
                    date_count=2,
                    number_count=2,
                    lists_count=2,
                ),
                Field(
                    "b",
                    "b",
                    count=1,
                    boolean_count=1,
                    date_count=1,
                    number_count=1,
                    lists_count=1,
                ),
            ],
        )

    def test_profile_nesting(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new(
                {
                    "_id": 5574287,
                    "associatedMedia": [
                        {
                            "_id": 2229038,
                            "assetID": "8239fe05-58ae-44cd-8456-b94a4aae1cfe",
                            "category": "Specimen",
                            "created": 1512750787000,
                        },
                        {
                            "_id": 2236914,
                            "assetID": "c0e5a5f8-de39-4261-a694-73370ac4d4ce",
                            "category": "Specimen",
                            "created": 1513242810000,
                        },
                    ],
                }
            ),
            Record.new(
                {
                    "_id": 9275977,
                    "associatedMedia": [
                        {
                            "_id": 3274539,
                            "assetID": "794cfc45-6418-467f-8765-35270d32f0d7",
                            "created": 1615565247000,
                        }
                    ],
                }
            ),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=6,
            fields=[
                Field(
                    "_id",
                    "_id",
                    count=2,
                    number_count=2,
                ),
                Field(
                    "associatedMedia",
                    "associatedMedia",
                    count=2,
                    lists_count=2,
                    is_value=False,
                    is_parent=True,
                ),
                Field(
                    "_id",
                    "associatedMedia._id",
                    count=2,
                    number_count=2,
                ),
                Field(
                    "assetID",
                    "associatedMedia.assetID",
                    count=2,
                ),
                Field(
                    "category",
                    "associatedMedia.category",
                    count=1,
                ),
                Field(
                    "created",
                    "associatedMedia.created",
                    count=2,
                    number_count=2,
                ),
            ],
        )

    def test_parent_override(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": {"x": "arms"}}),
            Record.new({"a": {"x": "legs"}}),
            Record.new({"a": 7}),
            Record.new({"a": 3}),
            Record.new({"a": 9}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=2,
            fields=[
                Field(
                    name="a",
                    path="a",
                    is_value=True,
                    is_parent=True,
                    count=5,
                    number_count=3,
                ),
                Field(name="x", path="a.x", is_value=True, is_parent=False, count=2),
            ],
        )

    def test_parent_with_lists(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [{"x": "arms"}, {"x": "legs"}]}),
            Record.new({"a": [{"x": "ears"}]}),
            Record.new({"b": "beans"}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=3,
            fields=[
                Field(
                    name="a",
                    path="a",
                    is_value=False,
                    is_parent=True,
                    count=2,
                    lists_count=2,
                ),
                Field(name="x", path="a.x", is_value=True, is_parent=False, count=2),
                Field(name="b", path="b", is_value=True, is_parent=False, count=1),
            ],
        )

    def test_multiple_versions(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record("1", {"a": "a string of data"}),
            Record("2", {"b": 50}),
            Record("3", {"c": True}),
            Record("4", {"d": datetime.now()}),
        ]
        version_1 = add_data(database, datetime(2020, 1, 2), records)

        records = [
            Record("1", {"a": "another string of data"}),
            Record("2", {"b": 30}),
            Record("5", {"c": False}),
        ]
        version_2 = add_data(database, datetime(2020, 1, 5), records)

        assert version_1 != version_2

    def test_nested_lists(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]}),
            Record.new({"a": [[1, 2, 3], [4, 1, 6], [7, 8, 9]]}),
            Record.new({"a": [[1, 7, 3], [4, 5, 6], [7, 8, 9]]}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=1,
            fields=[
                Field(
                    name="a",
                    path="a",
                    is_value=True,
                    is_parent=False,
                    number_count=3,
                    count=3,
                    lists_count=3,
                ),
            ],
        )

    def test_list_in_dict(self, splitgill: SplitgillClient):
        database = SplitgillDatabase("test", splitgill)
        records = [
            Record.new({"a": {"b": [1, 2, 3]}}),
            Record.new({"a": {"c": [1, 2, 3]}}),
            Record.new({"a": {"b": {"c": [1, 2, 3]}}}),
        ]
        version = add_data(database, datetime(2020, 7, 2), records)

        profile = database.get_profile(version)
        assert profile == Profile(
            name="test",
            version=version,
            total=len(records),
            changes=len(records),
            field_count=4,
            fields=[
                Field(
                    name="a",
                    path="a",
                    is_value=False,
                    is_parent=True,
                    count=3,
                ),
                Field(
                    name="b",
                    path="a.b",
                    is_value=True,
                    is_parent=True,
                    number_count=1,
                    count=2,
                    lists_count=1,
                ),
                Field(
                    name="c",
                    path="a.b.c",
                    is_value=True,
                    is_parent=False,
                    number_count=1,
                    count=1,
                    lists_count=1,
                ),
                Field(
                    name="c",
                    path="a.c",
                    is_value=True,
                    is_parent=False,
                    number_count=1,
                    count=1,
                    lists_count=1,
                ),
            ],
        )


class TestProfile:
    def test_field_helpers(self):
        parent_field = Field("parent", "parent", 1, is_parent=True, is_value=False)
        value_field = Field("value", "parent.value", 1, is_parent=False, is_value=True)
        both_field = Field("both", "both", 1, is_parent=True, is_value=True)
        profile = Profile(
            name="test",
            version=1,
            total=1,
            changes=1,
            field_count=1,
            fields=[both_field, parent_field, value_field],
        )

        assert profile.get_parents(exclusive=False) == {
            "parent": parent_field,
            "both": both_field,
        }
        assert profile.get_parents(exclusive=True) == {"parent": parent_field}

        assert profile.get_values(exclusive=False) == {
            "parent.value": value_field,
            "both": both_field,
        }
        assert profile.get_values(exclusive=True) == {"parent.value": value_field}

        assert profile.get_fields() == {field.path: field for field in profile.fields}
