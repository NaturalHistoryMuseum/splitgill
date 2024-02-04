from datetime import datetime
from typing import List

from freezegun import freeze_time

from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.manager import SplitgillDatabase, SplitgillClient
from splitgill.model import Record
from splitgill.profiles import Field, Profile
from splitgill.utils import to_timestamp


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
        database.commit()
    # sync the index, this will also update the profile
    database.sync(parallel=False)
    # return the new version
    return to_timestamp(version)


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
            fields={
                "a": Field("a", "a", count=1),
                "b": Field("b", "b", count=1, number_count=1),
                "c": Field("c", "c", count=1, boolean_count=1),
                "d": Field("d", "d", count=1, date_count=1),
            },
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
            fields={
                "a": Field(
                    "a", "a", count=4, boolean_count=1, date_count=1, number_count=1
                ),
            },
        )

    def test_profile_single_type_arrays(self, splitgill: SplitgillClient):
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
            fields={
                "a": Field("a", "a", count=1, array_count=1),
                "b": Field("b", "b", count=1, number_count=1, array_count=1),
                "c": Field("c", "c", count=1, boolean_count=1, array_count=1),
                # note that this is still counted as an array even though it's only got
                # one value (the parser doesn't unpack it basically)
                "d": Field("d", "d", count=1, date_count=1, array_count=1),
            },
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
            fields={
                "a": Field(
                    "a",
                    "a",
                    count=2,
                    boolean_count=2,
                    date_count=2,
                    number_count=2,
                    array_count=2,
                ),
                "b": Field(
                    "b",
                    "b",
                    count=1,
                    boolean_count=1,
                    date_count=1,
                    number_count=1,
                    array_count=1,
                ),
            },
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
            field_count=5,
            fields={
                "_id": Field(
                    "_id",
                    "_id",
                    count=2,
                    number_count=2,
                ),
                "associatedMedia._id": Field(
                    "_id",
                    "associatedMedia._id",
                    count=2,
                    number_count=2,
                ),
                "associatedMedia.assetID": Field(
                    "assetID",
                    "associatedMedia.assetID",
                    count=2,
                ),
                "associatedMedia.category": Field(
                    "category",
                    "associatedMedia.category",
                    count=1,
                ),
                "associatedMedia.created": Field(
                    "created",
                    "associatedMedia.created",
                    count=2,
                    number_count=2,
                ),
            },
        )


# TODO: write a test which fails on nested arrays
