from datetime import datetime
from itertools import chain

import pytest

from splitgill.diffing import prepare_data
from splitgill.indexing.fields import DataType
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.parser import parse_for_index, ParsedData, parse
from splitgill.utils import to_timestamp


class TestParseForIndex:
    def test_no_nesting(self):
        data = {"x": "beans"}
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(data, {"x": parse("beans", options)}, {}, {})

    def test_tuple_of_strings(self):
        data = {"x": ("beans", "lemons", "goats")}
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data, {"x": [parse(value, options) for value in data["x"]]}, {}, {"x": 3}
        )

    def test_list_of_string(self):
        data = {"x": ["beans", "lemons", "goats"]}
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data, {"x": [parse(value, options) for value in data["x"]]}, {}, {"x": 3}
        )

    def test_list_of_dicts(self):
        data = {"x": [{"a": 4}, {"a": 5}, {"a": 6}]}
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data=data,
            parsed={"x": [{"a": parse(n, options)} for n in range(4, 7)]},
            geo={},
            lists={"x": 3},
        )

    def test_nested_dict(self):
        data = {"x": "beans", "y": {"a": "5", "b": "buckets!"}}
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data,
            {
                "x": parse("beans", options),
                "y": {"a": parse("5", options), "b": parse("buckets!", options)},
            },
            {},
            {},
        )

    def test_nested_mix(self):
        data = {
            "x": ("4", "6", ({"a": ("1", "2")}, {"a": ("6", "1")})),
            "y": {"t": ({"x": "4"}, {"x": "1"}, {"x": "7"})},
        }
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data,
            {
                "x": [
                    parse("4", options),
                    parse("6", options),
                    [
                        {"a": [parse("1", options), parse("2", options)]},
                        {"a": [parse("6", options), parse("1", options)]},
                    ],
                ],
                "y": {
                    "t": [
                        {"x": parse("4", options)},
                        {"x": parse("1", options)},
                        {"x": parse("7", options)},
                    ]
                },
            },
            {},
            {
                "x": 3,
                "x.2": 2,
                "x.2.0.a": 2,
                "x.2.1.a": 2,
                "y.t": 3,
            },
        )

    def test_geojson_two_fields(self):
        data = {
            "x": "something",
            "y": "somewhere",
            "decimalLatitude": "14.897",
            "decimalLongitude": "-87.956",
        }
        options = ParsingOptionsBuilder().with_default_geo_hints().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data,
            {
                "x": parse("something", options),
                "y": parse("somewhere", options),
                "decimalLatitude": parse("14.897", options),
                "decimalLongitude": parse("-87.956", options),
            },
            {
                "compound.decimalLatitude/decimalLongitude.geojson": {
                    "type": "Point",
                    "coordinates": (-87.956, 14.897),
                }
            },
            {},
        )

    def test_geojson_field(self, geojson_point: dict):
        data = {
            "x": prepare_data(geojson_point),
            "y": "somewhere",
        }
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data,
            {
                "x": {
                    "type": parse("Point", options),
                    "coordinates": [parse(30.0, options), parse(10.0, options)],
                },
                "y": parse("somewhere", options),
            },
            {
                "single.x.geojson": geojson_point,
            },
            {"x.coordinates": 2},
        )

    def test_geojson_at_root_not_recognised(self, geojson_point: dict):
        data = prepare_data(geojson_point)
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed == ParsedData(
            data,
            {
                "type": parse("Point", options),
                "coordinates": [parse(30.0, options), parse(10.0, options)],
            },
            # geo must be empty
            {},
            {"coordinates": 2},
        )
        assert not parsed.geo

    def test_geojson_field_list(
        self,
        geojson_point: dict,
        geojson_polygon: dict,
        geojson_linestring: dict,
        geojson_holed_polygon: dict,
    ):
        data = {
            "x": prepare_data(
                (
                    geojson_point,
                    geojson_linestring,
                    geojson_polygon,
                    geojson_holed_polygon,
                )
            ),
        }
        options = ParsingOptionsBuilder().build()
        parsed = parse_for_index(data, options)
        assert parsed.lists["x"] == 4
        assert parsed.geo == {
            "single.x.0.geojson": geojson_point,
            "single.x.1.geojson": geojson_linestring,
            "single.x.2.geojson": geojson_polygon,
            "single.x.3.geojson": geojson_holed_polygon,
        }


class TestParse:
    def test_normal_text(self):
        options = ParsingOptionsBuilder().build()
        assert parse("banana", options) == {
            DataType.TEXT.value: "banana",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "banana",
            DataType.KEYWORD_CASE_SENSITIVE.value: "banana",
        }

    def test_bools(self):
        options = ParsingOptionsBuilder().with_default_boolean_values().build()

        for value in chain(
            options.true_values, [o.upper() for o in options.true_values]
        ):
            assert parse(value, options) == {
                DataType.TEXT.value: value,
                DataType.KEYWORD_CASE_INSENSITIVE.value: value,
                DataType.KEYWORD_CASE_SENSITIVE.value: value,
                DataType.BOOLEAN.value: True,
            }
        for value in chain(
            options.false_values, [o.upper() for o in options.false_values]
        ):
            assert parse(value, options) == {
                DataType.TEXT.value: value,
                DataType.KEYWORD_CASE_INSENSITIVE.value: value,
                DataType.KEYWORD_CASE_SENSITIVE.value: value,
                DataType.BOOLEAN.value: False,
            }

    def test_number(self):
        options = ParsingOptionsBuilder().build()
        assert parse("5.3", options) == {
            DataType.TEXT.value: "5.3",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "5.3",
            DataType.KEYWORD_CASE_SENSITIVE.value: "5.3",
            DataType.NUMBER.value: 5.3,
        }
        assert parse("70", options) == {
            DataType.TEXT.value: "70",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "70",
            DataType.KEYWORD_CASE_SENSITIVE.value: "70",
            DataType.NUMBER.value: 70.0,
        }
        assert parse("70.0", options) == {
            DataType.TEXT.value: "70.0",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "70.0",
            DataType.KEYWORD_CASE_SENSITIVE.value: "70.0",
            DataType.NUMBER.value: 70.0,
        }
        assert parse(4, options) == {
            DataType.TEXT.value: "4",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "4",
            DataType.KEYWORD_CASE_SENSITIVE.value: "4",
            DataType.NUMBER.value: 4,
        }
        assert parse(16.04, options) == {
            DataType.TEXT.value: "16.04",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "16.04",
            DataType.KEYWORD_CASE_SENSITIVE.value: "16.04",
            DataType.NUMBER.value: 16.04,
        }
        assert parse(16.042245342119813456, options) == {
            DataType.TEXT.value: "16.0422453421198",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "16.0422453421198",
            DataType.KEYWORD_CASE_SENSITIVE.value: "16.0422453421198",
            DataType.NUMBER.value: 16.042245342119813456,
        }
        assert parse("1.2312e-20", options) == {
            DataType.TEXT.value: "1.2312e-20",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "1.2312e-20",
            DataType.KEYWORD_CASE_SENSITIVE.value: "1.2312e-20",
            DataType.NUMBER.value: 1.2312e-20,
        }

    def test_invalid_numbers(self):
        options = ParsingOptionsBuilder().build()
        assert DataType.NUMBER.value not in parse("5.3.4", options)
        assert DataType.NUMBER.value not in parse("NaN", options)
        assert DataType.NUMBER.value not in parse("inf", options)

    def test_date_date_and_time(self):
        options = ParsingOptionsBuilder().with_default_date_formats().build()
        assert parse("2005-07-02 20:16:47.458301", options) == {
            DataType.TEXT.value: "2005-07-02 20:16:47.458301",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "2005-07-02 20:16:47.458301",
            DataType.KEYWORD_CASE_SENSITIVE.value: "2005-07-02 20:16:47.458301",
            DataType.DATE.value: to_timestamp(
                datetime.fromisoformat("2005-07-02T20:16:47.458301")
            ),
        }

    def test_date_date_and_time_and_tz(self):
        options = ParsingOptionsBuilder().with_default_date_formats().build()
        assert parse("2005-07-02 20:16:47.103+05:00", options) == {
            DataType.TEXT.value: "2005-07-02 20:16:47.103+05:00",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "2005-07-02 20:16:47.103+05:00",
            DataType.KEYWORD_CASE_SENSITIVE.value: "2005-07-02 20:16:47.103+05:00",
            DataType.DATE.value: to_timestamp(
                datetime.fromisoformat("2005-07-02T20:16:47.103000+05:00")
            ),
        }

    def test_date_just_a_date(self):
        options = ParsingOptionsBuilder().with_default_date_formats().build()
        assert parse("2005-07-02", options) == {
            DataType.TEXT.value: "2005-07-02",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "2005-07-02",
            DataType.KEYWORD_CASE_SENSITIVE.value: "2005-07-02",
            DataType.DATE.value: to_timestamp(
                datetime.fromisoformat("2005-07-02T00:00:00")
            ),
        }

    @pytest.mark.parametrize(
        "value,epoch",
        [
            # RFC 3339
            ("1996-12-19T16:39:57-08:00", 851042397000),
            ("1990-12-31T23:59:59Z", 662687999000),
            # dates
            ("2012", 1325376000000),
            ("2012-05-03", 1336003200000),
            ("20120503", 1336003200000),
            ("2012-05", 1335830400000),
        ],
    )
    def test_date_formats(self, value: str, epoch: int):
        options = ParsingOptionsBuilder().with_default_date_formats().build()
        parsed = parse(value, options)
        assert parsed[DataType.DATE.value] == epoch

    def test_date_formats_that_we_want_ignore(self):
        options = ParsingOptionsBuilder().with_default_date_formats().build()
        assert DataType.DATE.value not in parse("12:04:23", options)
        assert DataType.DATE.value not in parse(
            "2007-03-01T13:00:00Z/2008-05-11T15:30:00Z", options
        )

    def test_none(self):
        options = ParsingOptionsBuilder().build()
        assert parse(None, options) == {
            DataType.TEXT.value: "",
            DataType.KEYWORD_CASE_INSENSITIVE.value: "",
            DataType.KEYWORD_CASE_SENSITIVE.value: "",
        }

    def test_caching_of_bools_and_ints(self):
        options = ParsingOptionsBuilder().build()

        parsed_bool = parse(False, options)
        parsed_int = parse(0, options)

        assert parsed_bool is not parsed_int

    def test_caching_of_ints_and_floats(self):
        options = ParsingOptionsBuilder().build()

        parsed_float = parse(3.0, options)
        parsed_int = parse(3, options)

        assert parsed_float is not parsed_int

    def test_ensure_bools_are_not_ints(self):
        options = ParsingOptionsBuilder().build()

        result = parse(True, options)
        assert DataType.BOOLEAN.value in result
        assert DataType.NUMBER.value not in result

    def test_ensure_ints_are_not_bools(self):
        options = ParsingOptionsBuilder().build()

        result = parse(1, options)
        assert DataType.BOOLEAN.value not in result
        assert DataType.NUMBER.value in result
