from datetime import datetime
from itertools import chain

import pytest

from splitgill.diffing import prepare
from splitgill.indexing.fields import TypeField
from splitgill.indexing.parser import parse_for_index, ParsedData, parse
from splitgill.utils import to_timestamp


class TestParseForIndex:
    def test_no_nesting(self):
        data = {"x": "beans"}
        parsed = parse_for_index(data)
        assert parsed == ParsedData(data, {"x": parse("beans")}, {}, {})

    def test_array_of_strings(self):
        data = {"x": ("beans", "lemons", "goats")}
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data, {"x": list(map(parse, data["x"]))}, {}, {"x": 3}
        )

    def test_nested_dict(self):
        data = {"x": "beans", "y": {"a": "5", "b": "buckets!"}}
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data,
            {
                "x": parse("beans"),
                "y": {"a": parse("5"), "b": parse("buckets!")},
            },
            {},
            {},
        )

    def test_nested_mix(self):
        data = {
            "x": ("4", "6", ({"a": ("1", "2")}, {"a": ("6", "1")})),
            "y": {"t": ({"x": "4"}, {"x": "1"}, {"x": "7"})},
        }
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data,
            {
                "x": [
                    parse("4"),
                    parse("6"),
                    [
                        {"a": [parse("1"), parse("2")]},
                        {"a": [parse("6"), parse("1")]},
                    ],
                ],
                "y": {
                    "t": [
                        {"x": parse("4")},
                        {"x": parse("1")},
                        {"x": parse("7")},
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
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data,
            {
                "x": parse("something"),
                "y": parse("somewhere"),
                "decimalLatitude": parse("14.897"),
                "decimalLongitude": parse("-87.956"),
            },
            {
                "decimalLatitude/decimalLongitude": {
                    "type": "Point",
                    "coordinates": (-87.956, 14.897),
                }
            },
            {},
        )

    def test_geojson_field(self, geojson_point: dict):
        data = {
            "x": prepare(geojson_point),
            "y": "somewhere",
        }
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data,
            {
                "x": {
                    "type": parse("Point"),
                    "coordinates": [parse(30.0), parse(10.0)],
                },
                "y": parse("somewhere"),
            },
            {
                "x": geojson_point,
            },
            {"x.coordinates": 2},
        )

    def test_geojson_at_root_not_recognised(self, geojson_point: dict):
        data = prepare(geojson_point)
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data,
            {
                "type": parse("Point"),
                "coordinates": [parse(30.0), parse(10.0)],
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
            "x": prepare(
                (
                    geojson_point,
                    geojson_linestring,
                    geojson_polygon,
                    geojson_holed_polygon,
                )
            ),
        }
        parsed = parse_for_index(data)
        assert parsed.arrays["x"] == 4
        assert parsed.geo == {
            "x.0": geojson_point,
            "x.1": geojson_linestring,
            "x.2": geojson_polygon,
            "x.3": geojson_holed_polygon,
        }


class TestParseStr:
    def test_normal_text(self):
        assert parse("banana") == {
            TypeField.TEXT: "banana",
            TypeField.KEYWORD_CASE_INSENSITIVE: "banana",
            TypeField.KEYWORD_CASE_SENSITIVE: "banana",
        }

    def test_bools_trues(self):
        options = ["true", "yes", "y"]
        for option in chain(options, [o.upper() for o in options]):
            assert parse(option) == {
                TypeField.TEXT: option,
                TypeField.KEYWORD_CASE_INSENSITIVE: option,
                TypeField.KEYWORD_CASE_SENSITIVE: option,
                TypeField.BOOLEAN: True,
            }

    def test_bools_falses(self):
        options = ["false", "no", "n"]
        for option in chain(options, [o.upper() for o in options]):
            assert parse(option) == {
                TypeField.TEXT: option,
                TypeField.KEYWORD_CASE_INSENSITIVE: option,
                TypeField.KEYWORD_CASE_SENSITIVE: option,
                TypeField.BOOLEAN: False,
            }

    def test_number(self):
        assert parse("5.3") == {
            TypeField.TEXT: "5.3",
            TypeField.KEYWORD_CASE_INSENSITIVE: "5.3",
            TypeField.KEYWORD_CASE_SENSITIVE: "5.3",
            TypeField.NUMBER: 5.3,
        }
        assert parse("70") == {
            TypeField.TEXT: "70",
            TypeField.KEYWORD_CASE_INSENSITIVE: "70",
            TypeField.KEYWORD_CASE_SENSITIVE: "70",
            TypeField.NUMBER: 70.0,
        }
        assert parse("70.0") == {
            TypeField.TEXT: "70.0",
            TypeField.KEYWORD_CASE_INSENSITIVE: "70.0",
            TypeField.KEYWORD_CASE_SENSITIVE: "70.0",
            TypeField.NUMBER: 70.0,
        }
        assert parse(4) == {
            TypeField.TEXT: "4",
            TypeField.KEYWORD_CASE_INSENSITIVE: "4",
            TypeField.KEYWORD_CASE_SENSITIVE: "4",
            TypeField.NUMBER: 4,
        }
        assert parse(16.04) == {
            TypeField.TEXT: "16.04",
            TypeField.KEYWORD_CASE_INSENSITIVE: "16.04",
            TypeField.KEYWORD_CASE_SENSITIVE: "16.04",
            TypeField.NUMBER: 16.04,
        }
        assert parse(16.042245342119813456) == {
            TypeField.TEXT: "16.0422453421198",
            TypeField.KEYWORD_CASE_INSENSITIVE: "16.0422453421198",
            TypeField.KEYWORD_CASE_SENSITIVE: "16.0422453421198",
            TypeField.NUMBER: 16.042245342119813456,
        }
        assert parse("1.2312e-20") == {
            TypeField.TEXT: "1.2312e-20",
            TypeField.KEYWORD_CASE_INSENSITIVE: "1.2312e-20",
            TypeField.KEYWORD_CASE_SENSITIVE: "1.2312e-20",
            TypeField.NUMBER: 1.2312e-20,
        }

    def test_invalid_numbers(self):
        assert TypeField.NUMBER not in parse("5.3.4")
        assert TypeField.NUMBER not in parse("NaN")
        assert TypeField.NUMBER not in parse("inf")

    def test_date_date_and_time(self):
        assert parse("2005-07-02 20:16:47.458301") == {
            TypeField.TEXT: "2005-07-02 20:16:47.458301",
            TypeField.KEYWORD_CASE_INSENSITIVE: "2005-07-02 20:16:47.458301",
            TypeField.KEYWORD_CASE_SENSITIVE: "2005-07-02 20:16:47.458301",
            TypeField.DATE: to_timestamp(
                datetime.fromisoformat("2005-07-02T20:16:47.458301")
            ),
        }

    def test_date_date_and_time_and_tz(self):
        assert parse("2005-07-02 20:16:47.103+05:00") == {
            TypeField.TEXT: "2005-07-02 20:16:47.103+05:00",
            TypeField.KEYWORD_CASE_INSENSITIVE: "2005-07-02 20:16:47.103+05:00",
            TypeField.KEYWORD_CASE_SENSITIVE: "2005-07-02 20:16:47.103+05:00",
            TypeField.DATE: to_timestamp(
                datetime.fromisoformat("2005-07-02T20:16:47.103000+05:00")
            ),
        }

    def test_date_just_a_date(self):
        assert parse("2005-07-02") == {
            TypeField.TEXT: "2005-07-02",
            TypeField.KEYWORD_CASE_INSENSITIVE: "2005-07-02",
            TypeField.KEYWORD_CASE_SENSITIVE: "2005-07-02",
            TypeField.DATE: to_timestamp(datetime.fromisoformat("2005-07-02T00:00:00")),
        }

    # these scenarios come from the docs: https://pendulum.eustace.io/docs/#parsing
    @pytest.mark.parametrize(
        "value,epoch",
        [
            # RFC 3339
            ("1996-12-19T16:39:57-08:00", 851042397000),
            ("1990-12-31T23:59:59Z", 662687999000),
            # ISO 8601
            ("20161001T143028+0530", 1475312428000),
            ("20161001T14", 1475330400000),
            # dates
            ("2012", 1325376000000),
            ("2012-05-03", 1336003200000),
            ("20120503", 1336003200000),
            ("2012-05", 1335830400000),
            # ordinal day
            ("2012-007", 1325894400000),
            ("2012007", 1325894400000),
            # week number
            ("2012-W05", 1327881600000),
            ("2012W05", 1327881600000),
            ("2012-W05-5", 1328227200000),
            ("2012W055", 1328227200000),
        ],
    )
    def test_date_formats(self, value: str, epoch: int):
        parsed = parse(value)
        assert parsed[TypeField.DATE] == epoch

    def test_date_formats_that_we_want_ignore(self):
        assert TypeField.DATE not in parse("12:04:23")
        assert TypeField.DATE not in parse("2007-03-01T13:00:00Z/2008-05-11T15:30:00Z")

    def test_none(self):
        assert parse(None) == {
            TypeField.TEXT: "",
            TypeField.KEYWORD_CASE_INSENSITIVE: "",
            TypeField.KEYWORD_CASE_SENSITIVE: "",
        }
