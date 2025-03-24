from datetime import datetime, date, timezone, timedelta
from itertools import chain

import pytest
from shapely import from_wkt

from splitgill.diffing import prepare_data
from splitgill.indexing.fields import ParsedType, DataType
from splitgill.indexing.geo import match_hints, match_geojson
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.parser import parse, parse_value
from splitgill.model import ParsingOptions
from splitgill.utils import to_timestamp


def test_in_and_out_of_dates():
    # check that using the default options, we can put a date through prepare_data and
    # get the correct date back out when we parse it
    options = ParsingOptionsBuilder().build()
    tz = timezone(timedelta(hours=7))
    candidates = [
        date(2021, 1, 5),
        datetime(2021, 1, 5, 6, 23, 17),
        datetime(2021, 1, 5, 6, 23, 17, 234567),
        datetime(2021, 1, 5, 6, 23, 17, tzinfo=tz),
        datetime(2021, 1, 5, 6, 23, 17, 234567, tzinfo=tz),
    ]

    for candidate in candidates:
        parsed = parse_value(prepare_data(candidate), options)
        assert ParsedType.DATE in parsed
        if isinstance(candidate, datetime) and candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=timezone.utc)

        assert parsed[ParsedType.DATE] == to_timestamp(candidate)


def pt(path: str, *types: ParsedType, include_text: bool = True) -> str:
    types = list(types)
    # original is always there
    types.append(ParsedType.UNPARSED)
    if include_text:
        types.append(ParsedType.KEYWORD)
        types.append(ParsedType.TEXT)
    return f"{path}.{','.join(sorted(types))}"


def dt(path: str, *types: DataType) -> str:
    return f"{path}.{','.join(sorted(types))}"


class TestParse:
    def test_no_nesting(self, basic_options: ParsingOptions):
        data = {"x": "beans"}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {"x": parse_value("beans", basic_options)}
        assert parsed_data.data_types == [dt("x", DataType.STR)]
        assert parsed_data.parsed_types == [pt("x")]

    def test_list_of_strings(self, basic_options: ParsingOptions):
        data = {"x": ["beans", "lemons", "goats"]}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": [parse_value(value, basic_options) for value in data["x"]]
        }
        assert parsed_data.data_types == [
            dt("x", DataType.LIST),
            dt("x.", DataType.STR),
        ]
        assert parsed_data.parsed_types == [pt("x")]

    def test_list_of_dicts(self, basic_options: ParsingOptions):
        data = {"x": [{"a": 4}, {"a": 5}, {"a": 6}]}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": [
                {"a": parse_value(4, basic_options)},
                {"a": parse_value(5, basic_options)},
                {"a": parse_value(6, basic_options)},
            ]
        }
        assert parsed_data.data_types == [
            dt("x", DataType.LIST),
            dt("x.", DataType.DICT),
            dt("x..a", DataType.INT),
        ]
        assert parsed_data.parsed_types == [pt("x.a", ParsedType.NUMBER)]

    def test_list_of_lists(self, basic_options: ParsingOptions):
        data = {"x": [[1, 2, 3], [4, 5, 6]]}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": [
                [parse_value(value, basic_options) for value in [1, 2, 3]],
                [parse_value(value, basic_options) for value in [4, 5, 6]],
            ]
        }
        assert parsed_data.data_types == [
            dt("x", DataType.LIST),
            dt("x.", DataType.LIST),
            dt("x..", DataType.INT),
        ]
        assert parsed_data.parsed_types == [pt("x", ParsedType.NUMBER)]

    def test_nested_dict(self, basic_options: ParsingOptions):
        data = {"x": "beans", "y": {"a": "5", "b": "buckets!"}}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": parse_value("beans", basic_options),
            "y": {
                "a": parse_value("5", basic_options),
                "b": parse_value("buckets!", basic_options),
            },
        }
        assert parsed_data.data_types == [
            dt("x", DataType.STR),
            dt("y", DataType.DICT),
            dt("y.a", DataType.STR),
            dt("y.b", DataType.STR),
        ]
        assert parsed_data.parsed_types == [
            pt("x"),
            pt("y.a", ParsedType.NUMBER),
            pt("y.b"),
        ]

    def test_nested_mix(self, basic_options: ParsingOptions):
        data = {
            "x": ["4", "6", [{"a": ["1", "2"]}, {"a": ["6", "1"]}]],
            "y": {"t": [{"x": 4}, {"x": 1}, {"x": 5.6}]},
        }
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": [
                parse_value("4", basic_options),
                parse_value("6", basic_options),
                [
                    {
                        "a": [
                            parse_value("1", basic_options),
                            parse_value("2", basic_options),
                        ]
                    },
                    {
                        "a": [
                            parse_value("6", basic_options),
                            parse_value("1", basic_options),
                        ]
                    },
                ],
            ],
            "y": {
                "t": [
                    {"x": parse_value(4, basic_options)},
                    {"x": parse_value(1, basic_options)},
                    {"x": parse_value(5.6, basic_options)},
                ]
            },
        }
        assert parsed_data.data_types == [
            dt("x", DataType.LIST),
            dt("x.", DataType.STR, DataType.LIST),
            dt("x..", DataType.DICT),
            dt("x...a", DataType.LIST),
            dt("x...a.", DataType.STR),
            dt("y", DataType.DICT),
            dt("y.t", DataType.LIST),
            dt("y.t.", DataType.DICT),
            dt("y.t..x", DataType.INT, DataType.FLOAT),
        ]
        assert parsed_data.parsed_types == [
            pt("x", ParsedType.NUMBER),
            pt("x.a", ParsedType.NUMBER),
            pt("y.t.x", ParsedType.NUMBER),
        ]

    def test_geo_hinted_fields(self, basic_options: ParsingOptions):
        data = {
            "x": "something",
            "y": "somewhere",
            "decimalLatitude": 14.897,
            "decimalLongitude": -87.956,
        }

        parsed_data = parse(data, basic_options)
        geo_data = next(iter(match_hints(data, basic_options.geo_hints).values()))
        assert parsed_data.parsed == {
            "x": parse_value("something", basic_options),
            "y": parse_value("somewhere", basic_options),
            "decimalLatitude": {**parse_value(14.897, basic_options), **geo_data},
            "decimalLongitude": parse_value(-87.956, basic_options),
        }

    def test_geojson_field(
        self, geojson_point: dict, wkt_point: str, basic_options: ParsingOptions
    ):
        data = {
            "x": geojson_point,
            "y": "somewhere",
        }
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": {
                ParsedType.GEO_POINT: wkt_point,
                ParsedType.GEO_SHAPE: wkt_point,
                "type": parse_value("Point", basic_options),
                "coordinates": [
                    parse_value(30, basic_options),
                    parse_value(10, basic_options),
                ],
            },
            "y": parse_value("somewhere", basic_options),
        }

    def test_geojson_at_root_not_recognised(
        self, geojson_point: dict, basic_options: ParsingOptions
    ):
        parsed_data = parse(geojson_point, basic_options)

        assert parsed_data.parsed == {
            "type": parse_value("Point", basic_options),
            "coordinates": [
                parse_value(30.0, basic_options),
                parse_value(10.0, basic_options),
            ],
        }

    def test_geojson_field_list(
        self,
        geojson_point: dict,
        geojson_polygon: dict,
        geojson_linestring: dict,
        geojson_holed_polygon: dict,
        basic_options: ParsingOptions,
    ):
        data = {
            "x": [
                geojson_point,
                geojson_linestring,
                geojson_polygon,
                geojson_holed_polygon,
            ]
        }
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "x": [
                {
                    **parse(value, basic_options).parsed,
                    **match_geojson(value),
                }
                for value in [
                    geojson_point,
                    geojson_linestring,
                    geojson_polygon,
                    geojson_holed_polygon,
                ]
            ]
        }

    def test_dict_with_nulls(self, basic_options: ParsingOptions):
        data = {"a": "hello", "b": None, "c": ""}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "a": parse_value("hello", basic_options),
            "b": {ParsedType.UNPARSED: None},
            "c": {ParsedType.UNPARSED: ""},
        }
        assert parsed_data.data_types == [
            f"a.{DataType.STR}",
            f"b.{DataType.NONE}",
            f"c.{DataType.STR}",
        ]
        assert parsed_data.parsed_types == [pt("a")]

    def test_list_with_nulls(self, basic_options: ParsingOptions):
        data = {"a": ["hello", None, ""]}
        parsed_data = parse(data, basic_options)

        assert parsed_data.parsed == {
            "a": [parse_value("hello", basic_options), None, None]
        }
        assert parsed_data.data_types == [
            dt("a", DataType.LIST),
            dt("a.", DataType.NONE, DataType.STR),
        ]
        assert parsed_data.parsed_types == [pt("a")]


class TestParseValue:
    def test_normal_text(self, basic_options: ParsingOptions):
        assert parse_value("banana", basic_options) == {
            ParsedType.UNPARSED: "banana",
            ParsedType.TEXT: "banana",
            ParsedType.KEYWORD: "banana",
        }

    def test_bools(self, basic_options: ParsingOptions):
        for value in chain(
            basic_options.true_values, [o.upper() for o in basic_options.true_values]
        ):
            assert parse_value(value, basic_options) == {
                ParsedType.UNPARSED: value,
                ParsedType.TEXT: value,
                ParsedType.KEYWORD: value,
                ParsedType.BOOLEAN: True,
            }
        for value in chain(
            basic_options.false_values, [o.upper() for o in basic_options.false_values]
        ):
            assert parse_value(value, basic_options) == {
                ParsedType.UNPARSED: value,
                ParsedType.TEXT: value,
                ParsedType.KEYWORD: value,
                ParsedType.BOOLEAN: False,
            }

    def test_number(self, basic_options: ParsingOptions):
        assert parse_value("5.3", basic_options) == {
            ParsedType.UNPARSED: "5.3",
            ParsedType.TEXT: "5.3",
            ParsedType.KEYWORD: "5.3",
            ParsedType.NUMBER: 5.3,
        }
        assert parse_value("70", basic_options) == {
            ParsedType.UNPARSED: "70",
            ParsedType.TEXT: "70",
            ParsedType.KEYWORD: "70",
            ParsedType.NUMBER: 70.0,
        }
        assert parse_value("70.0", basic_options) == {
            ParsedType.UNPARSED: "70.0",
            ParsedType.TEXT: "70.0",
            ParsedType.KEYWORD: "70.0",
            ParsedType.NUMBER: 70.0,
        }
        assert parse_value(4, basic_options) == {
            ParsedType.UNPARSED: 4,
            ParsedType.TEXT: "4",
            ParsedType.KEYWORD: "4",
            ParsedType.NUMBER: 4,
        }
        assert parse_value(16.04, basic_options) == {
            ParsedType.UNPARSED: 16.04,
            ParsedType.TEXT: "16.04",
            ParsedType.KEYWORD: "16.04",
            ParsedType.NUMBER: 16.04,
        }
        assert parse_value(16.042245342119813456, basic_options) == {
            ParsedType.UNPARSED: 16.042245342119813456,
            ParsedType.TEXT: "16.0422453421198",
            ParsedType.KEYWORD: "16.0422453421198",
            ParsedType.NUMBER: 16.042245342119813456,
        }
        assert parse_value("1.2312e-20", basic_options) == {
            ParsedType.UNPARSED: "1.2312e-20",
            ParsedType.TEXT: "1.2312e-20",
            ParsedType.KEYWORD: "1.2312e-20",
            ParsedType.NUMBER: 1.2312e-20,
        }

    def test_invalid_numbers(self, basic_options: ParsingOptions):
        assert ParsedType.NUMBER.value not in parse_value("5.3.4", basic_options)
        assert ParsedType.NUMBER.value not in parse_value("NaN", basic_options)
        assert ParsedType.NUMBER.value not in parse_value("inf", basic_options)

    def test_date_date_and_time(self, basic_options: ParsingOptions):
        value = "2005-07-02 20:16:47.458301"

        assert parse_value(value, basic_options) == {
            ParsedType.UNPARSED: value,
            ParsedType.TEXT: value,
            ParsedType.KEYWORD: value,
            ParsedType.DATE: to_timestamp(
                # check the timestamp is converted correctly, it'll be UTC so add +00:00
                datetime.fromisoformat(f"{value}+00:00")
            ),
        }

    def test_date_date_and_time_and_tz(self, basic_options: ParsingOptions):
        assert parse_value("2005-07-02 20:16:47.103+05:00", basic_options) == {
            ParsedType.UNPARSED: "2005-07-02 20:16:47.103+05:00",
            ParsedType.TEXT: "2005-07-02 20:16:47.103+05:00",
            ParsedType.KEYWORD: "2005-07-02 20:16:47.103+05:00",
            ParsedType.DATE: to_timestamp(
                datetime.fromisoformat("2005-07-02T20:16:47.103000+05:00")
            ),
        }

    def test_date_just_a_date(self, basic_options: ParsingOptions):
        value = "2005-07-02"

        assert parse_value(value, basic_options) == {
            ParsedType.UNPARSED: value,
            ParsedType.TEXT: value,
            ParsedType.KEYWORD: value,
            ParsedType.DATE: to_timestamp(
                # use midnight UTC
                datetime.fromisoformat(f"{value}T00:00:00+00:00")
            ),
        }

    @pytest.mark.parametrize(
        "value,epoch",
        [
            # RFC 3339
            ("1996-12-19T16:39:57-08:00", 851042397000),
            ("1990-12-31T23:59:59+00:00", 662687999000),
            # dates
            ("2012-05-03", 1336003200000),
        ],
    )
    def test_date_formats(self, value: str, epoch: int, basic_options: ParsingOptions):
        parsed = parse_value(value, basic_options)
        assert parsed[ParsedType.DATE.value] == epoch

    def test_date_formats_that_we_want_ignore(self, basic_options: ParsingOptions):
        assert ParsedType.DATE.value not in parse_value("12:04:23", basic_options)
        assert ParsedType.DATE.value not in parse_value(
            "2007-03-01T13:00:00Z.2008-05-11T15:30:00Z", basic_options
        )

    def test_caching_of_bools_and_ints(self, basic_options: ParsingOptions):
        parsed_bool = parse_value(False, basic_options)
        parsed_int = parse_value(0, basic_options)

        assert parsed_bool is not parsed_int

    def test_caching_of_ints_and_floats(self, basic_options: ParsingOptions):
        parsed_float = parse_value(3.0, basic_options)
        parsed_int = parse_value(3, basic_options)

        assert parsed_float is not parsed_int

    def test_ensure_bools_are_not_ints(self, basic_options: ParsingOptions):
        result = parse_value(True, basic_options)
        assert ParsedType.BOOLEAN.value in result
        assert ParsedType.NUMBER.value not in result

    def test_ensure_ints_are_not_bools(self, basic_options: ParsingOptions):
        result = parse_value(1, basic_options)
        assert ParsedType.BOOLEAN.value not in result
        assert ParsedType.NUMBER.value in result

    def test_wkt_point(self, wkt_point: str, basic_options: ParsingOptions):
        result = parse_value(wkt_point, basic_options)
        assert result == {
            ParsedType.UNPARSED: wkt_point,
            ParsedType.TEXT: wkt_point,
            ParsedType.KEYWORD: wkt_point,
            ParsedType.GEO_POINT: wkt_point,
            ParsedType.GEO_SHAPE: wkt_point,
        }

    def test_wkt_linestring(self, wkt_linestring: str, basic_options: ParsingOptions):
        result = parse_value(wkt_linestring, basic_options)
        assert result == {
            ParsedType.UNPARSED: wkt_linestring,
            ParsedType.TEXT: wkt_linestring,
            ParsedType.KEYWORD: wkt_linestring,
            ParsedType.GEO_POINT: from_wkt(wkt_linestring).centroid.wkt,
            ParsedType.GEO_SHAPE: wkt_linestring,
        }

    def test_wkt_polygon(self, wkt_polygon: str, basic_options: ParsingOptions):
        result = parse_value(wkt_polygon, basic_options)
        assert result == {
            ParsedType.UNPARSED: wkt_polygon,
            ParsedType.TEXT: wkt_polygon,
            ParsedType.KEYWORD: wkt_polygon,
            ParsedType.GEO_POINT: from_wkt(wkt_polygon).centroid.wkt,
            ParsedType.GEO_SHAPE: wkt_polygon.upper(),
        }

    def test_wkt_holed_polygon(
        self, wkt_holed_polygon: str, basic_options: ParsingOptions
    ):
        result = parse_value(wkt_holed_polygon, basic_options)
        assert result == {
            ParsedType.UNPARSED: wkt_holed_polygon,
            ParsedType.TEXT: wkt_holed_polygon,
            ParsedType.KEYWORD: wkt_holed_polygon,
            ParsedType.GEO_POINT: from_wkt(wkt_holed_polygon).centroid.wkt,
            ParsedType.GEO_SHAPE: wkt_holed_polygon.upper(),
        }
