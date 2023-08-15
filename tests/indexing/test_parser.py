from itertools import chain

from splitgill.diffing import prepare
from splitgill.indexing.fields import TypeField
from splitgill.indexing.parser import parse_for_index, ParsedData, parse_str


class TestParseForIndex:
    def test_no_nesting(self):
        data = {"x": "beans"}
        parsed = parse_for_index(data)
        assert parsed == ParsedData(data, {"x": parse_str("beans")}, {}, {})

    def test_array_of_strings(self):
        data = {"x": ("beans", "lemons", "goats")}
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data, {"x": list(map(parse_str, data["x"]))}, {}, {"x": 3}
        )

    def test_nested_dict(self):
        data = {"x": "beans", "y": {"a": "5", "b": "buckets!"}}
        parsed = parse_for_index(data)
        assert parsed == ParsedData(
            data,
            {
                "x": parse_str("beans"),
                "y": {"a": parse_str("5"), "b": parse_str("buckets!")},
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
                    parse_str("4"),
                    parse_str("6"),
                    [
                        {"a": [parse_str("1"), parse_str("2")]},
                        {"a": [parse_str("6"), parse_str("1")]},
                    ],
                ],
                "y": {
                    "t": [
                        {"x": parse_str("4")},
                        {"x": parse_str("1")},
                        {"x": parse_str("7")},
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
                    "type": parse_str("Point"),
                    "coordinates": [parse_str("30.0"), parse_str("10.0")],
                },
                "y": parse_str("somewhere"),
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
                "type": parse_str("Point"),
                "coordinates": [parse_str("30.0"), parse_str("10.0")],
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
        assert parse_str("banana") == {
            TypeField.TEXT: "banana",
            TypeField.KEYWORD: "banana",
        }

    def test_bools_trues(self):
        options = ["true", "yes", "y"]
        for option in chain(options, [o.upper() for o in options]):
            assert parse_str(option) == {
                TypeField.TEXT: option,
                TypeField.KEYWORD: option,
                TypeField.BOOLEAN: True,
            }

    def test_bools_falses(self):
        options = ["false", "no", "n"]
        for option in chain(options, [o.upper() for o in options]):
            assert parse_str(option) == {
                TypeField.TEXT: option,
                TypeField.KEYWORD: option,
                TypeField.BOOLEAN: False,
            }

    def test_number(self):
        assert parse_str("5.3") == {
            TypeField.TEXT: "5.3",
            TypeField.KEYWORD: "5.3",
            TypeField.NUMBER: 5.3,
        }
        assert parse_str("70") == {
            TypeField.TEXT: "70",
            TypeField.KEYWORD: "70",
            TypeField.NUMBER: 70.0,
        }
        assert parse_str("70.0") == {
            TypeField.TEXT: "70.0",
            TypeField.KEYWORD: "70.0",
            TypeField.NUMBER: 70.0,
        }

    def test_invalid_numbers(self):
        assert TypeField.NUMBER not in parse_str("5.3.4")
        assert TypeField.NUMBER not in parse_str("NaN")
        assert TypeField.NUMBER not in parse_str("inf")

    def test_date_date_and_time(self):
        assert parse_str("2005-07-02 20:16:47.458301") == {
            TypeField.TEXT: "2005-07-02 20:16:47.458301",
            TypeField.KEYWORD: "2005-07-02 20:16:47.458301",
            TypeField.DATE: "2005-07-02T20:16:47.458301",
        }

    def test_date_date_and_time_and_tz(self):
        assert parse_str("2005-07-02 20:16:47.103+05:00") == {
            TypeField.TEXT: "2005-07-02 20:16:47.103+05:00",
            TypeField.KEYWORD: "2005-07-02 20:16:47.103+05:00",
            TypeField.DATE: "2005-07-02T20:16:47.103000+05:00",
        }

    def test_date_just_a_date(self):
        assert parse_str("2005-07-02") == {
            TypeField.TEXT: "2005-07-02",
            TypeField.KEYWORD: "2005-07-02",
            TypeField.DATE: "2005-07-02T00:00:00",
        }
