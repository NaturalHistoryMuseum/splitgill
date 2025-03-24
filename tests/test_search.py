from datetime import datetime, timezone

import pytest

from splitgill.indexing.fields import ParsedType
from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.indexing.parser import parse
from splitgill.search import (
    term_query,
    number,
    date,
    boolean,
    match_query,
    ALL_TEXT,
    text,
    keyword,
    range_query,
    rebuild_data,
)
from splitgill.utils import to_timestamp


class TestTermQuery:
    def test_no_infer(self):
        value = "banana"
        q = term_query("beans.toast", value, ParsedType.NUMBER)
        assert q.to_dict() == {"term": {number("beans.toast"): value}}

    def test_datetimes_are_converted(self):
        dt = datetime(2019, 6, 4, 14, 9, 45, tzinfo=timezone.utc)
        ms = to_timestamp(dt)

        # this should convert the datetime to an epoch
        q = term_query("beans.toast", dt, ParsedType.DATE)
        assert q.to_dict() == {"term": {date("beans.toast"): ms}}

        # this should not touch the value as it's not a datetime
        q = term_query("beans.toast", ms, ParsedType.DATE)
        assert q.to_dict() == {"term": {date("beans.toast"): ms}}

        # if the parsed type is inferred, it should still convert the datetime
        q = term_query("beans.toast", dt)
        assert q.to_dict() == {"term": {date("beans.toast"): ms}}

    def test_infer_number(self):
        assert term_query("beans.toast", 4).to_dict() == {
            "term": {number("beans.toast"): 4}
        }
        assert term_query("beans.toast", 9.2).to_dict() == {
            "term": {number("beans.toast"): 9.2}
        }

    def test_infer_boolean(self):
        assert term_query("beans.toast", True).to_dict() == {
            "term": {boolean("beans.toast"): True}
        }
        assert term_query("beans.toast", False).to_dict() == {
            "term": {boolean("beans.toast"): False}
        }

    def test_infer_date(self):
        dt = datetime(2019, 6, 4, 14, 9, 45, tzinfo=timezone.utc)
        ms = to_timestamp(dt)

        assert term_query("beans.toast", dt).to_dict() == {
            "term": {date("beans.toast"): ms}
        }

    def test_infer_str(self):
        assert term_query("beans.toast", "hello!").to_dict() == {
            "term": {keyword("beans.toast"): "hello!"}
        }

    def test_bad_type(self):
        with pytest.raises(ValueError):
            term_query("beans.toast", object())


class TestMatchQuery:
    def test_all_text(self):
        assert match_query("banana").to_dict() == {
            "match": {ALL_TEXT: {"query": "banana"}}
        }
        assert match_query("banana", fuzziness="AUTO").to_dict() == {
            "match": {ALL_TEXT: {"query": "banana", "fuzziness": "AUTO"}}
        }

    def test_a_field(self):
        assert match_query("banana", "beans.toast").to_dict() == {
            "match": {text("beans.toast"): {"query": "banana"}}
        }
        assert match_query("banana", "beans.toast", fuzziness="AUTO").to_dict() == {
            "match": {text("beans.toast"): {"query": "banana", "fuzziness": "AUTO"}}
        }


class TestRangeQuery:
    def test_int(self):
        assert range_query("beans.toast", 4, 10).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gte": 4,
                    "lt": 10,
                }
            }
        }

    def test_float(self):
        assert range_query("beans.toast", 4.5, 10.2).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gte": 4.5,
                    "lt": 10.2,
                }
            }
        }

    def test_number_mix(self):
        assert range_query("beans.toast", 4.5, 10).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gte": 4.5,
                    "lt": 10,
                }
            }
        }
        assert range_query("beans.toast", 4, 10.6).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gte": 4,
                    "lt": 10.6,
                }
            }
        }

    def test_lte_gte(self):
        assert range_query("beans.toast", gte=4, lte=10).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gte": 4,
                    "lte": 10,
                }
            }
        }
        assert range_query("beans.toast", gt=4, lt=10).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gt": 4,
                    "lt": 10,
                }
            }
        }
        assert range_query("beans.toast", gte=4, lt=10).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gte": 4,
                    "lt": 10,
                }
            }
        }
        assert range_query("beans.toast", gt=4, lte=10).to_dict() == {
            "range": {
                number("beans.toast"): {
                    "gt": 4,
                    "lte": 10,
                }
            }
        }

    def test_datetime(self):
        gte = datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        lt = datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

        assert range_query("beans.toast", gte, lt).to_dict() == {
            "range": {
                date("beans.toast"): {
                    "gte": to_timestamp(gte),
                    "lt": to_timestamp(lt),
                }
            }
        }

    def test_date(self):
        gte = datetime(2020, 1, 2)
        lt = datetime(2020, 1, 2)

        assert range_query("beans.toast", gte, lt).to_dict() == {
            "range": {
                date("beans.toast"): {
                    "gte": to_timestamp(gte),
                    "lt": to_timestamp(lt),
                }
            }
        }


rebuild_data_scenarios = [
    {"_id": "1", "x": 4, "y": None, "z": ""},
    {"_id": "1", "x": 4},
    {"_id": "2", "x": 4.2394823749823798423},
    {"_id": "3", "x": [1, 2, 3]},
    {"_id": "4", "x": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]},
    {"_id": "5", "x": {"y": 5, "z": 4.6}},
    {"_id": "6", "x": {"y": [1, 2, 3], "z": [4, 5, 6, [7, 8, 9, {"y": "beans"}]]}},
    {"_id": "7", "a geojson point": {"type": "Point", "coordinates": [30, 10]}},
    {
        "_id": "8",
        "anicelistofgeojson": [
            {"type": "Point", "coordinates": [30, 10]},
            {"type": "Point", "coordinates": [20, 20]},
            {"type": "Point", "coordinates": [10, 30]},
        ],
    },
]


@pytest.mark.parametrize("data", rebuild_data_scenarios)
def test_rebuild(data: dict):
    options = ParsingOptionsBuilder().build()
    parsed = parse(data, options)
    rebuilt_data = rebuild_data(parsed.parsed)
    assert rebuilt_data == data
