from datetime import datetime, timezone, timedelta, date
from decimal import Decimal

import pytest

from splitgill.diffing import (
    prepare_data,
    diff,
    DiffOp,
    DiffingTypeComparisonException,
    patch,
    DictComparison,
    ListComparison,
    prepare_field_name,
)


class TestPrepare:
    def test_none(self):
        assert prepare_data(None) is None

    def test_str(self):
        assert prepare_data("beans") == "beans"
        assert prepare_data("beans\tand\rlemons\neh?") == "beans\tand\rlemons\neh?"
        assert prepare_data("beans\x07andabell") == "beansandabell"
        assert (
            prepare_data("bea\x07ns\tand\rlem\x00ons\neh?") == "beans\tand\rlemons\neh?"
        )

    def test_numbers(self):
        assert prepare_data(23) == 23
        assert prepare_data(23.456) == 23.456
        assert prepare_data(-20.5012) == -20.5012
        assert prepare_data(0) == 0
        # only ints and float please!
        assert prepare_data(complex(3, 4)) == "(3+4j)"
        assert prepare_data(Decimal("3.4")) == "3.4"

    def test_bool(self):
        assert prepare_data(True) is True
        assert prepare_data(False) is False

    def test_datetime(self):
        naive_no_ms = datetime(2020, 5, 18, 15, 16, 56)
        assert prepare_data(naive_no_ms) == "2020-05-18T15:16:56.000000"
        naive_with_ms = datetime(2020, 5, 18, 15, 16, 56, 2908)
        assert prepare_data(naive_with_ms) == "2020-05-18T15:16:56.002908"

        minus_3_hours = timezone(timedelta(hours=-3))
        with_tz_no_ms = datetime(2020, 5, 18, 15, 16, 56, tzinfo=minus_3_hours)
        assert prepare_data(with_tz_no_ms) == "2020-05-18T15:16:56.000000-0300"
        with_tz_with_ms = datetime(2020, 5, 18, 15, 16, 56, 2908, tzinfo=minus_3_hours)
        assert prepare_data(with_tz_with_ms) == "2020-05-18T15:16:56.002908-0300"

    def test_date(self):
        assert prepare_data(date(2024, 3, 10)) == "2024-03-10"

    def test_dict(self):
        assert prepare_data({}) == {}
        assert prepare_data({"x": None}) == {"x": None}
        assert prepare_data({"x": "beans"}) == {"x": "beans"}
        assert prepare_data({"x": "4"}) == {"x": prepare_data("4")}
        assert prepare_data({3: True}) == {"3": prepare_data(True)}
        assert prepare_data({4: {6: 1}}) == {"4": {"6": prepare_data(1)}}
        assert prepare_data({"x.y": "4"}) == {"x_y": prepare_data("4")}
        assert prepare_data({"x\ny": "4"}) == {"xy": prepare_data("4")}
        assert prepare_data({"x\ny.n  ": "4"}) == {"xy_n": prepare_data("4")}

    def test_list(self):
        assert prepare_data([]) == []
        assert prepare_data([3, None, 5]) == [3, None, 5]
        assert prepare_data([1, 2, 3]) == [1, 2, 3]
        assert prepare_data([1, True, "3"]) == [1, True, "3"]

    def test_set(self):
        assert prepare_data(set()) == []

        prepared = prepare_data({1, 2, 3, "beans", None})
        assert isinstance(prepared, list)
        assert 1 in prepared
        assert 2 in prepared
        assert 3 in prepared
        assert "beans" in prepared
        assert None in prepared

    def test_tuple(self):
        assert prepare_data(tuple()) == []
        assert prepare_data((3, None, 5)) == [3, None, 5]
        assert prepare_data((1, 2, 3)) == [1, 2, 3]
        assert prepare_data((1, True, "3")) == [1, True, "3"]

    def test_fallback(self):
        class A:
            def __str__(self):
                return "beans"

        assert prepare_data(A()) == "beans"

    def test_mix(self):
        prepared = prepare_data(
            {
                "x": "4",
                "y": True,
                "z": [1, 2, 3],
                "a": {
                    "x": ["4", 20.7],
                    "y": datetime(2020, 5, 18, 15, 16, 56),
                },
                "b": [{"x": 1}, {"x": "4.2"}],
            }
        )
        assert prepared == {
            "x": "4",
            "y": True,
            "z": [1, 2, 3],
            "a": {
                "x": ["4", 20.7],
                "y": "2020-05-18T15:16:56.000000",
            },
            "b": [{"x": 1}, {"x": "4.2"}],
        }


def test_prepare_field_name():
    # not a str
    assert prepare_field_name(5) == "5"
    # a dot!
    assert prepare_field_name("x.y") == "x_y"
    # padded with whitespace
    assert prepare_field_name(" x   ") == "x"
    # lots of dots
    assert prepare_field_name(".x.y.z.1.2.") == "_x_y_z_1_2_"
    # a mix of horrors
    assert prepare_field_name("\nx.\ty\r  \x07fowien") == "x_y  fowien"


class TestDiff:
    def test_equal(self):
        base = {"x": "4"}
        new = {"x": "4"}
        assert list(diff(base, new)) == []

    def test_equal_is(self):
        base = new = {"x": "4"}
        assert list(diff(base, new)) == []

    def test_not_dicts(self):
        with pytest.raises(DiffingTypeComparisonException):
            list(diff(("1", "2", "3"), {"a": "4"}))
        with pytest.raises(DiffingTypeComparisonException):
            list(diff({"a": "4"}, ("1", "2", "3")))
        with pytest.raises(DiffingTypeComparisonException):
            list(diff("4", "beans"))

    def test_dict_new(self):
        base = {"a": "4"}
        new = {"a": "4", "b": "3"}
        assert list(diff(base, new)) == [DiffOp(tuple(), {"dn": {"b": "3"}})]

    def test_dict_delete(self):
        base = {"a": "4", "b": "3"}
        new = {"a": "4"}
        assert list(diff(base, new)) == [DiffOp(tuple(), {"dd": ["b"]})]

    def test_dict_change(self):
        base = {"a": "4", "b": "3"}
        new = {"a": "4", "b": "6"}
        assert list(diff(base, new)) == [DiffOp(tuple(), {"dc": {"b": "6"}})]

    def test_list_new(self):
        base = {"a": ["1", "2", "3"]}
        new = {"a": ["1", "2", "3", "4", "5"]}
        assert list(diff(base, new)) == [DiffOp(("a",), {"ln": ["4", "5"]})]

    def test_list_delete(self):
        base = {"a": ["1", "2", "3", "4", "5"]}
        new = {"a": ["1", "2", "3"]}
        assert list(diff(base, new)) == [DiffOp(("a",), {"ld": 3})]

    def test_list_change(self):
        base = {"a": ["1", "2", "3", "4", "5"]}
        new = {"a": ["1", "2", "3", "10", "5"]}
        assert list(diff(base, new)) == [DiffOp(("a",), {"lc": [(3, "10")]})]

    def test_list_with_embeds(self):
        base = {
            "a": [{"y": "4"}, {"z": "5"}],
            "b": [["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]],
        }
        new = {
            "a": [{"y": "4"}, {"z": "3"}],
            "b": [["1", "10", "3"], ["4", "5", "6"], ["4", "8", "9"]],
        }
        assert list(diff(base, new)) == [
            DiffOp(path=("a", 1), ops={"dc": {"z": "3"}}),
            DiffOp(path=("b", 0), ops={"lc": [(1, "10")]}),
            DiffOp(path=("b", 2), ops={"lc": [(0, "4")]}),
        ]

    def test_dict_embeds(self):
        base = {"a": {"b": "5", "c": "6"}}
        new = {"a": {"a": "2", "c": "4"}}
        assert list(diff(base, new)) == [
            DiffOp(path=("a",), ops={"dn": {"a": "2"}, "dd": ["b"], "dc": {"c": "4"}})
        ]


class TestDictComparisonCompare:
    def test_same(self):
        op, more = DictComparison(tuple(), {}, {}).compare()
        assert op is None
        assert len(more) == 0

        op, more = DictComparison(tuple(), {"a": 4}, {"a": 4}).compare()
        assert op is None
        assert len(more) == 0

        base = {"a": 4}
        op, more = DictComparison(tuple(), base, base).compare()
        assert op is None
        assert len(more) == 0

    def test_dn(self):
        comp = DictComparison(tuple(), {"a": 4}, {"a": 4, "b": 3, "c": 8})
        op, more = comp.compare()
        assert op.path == tuple()
        assert op.ops == {"dn": {"b": 3, "c": 8}}
        assert len(more) == 0

    def test_dd(self):
        comp = DictComparison(tuple(), {"a": 4, "b": 3, "c": 8}, {"a": 4})
        op, more = comp.compare()
        assert op.path == tuple()
        assert op.ops == {"dd": ["b", "c"]}
        assert len(more) == 0

    def test_dc(self):
        comp = DictComparison(tuple(), {"a": 4, "b": 3}, {"a": 1, "b": 9})
        op, more = comp.compare()
        assert op.path == tuple()
        assert op.ops == {"dc": {"a": 1, "b": 9}}
        assert len(more) == 0

    def test_nested_dicts(self):
        # both dicts
        op, more = DictComparison(("x",), {"a": {"x": 3}}, {"a": {"x": 4}}).compare()
        assert op is None
        assert more == [DictComparison(("x", "a"), {"x": 3}, {"x": 4})]

    def test_nested_dict_and_not(self):
        # one a dict, one not
        op, more = DictComparison(tuple(), {"a": {"x": 3}}, {"a": "x"}).compare()
        assert op.path == tuple()
        assert op.ops == {"dc": {"a": "x"}}
        assert len(more) == 0

        # one not a dict, one a dict
        op, more = DictComparison(tuple(), {"a": "x"}, {"a": {"x": 3}}).compare()
        assert op.path == tuple()
        assert op.ops == {"dc": {"a": {"x": 3}}}
        assert len(more) == 0

    def test_nested_lists(self):
        # both lists
        op, more = DictComparison(("x",), {"a": [1, 2, 3]}, {"a": [1, 2, 4]}).compare()
        assert op is None
        assert more == [ListComparison(("x", "a"), [1, 2, 3], [1, 2, 4])]

    def test_nested_list_and_not(self):
        # both dicts
        op, more = DictComparison(tuple(), {"a": [1, 2, 3]}, {"a": "x"}).compare()
        assert op.path == tuple()
        assert op.ops == {"dc": {"a": "x"}}
        assert len(more) == 0

        op, more = DictComparison(tuple(), {"a": "x"}, {"a": [1, 2, 3]}).compare()
        assert op.path == tuple()
        assert op.ops == {"dc": {"a": [1, 2, 3]}}
        assert len(more) == 0


class TestListComparisonCompare:
    def test_same(self):
        op, more = ListComparison(tuple(), [], []).compare()
        assert op is None
        assert len(more) == 0

        op, more = ListComparison(tuple(), [1, 2, 3], [1, 2, 3]).compare()
        assert op is None
        assert len(more) == 0

        base = [1, 2, 3]
        op, more = ListComparison(tuple(), base, base).compare()
        assert op is None
        assert len(more) == 0

    def test_ln(self):
        comp = ListComparison(tuple(), [1, 2, 3], [1, 2, 3, 4, 5])
        op, more = comp.compare()
        assert op.path == tuple()
        assert op.ops == {"ln": [4, 5]}
        assert len(more) == 0

    def test_ld(self):
        comp = ListComparison(tuple(), [1, 2, 3, 4, 5], [1, 2, 3])
        op, more = comp.compare()
        assert op.path == tuple()
        assert op.ops == {"ld": 3}
        assert len(more) == 0

    def test_lc(self):
        comp = ListComparison(tuple(), [1, 2, 3, 4, 5], [1, 9, 3, "b", 5])
        op, more = comp.compare()
        assert op.path == tuple()
        assert op.ops == {"lc": [(1, 9), (3, "b")]}
        assert len(more) == 0

    def test_nested_dicts(self):
        # both dicts
        op, more = ListComparison(("x",), ["b", {"x": 1}], ["b", {"x": 2}]).compare()
        assert op is None
        assert more == [DictComparison(("x", 1), {"x": 1}, {"x": 2})]

    def test_nested_dict_and_not(self):
        # one a dict, one not
        op, more = ListComparison(tuple(), ["b", {"x": 1}], ["b", "x"]).compare()
        assert op.path == tuple()
        assert op.ops == {"lc": [(1, "x")]}
        assert len(more) == 0

        # one not a dict, one a dict
        op, more = ListComparison(tuple(), ["b", "x"], ["b", {"x": 1}]).compare()
        assert op.path == tuple()
        assert op.ops == {"lc": [(1, {"x": 1})]}
        assert len(more) == 0

    def test_nested_lists(self):
        # both lists
        op, more = ListComparison(
            ("x",), [1, [9, 8, 7], 2], [1, [9, 8, 6], 2]
        ).compare()
        assert op is None
        assert more == [ListComparison(("x", 1), [9, 8, 7], [9, 8, 6])]

    def test_nested_list_and_not(self):
        # one a list, one not
        op, more = ListComparison(tuple(), ["a", [1, 2, 3]], ["a", "x"]).compare()
        assert op.path == tuple()
        assert op.ops == {"lc": [(1, "x")]}
        assert len(more) == 0

        # one not a list, one a dict
        op, more = ListComparison(tuple(), ["a", "x"], ["a", [1, 2, 3]]).compare()
        assert op.path == tuple()
        assert op.ops == {"lc": [(1, [1, 2, 3])]}
        assert len(more) == 0


# todo: make this a more complete, systematic set of scenarios
patching_scenarios = [
    # a basic example
    ({"x": "4"}, {"x": "5"}),
    ({"x": True}, {"x": 5}),
    # basic lists
    # ld
    ({"x": ["1", "2", "3"]}, {"x": ["1", "5"]}),
    ({"x": [False, 5, "hello"]}, {"x": [True, 5]}),
    # ln
    ({"x": ["1", "2", "3"]}, {"x": ["1", "2", "3", "4"]}),
    ({"x": [1, 2, 3.4]}, {"x": [1, 2, 3.4, 4]}),
    # lc
    ({"x": ["1", "2", "3"]}, {"x": ["1", "5", "3"]}),
    ({"x": ["1", 2, "3"]}, {"x": ["1", 5, "3"]}),
    ({"x": ["1", None, "3"]}, {"x": ["1", False, "3"]}),
    # basic dicts
    ({"x": {"y": "5"}}, {"x": {"y": "6"}}),
    ({"x": {"y": 5}}, {"x": {"y": 6}}),
    # dc
    ({"x": "4"}, {"x": "6"}),
    ({"x": "4"}, {"x": 6}),
    # dn
    ({"x": "4"}, {"x": "6", "y": "10"}),
    ({"x": "4"}, {"x": 6, "y": False}),
    # dd
    ({"x": "4", "y": "10"}, {"x": "4"}),
    ({"x": 4.523, "y": "10"}, {"x": 4.523}),
    # list becomes str
    ({"x": ["1", "2", "3"]}, {"x": "543"}),
    # dict becomes str
    ({"x": {"y": "4"}}, {"x": "543"}),
    # str becomes list
    ({"x": "543"}, {"x": ["1", "2", "3"]}),
    # str becomes dict
    ({"x": "543"}, {"x": {"y": "4"}}),
    # list becomes dict
    ({"x": ["1", "2", "3"]}, {"x": {"y": "1"}}),
    # dict becomes list
    ({"x": {"y": "1"}}, {"x": ["1", "2", "3"]}),
    # dict becomes list in dict
    ({"x": {"y": {"z": "43"}}}, {"x": {"y": ["1", "2", "3"]}}),
    # list of lists
    (
        {"x": [["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]]},
        {"x": [["1", "2", "4"], ["4", "10", "6"], ["0", "8", "9"]]},
    ),
    # list of dicts
    ({"x": [{"y": "5"}, {"y": "7"}]}, {"x": [{"y": "3"}, {"y": "7"}]}),
    # list of dicts with lists and changing types
    (
        {"x": [{"y": ["1", "2", "3"]}, {"y": ["7", "8"]}]},
        {"x": [{"y": "nope"}, {"y": ["3", "8"]}]},
    ),
    # list of lists becomes list of not-lists (and vice versa)
    (
        {"x": [["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]]},
        {"x": ["not", "a", "tuple"]},
    ),
    (
        {"x": ["not", "a", "tuple"]},
        {"x": [["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]]},
    ),
]


class TestPatch:
    def test_empty(self):
        assert patch({"c": "4"}, []) == {"c": "4"}

    @pytest.mark.parametrize(("base", "new"), patching_scenarios)
    def test_patching(self, base: dict, new: dict):
        diff_ops = list(diff(base, new))
        patched_base = patch(base, diff_ops)
        assert patched_base == new
