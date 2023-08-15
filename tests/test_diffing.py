from datetime import datetime

import pytest

from splitgill.diffing import (
    prepare,
    diff,
    DiffOp,
    DiffingTypeComparisonException,
    patch,
)


class TestPrepare:
    def test_none(self):
        assert prepare(None) is None

    def test_str(self):
        assert prepare("beans") == "beans"

    def test_numbers(self):
        assert prepare(23) == "23"
        assert prepare(23.456) == "23.456"
        assert prepare(-20.5012) == "-20.5012"
        assert prepare(complex(3, 4)) == "(3+4j)"
        assert prepare(0) == "0"

    def test_bool(self):
        assert prepare(True) == "true"
        assert prepare(False) == "false"

    def test_datetime(self):
        now = datetime.now()
        assert prepare(now) == now.isoformat()

    def test_dict(self):
        assert prepare({}) == {}
        assert prepare({"x": "beans"}) == {"x": "beans"}
        assert prepare({"x": "4"}) == {"x": prepare(4)}
        assert prepare({3: True}) == {prepare(3): prepare(True)}
        assert prepare({4: {6: 1}}) == {prepare(4): {prepare(6): prepare(1)}}

    def test_list(self):
        assert prepare([]) == tuple()
        assert prepare([1, 2, 3]) == ("1", "2", "3")
        assert prepare([1, True, 3]) == ("1", "true", "3")

    def test_set(self):
        assert prepare(set()) == tuple()

        prepared = prepare({1, 2, 3, "beans"})
        assert isinstance(prepared, tuple)
        assert "1" in prepared
        assert "2" in prepared
        assert "3" in prepared
        assert "beans" in prepared

    def test_tuple(self):
        assert prepare(tuple()) == tuple()
        assert prepare((1, 2, 3)) == ("1", "2", "3")
        assert prepare((1, True, 3)) == ("1", "true", "3")

    def test_fallback(self):
        class A:
            def __str__(self):
                return "beans"

        assert prepare(A()) == "beans"

    def test_mix(self):
        now = datetime.now()
        prepared = prepare(
            {
                "x": "4",
                "y": True,
                "z": [1, 2, 3],
                "a": {
                    "x": [4, 20.7],
                    "y": now,
                },
                "b": [{"x": 1}, {"x": "4.2"}],
            }
        )
        assert prepared == {
            "x": "4",
            "y": "true",
            "z": ("1", "2", "3"),
            "a": {
                "x": ("4", "20.7"),
                "y": now.isoformat(),
            },
            "b": ({"x": "1"}, {"x": "4.2"}),
        }


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

    def test_tuple_new(self):
        base = {"a": ("1", "2", "3")}
        new = {"a": ("1", "2", "3", "4", "5")}
        assert list(diff(base, new)) == [DiffOp(("a",), {"tn": ("4", "5")})]

    def test_tuple_delete(self):
        base = {"a": ("1", "2", "3", "4", "5")}
        new = {"a": ("1", "2", "3")}
        assert list(diff(base, new)) == [DiffOp(("a",), {"td": 3})]

    def test_tuple_change(self):
        base = {"a": ("1", "2", "3", "4", "5")}
        new = {"a": ("1", "2", "3", "10", "5")}
        assert list(diff(base, new)) == [DiffOp(("a",), {"tc": [(3, "10")]})]

    def test_tuple_with_embeds(self):
        base = {
            "a": ({"y": "4"}, {"z": "5"}),
            "b": (("1", "2", "3"), ("4", "5", "6"), ("7", "8", "9")),
        }
        new = {
            "a": ({"y": "4"}, {"z": "3"}),
            "b": (("1", "10", "3"), ("4", "5", "6"), ("4", "8", "9")),
        }
        assert list(diff(base, new)) == [
            DiffOp(
                path=("b",), ops={"tc": [(0, ("1", "10", "3")), (2, ("4", "8", "9"))]}
            ),
            DiffOp(path=("a", 1), ops={"dc": {"z": "3"}}),
        ]

    def test_dict_embeds(self):
        base = {"a": {"b": "5", "c": "6"}}
        new = {"a": {"a": "2", "c": "4"}}
        assert list(diff(base, new)) == [
            DiffOp(path=("a",), ops={"dn": {"a": "2"}, "dd": ["b"], "dc": {"c": "4"}})
        ]


# TODO: make this a more complete, systematic set of scenarios
patching_scenarios = [
    # a basic example
    ({"x": "4"}, {"x": "5"}),
    # basic tuples
    # td
    ({"x": ("1", "2", "3")}, {"x": ("1", "5")}),
    # tn
    ({"x": ("1", "2", "3")}, {"x": ("1", "2", "3", "4")}),
    # tc
    ({"x": ("1", "2", "3")}, {"x": ("1", "5", "3")}),
    # basic dicts
    ({"x": {"y": "5"}}, {"x": {"y": "6"}}),
    # dc
    ({"x": "4"}, {"x": "6"}),
    # dn
    ({"x": "4"}, {"x": "6", "y": "10"}),
    # dd
    ({"x": "4", "y": "10"}, {"x": "4"}),
    # tuple becomes str
    ({"x": ("1", "2", "3")}, {"x": "543"}),
    # dict becomes str
    ({"x": {"y": "4"}}, {"x": "543"}),
    # str becomes tuple
    ({"x": "543"}, {"x": ("1", "2", "3")}),
    # str becomes dict
    ({"x": "543"}, {"x": {"y": "4"}}),
    # tuple becomes dict
    ({"x": ("1", "2", "3")}, {"x": {"y": "1"}}),
    # dict becomes tuple
    ({"x": {"y": "1"}}, {"x": ("1", "2", "3")}),
    # dict becomes tuple in dict
    ({"x": {"y": {"z": "43"}}}, {"x": {"y": ("1", "2", "3")}}),
    # tuple of tuples
    (
        {"x": (("1", "2", "3"), ("4", "5", "6"), ("7", "8", "9"))},
        {"x": (("1", "2", "4"), ("4", "10", "6"), ("0", "8", "9"))},
    ),
    # tuple of dicts
    ({"x": ({"y": "5"}, {"y": "7"})}, {"x": ({"y": "3"}, {"y": "7"})}),
    # tuple of dicts with tuples and changing types
    (
        {"x": ({"y": ("1", "2", "3")}, {"y": ("7", "8")})},
        {"x": ({"y": "nope"}, {"y": ("3", "8")})},
    ),
    # tuple of tuples becomes tuple of not-tuples (and vice versa)
    (
        {"x": (("1", "2", "3"), ("4", "5", "6"), ("7", "8", "9"))},
        {"x": ("not", "a", "tuple")},
    ),
    (
        {"x": ("not", "a", "tuple")},
        {"x": (("1", "2", "3"), ("4", "5", "6"), ("7", "8", "9"))},
    ),
]


class TestPatch:
    def test_empty(self):
        assert patch({"c": "4"}, []) == {"c": "4"}

    def test_always_a_new_dict(self):
        base = {"c": "4"}
        assert patch(base, []) is not base
        assert patch(base, list(diff(base, {"c": "5"}))) is not base

    @pytest.mark.parametrize(("base", "new"), patching_scenarios)
    def test_patching(self, base: dict, new: dict):
        diff_ops = list(diff(base, new))
        patched_base = patch(base, diff_ops)
        assert patched_base == new
