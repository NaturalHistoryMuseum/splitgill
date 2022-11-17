import pytest

from splitgill.diffing import SHALLOW_DIFFER


class TestShallowDiffer(object):
    def test_can_diff(self):
        assert SHALLOW_DIFFER.can_diff({})
        assert SHALLOW_DIFFER.can_diff({u'a': 4, u'x': [1, 2, 3]})
        assert not SHALLOW_DIFFER.can_diff({u'a': 4, u'x': {u'l': u'beans'}})

    def test_diff(self):
        assert SHALLOW_DIFFER.diff({}, {}) == {}
        assert (
            SHALLOW_DIFFER.diff({u'x': 4, u'y': [1, 2, 3]}, {u'x': 4, u'y': [1, 2, 3]})
            == {}
        )
        assert SHALLOW_DIFFER.diff(
            {u'x': 4, u'y': [1, 2, 3]}, {u'x': 4, u'y': [1, 2, 6]}
        ) == {u'c': {u'y': [1, 2, 6]}}
        assert SHALLOW_DIFFER.diff(
            {u'x': 8, u'y': [1, 2, 3]}, {u'x': u'8', u'y': [1, 2, 3]}
        ) == {u'c': {u'x': u'8'}}
        assert SHALLOW_DIFFER.diff(
            {u'x': 8, u'y': [54, 2, 3]}, {u'x': u'8', u'y': [1, 2, 6]}
        ) == {u'c': {u'x': u'8', u'y': [1, 2, 6]}}
        assert SHALLOW_DIFFER.diff({u'x': 4, u'y': u'beans'}, {u'x': 4}) == {
            u'r': [u'y']
        }
        diff = SHALLOW_DIFFER.diff({u'x': 4, u'y': u'beans'}, {})
        assert isinstance(diff[u'r'], list)
        assert u'x' in diff[u'r']
        assert u'y' in diff[u'r']
        assert SHALLOW_DIFFER.diff({}, {u'x': 4}) == {u'c': {u'x': 4}}
        assert SHALLOW_DIFFER.diff(
            {u'l': u'beans'}, {u'l': u'beans', u'x': 4, u'y': 12}
        ) == {u'c': {u'x': 4, u'y': 12}}
        assert SHALLOW_DIFFER.diff(
            {u'x': 4, u'y': u'beans'}, {u'l': 24246, u'x': 5}
        ) == {u'r': [u'y'], u'c': {u'l': 24246, u'x': 5}}

    def test_patch(self):
        assert SHALLOW_DIFFER.patch({}, {}) == {}
        assert SHALLOW_DIFFER.patch({u'c': {u'x': 4}}, {}) == {u'x': 4}
        assert SHALLOW_DIFFER.patch({u'c': {u'x': 4}}, {u'x': 5}) == {u'x': 4}
        assert SHALLOW_DIFFER.patch({u'c': {u'x': 4}}, {u'y': 5}) == {u'y': 5, u'x': 4}
        assert SHALLOW_DIFFER.patch({u'r': [u'x']}, {u'x': 2380}) == {}
        assert SHALLOW_DIFFER.patch({u'r': [u'x']}, {u'x': 2380, u'y': u'goat'}) == {
            u'y': u'goat'
        }
        with pytest.raises(KeyError):
            assert SHALLOW_DIFFER.patch({u'r': [u'x']}, {})
        assert SHALLOW_DIFFER.patch({u'r': [u'x'], u'c': {u'b': 2}}, {u'x': 2380}) == {
            u'b': 2
        }
