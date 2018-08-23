from unittest.mock import Mock

from eevee.indexing.utils import get_version_condition
from eevee.search.search import Searcher


class TestPreProcess:

    def test_no_indexes(self):
        indexes = None
        body = {}
        version = 1
        searcher = Searcher(Mock(search_default_indexes=['a', 'b']))
        processed_indexes, _body, _version = searcher.pre_process(indexes, body, version)
        assert processed_indexes == ['a', 'b']

    def test_indexes(self):
        indexes = ['a', 'b', 'c']
        body = {}
        version = 1
        searcher = Searcher(Mock())
        processed_indexes, _body, _version = searcher.pre_process(indexes, body, version)
        assert processed_indexes == ['a', 'b', 'c']

    def test_no_body(self):
        indexes = ['a', 'b']
        body = None
        # give no version to avoid the body being modified
        version = None
        searcher = Searcher(Mock(search_from=0, search_size=14))
        _indexes, processed_body, _version = searcher.pre_process(indexes, body, version)
        assert processed_body == {
            'from': 0,
            'size': 14,
            'query': {},
        }

    def test_body(self):
        indexes = ['a', 'b']
        body = {'a': 4}
        # give no version to avoid the body being modified
        version = None
        searcher = Searcher(Mock(search_from=0, search_size=14))
        _indexes, processed_body, _version = searcher.pre_process(indexes, body, version)
        assert processed_body == {'a': 4}

    def test_version_no_body(self):
        indexes = ['a', 'b', 'c']
        body = None
        version = 1
        searcher = Searcher(Mock(search_from=0, search_size=14))
        _indexes, processed_body, processed_version = searcher.pre_process(indexes, body, version)
        assert processed_body == {
            'from': 0,
            'size': 14,
            'query': {
                'bool': {
                    'filter': [
                        get_version_condition(1)
                    ]
                }
            },
        }
        assert processed_version == 1

    def test_version_with_body_query(self):
        indexes = ['a', 'b', 'c']
        body = None
        version = 1
        searcher = Searcher(Mock(search_from=0, search_size=14))
        _indexes, processed_body, processed_version = searcher.pre_process(indexes, body, version)
        assert processed_body == {
            'from': 0,
            'size': 14,
            'query': {
                'bool': {
                    'filter': [
                        get_version_condition(1)
                    ]
                }
            },
        }
        assert processed_version == 1
