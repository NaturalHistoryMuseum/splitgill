#!/usr/bin/env python
# encoding: utf-8

from collections import OrderedDict

from mock import MagicMock, call
from six.moves import zip

from eevee.diffing import format_diff, DICT_DIFFER_DIFFER
from eevee.indexing.utils import get_versions_and_data, update_refresh_interval


def test_get_versions_and_data():
    data = OrderedDict([
        (3, {u'a': 20, u'x': 3812, u't': u'llamas'}),
        (5, {u'a': 20, u'x': 4000, u't': u'llamas', u'c': True}),
        (6, {u'a': 23, u'x': 4000, u'c': True}),
        (21, {u'a': 22, u'x': 4002, u't': u'llamas', u'c': False}),
    ])
    mongo_doc = {
        u'versions': list(data.keys()),
        u'diffs': {
            u'3': format_diff(DICT_DIFFER_DIFFER, DICT_DIFFER_DIFFER.diff({}, data[3])),
            u'5': format_diff(DICT_DIFFER_DIFFER, DICT_DIFFER_DIFFER.diff(data[3], data[5])),
            u'6': format_diff(DICT_DIFFER_DIFFER, DICT_DIFFER_DIFFER.diff(data[5], data[6])),
            u'21': format_diff(DICT_DIFFER_DIFFER, DICT_DIFFER_DIFFER.diff(data[6], data[21])),
        }
    }

    next_versions = list(data.keys())[1:] + [float(u'inf')]
    # check all the versions and data values match the test data
    for (rv, rd, rnv), (tv, td), tnv in zip(get_versions_and_data(mongo_doc), data.items(),
                                            next_versions):
        assert rv == tv
        assert rd == td
        assert rnv == tnv


def test_update_refresh_interval():
    # update_refresh_interval(elasticsearch, indexes, refresh_interval)
    mock_elasticsearch_client = MagicMock(indices=MagicMock(put_settings=MagicMock()))
    mock_index_1 = MagicMock()
    mock_index_1.configure_mock(name=u'index_1')
    mock_index_2 = MagicMock()
    mock_index_2.configure_mock(name=u'index_2')

    refresh_interval = 10
    # pass 2 mock_index_2 objects so that we can check the refresh isn't applied multiple times to
    # the same index
    update_refresh_interval(mock_elasticsearch_client, [mock_index_1, mock_index_2, mock_index_2],
                            refresh_interval)

    assert mock_elasticsearch_client.indices.put_settings.call_count == 2
    assert (call({u'index': {u'refresh_interval': refresh_interval}}, mock_index_1.name) in
            mock_elasticsearch_client.indices.put_settings.call_args_list)
    assert (call({u'index': {u'refresh_interval': refresh_interval}}, mock_index_2.name) in
            mock_elasticsearch_client.indices.put_settings.call_args_list)
