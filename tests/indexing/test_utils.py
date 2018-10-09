#!/usr/bin/env python
# encoding: utf-8

from collections import OrderedDict

import dictdiffer
import six

from eevee.indexing.utils import get_versions_and_data
from eevee.utils import serialise_diff

if six.PY2:
    # the builtin version of zip in python 2 returns a list, we need an iterator so we have to use
    # the itertools version
    from itertools import izip as zip


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
            u'3': serialise_diff(list(dictdiffer.diff({}, data[3]))),
            u'5': serialise_diff(list(dictdiffer.diff(data[3], data[5]))),
            u'6': serialise_diff(list(dictdiffer.diff(data[5], data[6]))),
            u'21': serialise_diff(list(dictdiffer.diff(data[6], data[21]))),
        }
    }

    next_versions = list(data.keys())[1:] + [float(u'inf')]
    # check all the versions and data values match the test data
    for (rv, rd, rnv), (tv, td), tnv in zip(get_versions_and_data(mongo_doc), data.items(),
                                            next_versions):
        assert rv == tv
        assert rd == td
        assert rnv == tnv
