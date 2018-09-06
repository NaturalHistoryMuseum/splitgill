import math as maths
from collections import OrderedDict

import dictdiffer

from eevee.indexing.utils import get_versions_and_data
from eevee.utils import serialise_diff


def test_get_versions_and_data():
    data = OrderedDict([
        (3, {'a': 20, 'x': 3812, 't': 'llamas'}),
        (5, {'a': 20, 'x': 4000, 't': 'llamas', 'c': True}),
        (6, {'a': 23, 'x': 4000, 'c': True}),
        (21, {'a': 22, 'x': 4002, 't': 'llamas', 'c': False}),
    ])
    mongo_doc = {
        'versions': sorted(data.keys()),
        'diffs': {
            '3': serialise_diff(list(dictdiffer.diff({}, data[3]))),
            '5': serialise_diff(list(dictdiffer.diff(data[3], data[5]))),
            '6': serialise_diff(list(dictdiffer.diff(data[5], data[6]))),
            '21': serialise_diff(list(dictdiffer.diff(data[6], data[21]))),
        }
    }

    next_versions = list(data.keys())[1:] + [maths.inf]
    # check all the versions and data values match the test data
    for (rv, rd, rnv), (tv, td), tnv in zip(get_versions_and_data(mongo_doc), data.items(), next_versions):
        assert rv == tv
        assert rd == td
        assert rnv == tnv
