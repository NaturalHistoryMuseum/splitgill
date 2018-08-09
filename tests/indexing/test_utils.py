from collections import OrderedDict

import dictdiffer

from eevee.indexing.utils import get_versions_and_data


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
            '3': list(dictdiffer.diff({}, data[3])),
            '5': list(dictdiffer.diff(data[3], data[5])),
            '6': list(dictdiffer.diff(data[5], data[6])),
            '21': list(dictdiffer.diff(data[6], data[21])),
        }
    }

    # check all the versions and data values match the test data
    for (result_version, result_data), (test_version, test_data) in zip(get_versions_and_data(mongo_doc), data.items()):
        assert result_version == test_version
        assert result_data == test_data
