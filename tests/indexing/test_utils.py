import types
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

    generator = get_versions_and_data(mongo_doc)
    # check it's a generator, this is important because this function is designed to be used lazily and therefore if it
    # was changed to not be a lazy generator it could have a knock-on performance impact
    assert isinstance(generator, types.GeneratorType)

    # check all the versions and data values match the test data
    for (result_version, result_data), (test_version, test_data) in zip(generator, data.items()):
        assert result_version == test_version
        assert result_data == test_data
