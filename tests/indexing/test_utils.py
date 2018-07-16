import dictdiffer

from eevee.indexing.utils import get_data_at_version


def test_get_data_at_version():
    data = {
        '3': {'a': 20, 'x': 3812, 't': 'llamas'},
        '5': {'a': 20, 'x': 4000, 't': 'llamas', 'c': True},
        '6': {'a': 23, 'x': 4000, 'c': True},
        '21': {'a': 22, 'x': 4002, 't': 'llamas', 'c': False},
    }
    mongo_doc = {
        'versions': ['3', '5', '6', '21'],
        'diffs': {
            '3': list(dictdiffer.diff({}, data['3'])),
            '5': list(dictdiffer.diff(data['3'], data['5'])),
            '6': list(dictdiffer.diff(data['5'], data['6'])),
            '21': list(dictdiffer.diff(data['6'], data['21'])),
        }
    }

    assert get_data_at_version(mongo_doc, '0') == {}
    assert get_data_at_version(mongo_doc, '2') == {}
    assert get_data_at_version(mongo_doc, '3') == data['3']
    assert get_data_at_version(mongo_doc, '4') == data['3']
    assert get_data_at_version(mongo_doc, '5') == data['5']
    assert get_data_at_version(mongo_doc, '6') == data['6']
    assert get_data_at_version(mongo_doc, '10') == data['6']
    assert get_data_at_version(mongo_doc, '20') == data['6']
    assert get_data_at_version(mongo_doc, '21') == data['21']
    assert get_data_at_version(mongo_doc, '22') == data['21']
    assert get_data_at_version(mongo_doc, '300') == data['21']
