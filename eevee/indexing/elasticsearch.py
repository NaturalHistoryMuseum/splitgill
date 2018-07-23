import json
from datetime import datetime

import requests

from eevee import utils


def non_standard_type_converter(data):
    """
    Handles non-standard types that the default json dumper can't handle. Anything that is not handled in this function
    throws a TypeError. Currently this function only handles datetime objects which are converted into ISO format and
    returned.

    :param data:    the data to handle
    :return: the converted data
    """
    if isinstance(data, datetime):
        # elasticsearch by default can read the ISO format for dates so this is a decent default
        return data.isoformat()
    raise TypeError(f'Unable to serialize {repr(data)} (type: {type(data)})')


def serialise_for_elasticsearch(data):
    """
    Helper that serialises the given dict into a json string ensuring that datetime objects are serialised into the ISO
    format which can be read into Elasticsearch as a date type by default.

    :param data:    a dict
    :return: a json string
    """
    return json.dumps(data, default=non_standard_type_converter)


def send_mapping(config, index_name, mapping):
    """
    Send the mapping for the given index to elasticsearch.

    :param config: the config object
    :param index_name: the name of the index that the mapping defines
    :param mapping: the mapping definition
    :return: the response from elasticsearch
    """
    response = requests.put(f'{config.elasticsearch_url}/{index_name}', json=mapping)
    return response


def send_aliases(config, alias_operations):
    """
    Send the set of alias operations to elasticsearch.

    :param config: the config object
    :param alias_operations: the alias operations
    :return: the response from elasticsearch
    """
    response = requests.post(f'{config.elasticsearch_url}/_aliases', json=alias_operations)
    return response


def send_bulk_index(config, commands):
    """
    Send the bulk commands to elasticsearch and yield each response as we go.

    :param config: the config object
    :param commands: the bulk commands
    :return: yields the response objects after each is sent
    """
    for chunk in utils.chunk_iterator(commands, chunk_size=1000):
        # use the special content-type required by elasticsearch
        headers = {'Content-Type': 'application/x-ndjson'}
        # format the commands as required by elasticsearch
        data = '\n'.join(map(serialise_for_elasticsearch, chunk))
        # there must be a new line at the end of the command list
        data += '\n'
        # send the commands to elasticsearch
        response = requests.post(f'{config.elasticsearch_url}/_bulk', headers=headers, data=data)
        yield response
