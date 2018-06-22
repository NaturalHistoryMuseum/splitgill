import requests


class Searcher(object):

    def __init__(self, config):
        self.config = config

    def pre_process(self, index, search, version):
        if search is None:
            search = {
                'from': self.config.search_from,
                'size': self.config.search_size,
                'query': {}
            }
        if not version:
            index = f'{self.config.elasticsearch_current_alias_prefix}{index}'
        return index, search, version

    def post_process(self, index, search, version, response, raise_error=True):
        if raise_error:
            response.raise_for_status()
        return response.json()

    def search(self, index, search=None, version=None):
        index, search, version = self.pre_process(index, search, version)
        response = requests.post(f'{self.config.elasticsearch_url}/{index}/_search', json=search)
        return self.post_process(index, search, version, response)
