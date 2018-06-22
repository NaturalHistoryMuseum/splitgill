

class MongoToElasticsearchConverter(object):
    """
    This class is not a subclass of Versioned because a different version is passed through with each call rather than
    the same version being used throughout the objects life.
    """

    def __init__(self, elasticsearch_index):
        self.elasticsearch_index = elasticsearch_index

    def prepare(self, record_id, data, version=None, next_version=None):
        return self.create_action(record_id, version), self.create_index_document(data, version, next_version)

    def create_action(self, record_id, version):
        return {
            'index': {
                '_id': record_id if not version else f'{record_id}:{version.strftime("%Y%m%d")}',
                '_type': '_doc',
                '_index': self.elasticsearch_index,
            }
        }

    def create_index_document(self, data, version, next_version):
        # TODO: decide if this is the best way to structure the index document
        return {
            'meta': self.create_metadata(version, next_version),
            'data': data
        }

    def create_metadata(self, version, next_version=None):
        metadata = {
            'versions': {
                'gte': version,
            },
            'version': version,
        }
        if next_version:
            metadata['versions']['lt'] = next_version
            metadata['next_version'] = next_version
        return metadata
