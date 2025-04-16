Basic usage example:

```python
from splitgill.manager import SplitgillClient
from splitgill.model import Record
from splitgill.search import term_query

# create these yourself
mongo_client = ...
elasticsearch_client = ...

# create a splitgill client and get a SplitgillDatabase object
client = SplitgillClient(mongo_client, elasticsearch_client)
database = client.get_database("my-database")

# add some data to the database
records = [
    Record("animal-1", {"name": "Jeremy", "animalType": "llama", "height": 40.6}),
    Record("animal-2", {"name": "Paru", "animalType": "cat", "height": 10.3}),
    Record("animal-3", {"name": "Frankie", "animalType": "jaguar", "height": 100}),
    Record("animal-4", {"name": "Doti", "animalType": "cat", "height": 14.3}),
]
result = database.ingest(records)
version_1 = result.version

# index the data into Elasticsearch
database.sync()

# search the data
assert database.search().filter(term_query("animalType", "cat")).count() == 2

# update a record
updated_records = [
    # she is a panther now
    Record("animal-2", {"name": "Paru", "animalType": "Panther", "height": 10.3}),
]
database.ingest(records)
database.sync()

# search the data
assert database.search().filter(term_query("animalType", "cat")).count() == 1

# search the data at the first version
assert database.search(version_1).filter(term_query("animalType", "cat")).count() == 1
```
