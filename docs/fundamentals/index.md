Splitgill is a versioned storage and search library.
It is a management layer that sits on top of MongoDB and Elasticsearch to store data
and track the versions of that data.

MongoDB provides the persistent, versioned storage, while Elasticsearch provides a
searchable view on that versioned data.

A number of considerations are addressed by the Splitgill data model:

  - the latest data needs to be searchable
  - all versions of the data need to be searchable at any specific version
  - as many of the powerful search features in ElasticSearch need to be available for
    users as possible
  - changes in data type between versions of a field need to be supported
  - the model should support efficient type querying to assist with searches
  - the model should support all JSON types nested at any level
    - string
    - number
    - object
    - array
    - boolean
    - null
