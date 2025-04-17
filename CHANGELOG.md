## v3.0.1 (2025-04-17)

### Fix

- update version number to allow bump to work

## v3.0.0 (2025-04-17)

### Breaking Changes

- give arc documents an auto id and proactively delete on resync
- implement a series of arc indices instead of a fixed number
- combine the parsed and data root fields into one structure in Elasticsearch
- ensure the _id data field is added before ingestion
- store parsed keyword and text fields
- remove case sensitive keyword parsed type
- switch caret in field names to underscore
- change how data and parsed fields metadata is indexed
- remove default config values
- rework our index doc structure to better accommodate geo + many knock on effects
- change client's get_database method name to get_mongo_database
- introduce an object for managing index names
- rename add to ingest
- switch the diffing from using tuples to lists
- change how uncommitted record data is handled
- change the prepare function to produce other simple types beyond str
- upgrade to Elasticsearch 8
- properly define error sync scenario and add refresh interval/replica optimisations
- redefine the ingesting and model code, with tests

### Feature

- add id_query to search module
- give arc documents an auto id and proactively delete on resync
- sleep after refresh failure with increasing backoff
- use best_compression in both templates
- implement a series of arc indices instead of a fixed number
- combine the parsed and data root fields into one structure in Elasticsearch
- ensure the _id data field is added before ingestion
- store parsed keyword and text fields
- remove case sensitive keyword parsed type
- add changed counts method with refactor
- use best_compression codec by default
- enhance field name cleaning
- allow mongo database name customisation
- optimise search creation when version number >= latest version
- add get_rounded_version method to manager for version rounding
- add a has_geo helper function to the search module
- add range query builder to search module
- allow to_timestamp to receive date objects
- add methods to the ParsingOptionsBuilder to clear out and reset date formats
- check for rubbish wkt candidates before we pass to from_wkt
- make quad_segs circle creation option available as a parsing option per hint
- remove default config values
- rework our index doc structure to better accommodate geo + many knock on effects
- add access to all profiles easily from the database
- bundle the bulk ops sync options into an object
- introduce our own elasticsearch bulk op implementation
- lock during database commit
- add lock manager creation to SplitgillClient
- allow the storage of additional data with the lock metadata
- add a locking module to provide machine independent locking functionality using mongo
- add a resync parameter for full reloads
- add a way of getting a SplitgillDatabase object from the SplitgillClient
- clean field names as they enter the system
- add version parameter to search helper method
- add return from rollback_options to indicate how many options were removed
- shortcut inserting records that are new for speed
- make ingest find size an optional paramater
- add a stats return object for adding data
- add a modified field option that can be ignored during mongo diff adds
- reinstate source filtering
- refactor field definitions and add additional parsing options
- add way of updating profiles through database object
- add convenience functions for getting value/parent fields from profiles
- include field information about parent fields
- add a cached profile for each version of a database
- add search helpers for paths and version checks
- add the meta.geo field back in, populated with all other record geo values
- change how uncommitted record data is handled
- bring the config updates into the commit system with data
- add parsing configs for versioned control
- change the prepare function to produce other simple types beyond str
- remove unicode control characters from strings before ingesting them
- parse more kinds of strings to dates
- change the date field to use epoch_millis
- adds a case-sensitive keyword field to the model
- upgrade to Elasticsearch 8
- properly define error sync scenario and add refresh interval/replica optimisations
- add a function that creates a database's wildcard elasticsearch index matcher
- add an option for single threaded elasticsearch sync
- allow adding to the GeoFieldHints object but keep it immutable
- reorder data index names
- add a way to get the latest elasticsearch data version
- add manager sync function to get mongo data to elasticsearch
- add bulk index op generating code and tests
- add index parsing code
- remove all the old indexing code
- add field name definitions
- remove set_status and use commit only for updating m_version status
- update the pyproject.toml definition
- redefine the ingesting and model code, with tests

### Fix

- accommodate None values in lists during data rebuild
- avoid error deleting missing arc-0
- add retry to refresh during index sync
- switch caret in field names to underscore
- use modified_count not upserted_count for bulk ingest counts
- change how data and parsed fields metadata is indexed
- fix typing annotation for prepare_data function
- fixes get_versions bug where versions containing only deletes were missed
- also inspect the document's next field when getting the current elasticsearch version
- fix date and datetime management
- handle 3d wkt/geojson properly
- use match_pattern=simple to avoid warnings about possible regexes
- change keyword length ranges to be valid
- use a non-naive datetime for now()
- sort out imports that aren't full
- fix up how indexing ops are generated so that they take into account options
- ensure ingest batch size is the same as the generation batch size
- stop creating new versions of records that are the same but have lists
- fix since last index comparisons
- fix how geo.* paths are formed
- define a mapping for the profiles index
- allow the profiles index to use many more fields
- increase the default field limit
- only create the profiles index when we need it
- ensure bools are not passed as ints
- fix import path
- cache parsing results by type
- switch the diffing from using tuples to lists
- add prepare_data to patching and refactor
- allow lists in parse_for_index
- fix major logic issues with the builder
- ensure we catch all kinds of date parsing errors that could be thrown
- change the number mapping from float to double
- stop ingesting deletes for non-existent records into mongo
- ensure root field usage is consistent in generated field paths
- fix test_manager.py imports
- change the MetaField enum to not use full paths as values
- fix tuple <-> dict value change diffing
- default MongoRecord.diffs correctly
- use keyword id and values from enums
- test and fix bugs in set/get status
- use replace_one instead of update_one to update status

### Refactor

- rename version filter shortcuts to remove create prefix
- use parse_to_timestamp instead of repeating that code in the parser
- extract ParsedType inference from term_query to allow reuse
- provide defaults for the ParsingOptionsBuilder so that it can be built with no params legally
- streamline the search method's parameters
- change client's get_database method name to get_mongo_database
- introduce an object for managing index names
- clean up docs and code around the start version of index op generation
- rename add test to ingest
- rename add to ingest
- rename the has_version function to something more semantic
- move the counting to the AddResult class
- remove old print debug horrors
- remove direct get_fields function, just use get_profile().fields
- refactor some internal typing to make it easier to read the code
- rename config collection options
- remove custom hash function for GeoFieldHint and use dataclass's inbuilt one + test
- make the versions and values attributes available in the ParsingOptionsRange object
- move the bool constant string values to the module level for others to use
- remove unused import
- rename a variable to avoid rename issues and add test just in case
- remove commented out unused code
- change GeoFieldHint lists to a class container
- remove config index/collection name definitions
- move get_version from ingest module into manager directly
- rename test class after data -> committed rename
- rename database data_version to committed_version for clarity
- use the type specific path forming functions instead of parsed_path generic function
- use a StrEnum lib to make working with fields easier
- rename the MongoRecord.__iter__ method to iter
- rename connection -> client
- convert database property into method
- rename SplitgillConnection to SplitgillClient and add doc

### Docs

- update elasticsearch model docs
- add basic usage example
- update docs significantly
- add some additional informatino about parsing radius values to hint builder
- update comment
- update docs after changing dates and keywords
- remove config section from docs
- update docs to be in line with float -> double model change
- update branch in coveralls branch
- add main doc to SplitgillDatabase class
- update python versions in readme
- add doc to partition function
- update test running doc in readme
- add documentation about how Splitgill will work in v3
- fix tests badge

### Style

- reformat readme

### Tests

- fix out of date template test
- add a database fixture
- update test to use new to_timestamp date taking abilities
- fix options builder date format test
- fix imports again
- add an explicit test for datetime and date complete flow
- remove unnecessary prepare_data call in parser tests
- allow using envvars to override default mongo & es hosts
- fix import issues again
- fix test import
- fix more test import paths
- fix importing issues for tests
- rename the data_collection fixture to mongo_collection to make it reusable
- refactor some tests to use the SplitgillDatabase.search method
- add options collection test
- fix tests
- add a test specifically for same record ingests
- add a test for parsing a list of dicts
- rename test to despecify it
- rename test to match previous changes
- add a test for the unicode cleaner
- add a couple of additional test cases to the prepare tests
- make sure polygons are closed
- cover off an edge case in generate_index_ops
- add tests for type path generation
- add a test for non-container to container diffs and vice versa
- add typing to queue in diff function
- add some more specific tests to make sure all container changes are covered
- add prepare fallback test
- add comment to explain netcat install in Dockerfile
- wait for es and mongo to start before launching tests
- add extra elasticsearch config option to avoid warnings in tests
- add elasticsearch clean up to the elasticsearch fixture
- add tests for the add method
- add tests for next version determination
- add tests for SplitgillDatabase.data_version
- remove tox config
- fix running tests through github actions
- update .coveragerc to work

### Build System(s)

- update project dependencies
- swap docker-compose for docker compose
- remove duplicate dependency
- switch pytest-asyncio into auto mode and remove now unnecessary decorators
- pass elasticsearch config as env vars
- change name of main testing service from sg to test
- add docs requirements to pyproject.toml
- add dateutil to dependencies
- add service to docker-compose.yaml to serve the docs locally
- add docker-compose config for running tests

### CI System(s)

- update pypy publish workflow
- rename main.yml -> tests.yml
- fix bump workflow
- update pre commit
- upgrade checkout action to v4
- add coverage to the actions test run
- **pre-commit**: add cz-nhm as a dependency

### Chores/Misc

- indicate that bulk options can be optional in typing
- add clear methods for other parsing builder parts
- add a convenient way of accessing all Splitgill's used date formats
- fix typing in search helpers
- specify that we want to ignore geo z values
- remove some commented out template ideas
- use full=True across all parsed dynamic template for consistency
- make all todo comments lowercase
- be specific about the search version
- remove some really unnecessary comments
- remove now unecessary field enlarging
- remove unused import from diffing module
- add a todo about a diff shortcut
- switch to pyproject completely
- **commitizen**: replace cz_customize with cz_nhm

## v2.0.0 (2022-11-17)

### Breaking changes

-  "eevee" was taken on pypi

### Build

- **requirements**: add coveralls to requirements

### CI

- fix commitizen config so it bumps correctly
- install requirements from .txt in actions
- add github actions

### Docs

- add instructions for installation from pypi
- add section explaining the name
- add installation section, separate sections in docs
- include README content in docs
- attempting to symlink readme
- replace module name
- switch to mkdocs, add RTD config

### Misc

- add commitizen and pre-commit
- switch to pyproject.toml
- rename license
- remove travis
- ignore egg

### Refactor

- **name**: change name from eevee to splitgill

### Style

- apply formatting

### Tests

- **versions**: add python 3.8 and 3.9

## v1.2.3 (2021-01-04)

## v1.2.2 (2020-11-17)

## v1.2.1 (2019-11-21)

## v1.2.0 (2019-11-13)

## v1.1.1 (2019-10-03)

## v1.1.0 (2019-08-29)

## v1.0.3 (2019-08-28)

## v1.0.2 (2019-08-14)

## v1.0.1 (2019-08-14)

## v1.0.0 (2019-08-12)
