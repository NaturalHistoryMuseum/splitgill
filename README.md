<!--header-start-->

# splitgill

[![Tests](https://img.shields.io/github/actions/workflow/status/NaturalHistoryMuseum/splitgill/tests.yml?branch=main&style=flat-square)](https://github.com/NaturalHistoryMuseum/splitgill/actions/workflows/tests.yml)
[![Coveralls](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/splitgill/main?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/splitgill)
[![Python version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue?style=flat-square)](https://www.python.org/downloads)
[![Docs](https://img.shields.io/readthedocs/splitgill?style=flat-square)](https://splitgill.readthedocs.io)
<!--header-end-->

## Overview

<!--overview-start-->
Splitgill is a library providing functionality to create, update, and query versioned
data.
Uses MongoDB and Elasticsearch.

Note that this library is relatively stable but is still quite new and could still be
significantly altered.

### Split-gill mushroom

The split-gill mushroom is a fungus with
a [very high level of genetic diversity](https://doi.org/10.1093/molbev/msv153) (a lot
of _versions_, you could say).

<!--overview-end-->

## Installation

<!--installation-start-->
splitgill can be installed from PyPI:

```shell
pip install splitgill
```

Or from Github:

```shell
pip install git+git://github.com/NaturalHistoryMuseum/splitgill.git#egg=splitgill
```

Splitgill requires:

- MongoDB >= version 6
- Elasticsearch >= version 8

This library has not been tested across many MongoDB and Elasticsearch versions, your
mileage may vary, and it'd be worth running the test suite against the versions you're
targeting before using this library in earnest.

<!--installation-end-->

## Tests

<!--tests-start-->
Tests are run through docker-compose so that MongoDB and Elasticsearch are available for
real testing.

To run the tests:

```bash
docker compose build
docker compose run test
```

<!--tests-end-->
