# Versions

[![Travis](https://img.shields.io/travis/NaturalHistoryMuseum/versions.svg?style=flat-square)](https://travis-ci.org/NaturalHistoryMuseum/versions)
[![Coveralls github](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/versions.svg?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/versions)

A library providing base classes with the functionality to create, update and query versioned data. Uses MongoDB and Elasticsearch.

**This project is currently under active development.**


### Running the tests

Make sure you've installed the test requirements into your virtualenv - `pip install -r tests/requirements.txt`, then:

 - To run the tests against all python versions this library is compatible with, run `tox`
 - To run the tests against the python version installed in your virtualenv, run `pytest`
 - To run the tests against the python version installed in your virtualenv and get a coverage report too, run `pytest --cov=versions`
