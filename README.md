# Eevee

[![Travis](https://img.shields.io/travis/NaturalHistoryMuseum/eevee/master.svg?style=flat-square)](https://travis-ci.org/NaturalHistoryMuseum/eevee)
[![Coveralls github](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/eevee/master.svg?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/eevee)
[![Python version](https://img.shields.io/badge/python-2.7%20%7C%203.7-blue?style=flat-square)](https://www.python.org/downloads/)

A library providing base classes with the functionality to create, update and query versioned data. Uses MongoDB and Elasticsearch.

Note that this library is relatively stable but is still quite new and could still be significantly altered.
A stable version 1.0 will be released soon!

### Running the tests

Make sure you've installed the test requirements into your virtualenv - `pip install .[test]`, then:

 - To run the tests against all python versions this library is compatible with, run `tox`
 - To run the tests against the python version installed in your virtualenv, run `pytest`
 - To run the tests against the python version installed in your virtualenv and get a coverage report too, run `pytest --cov=eevee`
