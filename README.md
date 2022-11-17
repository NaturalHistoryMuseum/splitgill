<!--header-start-->
# Eevee

[![Tests](https://img.shields.io/github/workflow/status/NaturalHistoryMuseum/eevee/Tests?style=flat-square)](https://github.com/NaturalHistoryMuseum/eevee/actions/workflows/main.yml)
[![Coveralls](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/eevee/master?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/eevee)
[![Python version](https://img.shields.io/badge/python-2.7%20%7C%203.7-blue?style=flat-square)](https://www.python.org/downloads)
[![Docs](https://img.shields.io/readthedocs/eevee?style=flat-square)](https://eevee.readthedocs.io)
<!--header-end-->

## Overview
<!--overview-start-->
Eevee is a library providing base classes with the functionality to create, update and query versioned data. Uses MongoDB and Elasticsearch.

Note that this library is relatively stable but is still quite new and could still be significantly altered.
<!--overview-end-->

## Installation
<!--installation-start-->
Currently, Eevee can only be installed from Github:
```shell
pip install git+git://github.com/NaturalHistoryMuseum/eevee.git#egg=eevee
```
<!--installation-end-->

## Tests
<!--tests-start-->
Make sure you've installed the test requirements into your virtualenv - `pip install .[test]`, then:

 - To run the tests against all python versions this library is compatible with, run `tox`
 - To run the tests against the python version installed in your virtualenv, run `pytest`
 - To run the tests against the python version installed in your virtualenv and get a coverage report too, run `pytest --cov=eevee`
<!--tests-end-->
