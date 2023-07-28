#!/bin/bash

echo "Running pytest with coverage"
pytest --cov="splitgill" tests
test_exit_code=$?

if [ -n "$COVERALLS_REPO_TOKEN" ]; then
  echo "Running coveralls"
  coveralls
else
  echo "Not running coveralls as COVERALLS_REPO_TOKEN isn't set"
fi

exit $test_exit_code
