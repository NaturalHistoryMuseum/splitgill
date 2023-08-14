#!/bin/bash

# wait for MongoDB to get going
while ! nc -z mongo 27017; do
  sleep 1
done

# wait for Elasticsearch to get going
while ! curl -s "http://es:9200/_cluster/health" | grep -q green; do
  sleep 1
done

# run the tests
echo "Running pytest with coverage"
pytest --cov="splitgill" tests
# store the exit code from the pytest run
test_exit_code=$?

# run coveralls if needed
if [ -n "$COVERALLS_REPO_TOKEN" ]; then
  echo "Running coveralls"
  coveralls
else
  echo "Not running coveralls as COVERALLS_REPO_TOKEN isn't set"
fi

# return the pytest exit code
exit $test_exit_code
