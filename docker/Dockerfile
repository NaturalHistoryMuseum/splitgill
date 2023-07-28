FROM python:3.8

WORKDIR /usr/src/app

# install main dependencies + test dependencies + docs dependencies
COPY pyproject.toml ./pyproject.toml
RUN pip install --no-cache-dir .[test,docs]

COPY . .

CMD [ "bash", "docker/run-tests.sh" ]
