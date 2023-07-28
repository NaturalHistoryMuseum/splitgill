FROM python:3.8

WORKDIR /usr/src/app

# install main dependencies + test dependencies
COPY pyproject.toml ./pyproject.toml
RUN pip install --no-cache-dir .[test]

# install doc dependencies
COPY docs/requirements.txt ./docs/requirements.txt
RUN pip install -r ./docs/requirements.txt

COPY . .

CMD [ "bash", "docker/run-tests.sh" ]
