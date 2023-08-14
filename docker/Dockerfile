FROM python:3.8

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    net-tools \
    netcat-openbsd \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# install main dependencies + test dependencies + docs dependencies
COPY pyproject.toml ./pyproject.toml
RUN pip install --no-cache-dir .[test,docs]

COPY . .

CMD [ "bash", "docker/run-tests.sh" ]
