# FROM python:3.10.6
FROM python:3.10.8-slim-bullseye

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=200 \
  POETRY_VERSION=1.3.0

WORKDIR /usr/src/app

RUN apt-get update -qqy \
  && apt-get install -qqy \
  libopenblas-dev \
  gfortran 

# remove libtiff5 for security reasons
RUN apt remove -y libtiff5

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false

RUN poetry install --no-interaction --no-ansi --no-root

COPY . ./

RUN poetry install --only-root