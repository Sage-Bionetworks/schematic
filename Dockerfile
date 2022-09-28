FROM python:3.10.6

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=200 \
  POETRY_VERSION=1.2.0rc1

WORKDIR /usr/src/app

RUN apt-get update -qqy \
  && apt-get install -qqy \
    libopenblas-dev \
    gfortran

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false

RUN poetry install --no-interaction --no-ansi --no-root

COPY . ./

RUN poetry install --no-interaction --no-ansi --only-root
