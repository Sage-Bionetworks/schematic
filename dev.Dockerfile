## For aws deployments
FROM python:3.10.9-slim-bullseye

# set APP_PORT to 80 to avoid 308 unhealthy target group error
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=200 \
    POETRY_VERSION=1.2.0 \
    APP_PORT=80 

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

CMD ["python", "/usr/src/app/run_api.py"]