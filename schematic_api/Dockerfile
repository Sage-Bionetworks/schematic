## For aws deployments
FROM python:3.10.9-slim-bullseye

SHELL ["/bin/bash", "-euxo", "pipefail", "-c"]

# set default APP_PORT to 80
# the environment variables defined here are the default
# and can be overwritten by docker run -e VARIABLE = XX
# or can be overwritten by .env when using docker compose
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=200 \
    POETRY_VERSION=1.2.0 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \ 
    APP_PORT=80 \
    APP_DIR=/opt/app \ 
    SERVER_PROTOCOL=http:// \
    SERVER_DOMAIN=localhost \
    SERVER_PORT=7080

# RUN apt-get autoclean && apt-get -y autoremove

RUN apt-get update -qq -y \
    && apt-get install --no-install-recommends -qq -y \
        build-essential \
        gosu \
        libpcre3 \
        libpcre3-dev \
        python3-dev \
        libopenblas-dev \
        gfortran \
    && apt-get -y autoclean \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*


# remove libtiff5 for security reasons
# RUN apt remove -y libtiff5
WORKDIR ${APP_DIR}
RUN chown www-data:www-data /opt/app/

# install poetry
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

# copy relevant files and run poetry install
COPY ./pyproject.toml ./poetry.lock ./uwsgi.ini ./config.yml ./run_api.py ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi --no-root --with aws

# copy schematic_api folder
COPY  schematic_api ./schematic_api

# copy great_expectations folder
COPY great_expectations ./great_expectations
RUN chown -R www-data:www-data /opt/app/great_expectations/

# copy schematic
COPY schematic  ./schematic

# change permission 
WORKDIR /var/www/
RUN chown www-data:www-data /var/www/

WORKDIR /
COPY schematic_api/docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

EXPOSE 7080

ENTRYPOINT ["/docker-entrypoint.sh"]

# Run server in production mode
CMD ["uwsgi", "--ini", "uwsgi.ini", "--lazy", "--http", ":7080"]