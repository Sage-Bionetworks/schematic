FROM tiangolo/uwsgi-nginx-flask:python3.10

# set APP_PORT to 80
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=200 \
    POETRY_VERSION=1.2.0 \
    APP_DIR=/app \ 
    UWSGI_INI=/app/uwsgi.ini \
    NGINX_WORKER_PROCESSES=1

# Note: 
# The starting number of uWSGI processes is controlled by the variable UWSGI_CHEAPER, by default set to 2.
# The maximum number of uWSGI processes is controlled by the variable UWSGI_PROCESSES, by default set to 16
# NGINX_MAX_UPLOAD is set to 0 by default that allows unlimited upload file sizes



# install poetry
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

# set work directory
WORKDIR ${APP_DIR}
RUN chown www-data:www-data ${APP_DIR}

# remove the old uwsgi.ini and main.py
RUN rm -rf ${APP_DIR}/main.py
RUN rm -rf ${APP_DIR}/uwsgi.ini

# copy relevant files and run poetry install
COPY ./uwsgi.ini ./

# create a separate folder called app
RUN mkdir app 
WORKDIR ${APP_DIR}/app

# copy other files to app/app
# Note: run_api.py is not needed
COPY ./pyproject.toml ./poetry.lock ./config.yml ./main.py ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi --no-root

# copy schematic_api folder
COPY  schematic_api ./schematic_api

# copy great_expectations folder
COPY great_expectations ./great_expectations

# copy tests folder because some endpoints by default download to the tests folder
COPY tests ./tests

# change permission
RUN chown -R www-data:www-data ${APP_DIR}/app

# allow downloading to synapse cache 
RUN chown -R www-data:www-data /root

# copy schematic
COPY schematic  ./schematic

# change permission 
WORKDIR /var/www/
RUN chown www-data:www-data /var/www/

# change work directory back
WORKDIR ${APP_DIR}/app