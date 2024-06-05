FROM tiangolo/uwsgi-nginx-flask:python3.10

# add version tag as a build argument
ARG TAG

# the environment variables defined here are the default
# and can be overwritten by docker run -e VARIABLE = XX
# or can be overwritten by .env when using docker compose
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=200 \
    POETRY_VERSION=1.3.0 \
    APP_PARENT_DIR=/app \ 
    NGINX_CONFIG=/etc/nginx/conf.d \
    APP_DIR=/app/app \
    ROOT=/ \
    UWSGI_INI=/app/uwsgi.ini \
    NGINX_WORKER_PROCESSES=1 \
    VERSION=$TAG



LABEL maintainer="Lingling Peng <lingling.peng@sagebase.org>\
                Andrew Lamb <andrew.lamb@sagebase.org>\
                Gianna Jordan <gianna.jordan@sagebase.org>"
                
LABEL version=$TAG 

                    
# Note: 
# The starting number of uWSGI processes is controlled by the variable UWSGI_CHEAPER, by default set to 2.
# The maximum number of uWSGI processes is controlled by the variable UWSGI_PROCESSES, by default set to 16
# By default, the image starts with 2 uWSGI processes running. When the server is experiencing a high load, it creates up to 16 uWSGI processes to handle it on demand.
# NGINX_MAX_UPLOAD is set to 0 by default that allows unlimited upload file sizes
# NGINX_WORKER_CONNECTIONS is set to 1024 by default that allows a maximum limit of 1024 connections per worker.
# NGINX_WORKER_OPEN_FILES is set to 2048 by default that allows 2048 open files 

# run open ssl and generate certificate
RUN apt update && \
    apt-get install openssl && \
    openssl req -x509 -nodes -days 365 \
    -subj  "/C=CA/ST=QC/O=Company" \
    -newkey rsa:2048 -keyout /etc/ssl/private/localhost.key \
    -out /etc/ssl/certs/localhost.crt;

# add dhparam.pem
# this step takes quite some time
RUN openssl dhparam -out /etc/ssl/dhparam.pem 4096

# copy config files that handle encryption to docker
WORKDIR ${NGINX_CONFIG}
COPY ./self-signed.conf ./ssl-params.conf ./certificate.conf ./


# use custom uwsgi-nginx-entrypoint.sh
# this uwsgi-nginx-entrypoint.sh file is derived from: https://github.com/tiangolo/uwsgi-nginx-flask-docker/blob/master/docker-images/entrypoint.sh
# we have to modify it so that we could generate a different /etc/nginx/conf.d/nginx.conf file
WORKDIR ${ROOT}
COPY ./uwsgi-nginx-entrypoint.sh ./entrypoint2.sh
COPY ./uwsgi-nginx-entrypoint.sh ./uwsgi-nginx-entrypoint2.sh
RUN chmod +x uwsgi-nginx-entrypoint2.sh
RUN chmod +x entrypoint2.sh
RUN chown -R nginx /uwsgi-nginx-entrypoint2.sh
RUN chown -R nginx /entrypoint2.sh

# install poetry
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

# set work directory
WORKDIR ${APP_PARENT_DIR}
RUN chown www-data:www-data ${APP_PARENT_DIR}

# remove the old uwsgi.ini and main.py from the original image
RUN rm -rf ${APP_PARENT_DIR}/main.py
RUN rm -rf ${APP_PARENT_DIR}/uwsgi.ini

# copy to use custom uwsgi.ini 
COPY ./uwsgi.ini ./

# create a separate folder called app
RUN mkdir app 
WORKDIR ${APP_DIR}

# copy other files to app/app
# Note: run_api.py is not needed

COPY ./pyproject.toml ./poetry.lock ./main.py ./
COPY ./config_example.yml ./config.yml
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --all-extras --no-root

# copy schematic_api folder
COPY  schematic_api ./schematic_api

# copy great_expectations folder
COPY great_expectations ./great_expectations

# copy tests folder because some endpoints by default download to the tests folder
COPY tests ./tests

# change permission
RUN chown -R www-data:www-data ${APP_DIR}

# allow downloading to synapse cache 
RUN chown -R www-data:www-data /root

# copy schematic
COPY schematic  ./schematic

# change permission 
WORKDIR /var/www/
#The -R option: make the command recursive, so it will change the owner of all files and subdirectories within a given folder.
RUN chown -R www-data:www-data /var/www/

RUN chown -R www-data:www-data /var/tmp/

# change work directory back
WORKDIR ${APP_DIR}

# specify entrypoint again to generate config
# have to respecify CMD too
ENTRYPOINT ["/entrypoint2.sh"]
CMD ["/start.sh"]

# Expose ports
EXPOSE 443
