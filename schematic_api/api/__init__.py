import os

import connexion
from typing import Tuple

from celery import Celery
import traceback
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
)
from schematic.exceptions import AccessCredentialsError


from schematic import CONFIG
from jaeger_client import Config
from flask_opentracing import FlaskTracer

config = Config(
    config={
        "enabled": True,
        "sampler": {"type": "const", "param": 1},
        "logging": True,
    },
    service_name="schema-api",
)
jaeger_tracer = config.initialize_tracer


def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api(
        "api.yaml", arguments={"title": "Schematic REST API"}, pythonic_params=True
    )

    # get the underlying Flask app instance
    app = connexionapp.app
    app.logger.error("service: " + app.name)
    # path to config.yml file saved as a Flask config variable
    default_config = os.path.abspath(os.path.join(__file__, "../../../config.yml"))
    schematic_config = os.environ.get("SCHEMATIC_CONFIG", default_config)
    schematic_config_content = os.environ.get("SCHEMATIC_CONFIG_CONTENT")

    app.config["SCHEMATIC_CONFIG"] = schematic_config
    app.config["SCHEMATIC_CONFIG_CONTENT"] = schematic_config_content
    app.config['CELERY_BROKER_URL'] = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app.config['CELERY_RESULT_BACKEND'] =  os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    # TODO need to attach celery as an extension to the flask app...
    celery = Celery(
        app.name,
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(
        broker_url=app.config['CELERY_BROKER_URL'],
        result_backend=app.config['CELERY_BROKER_URL']
    )
    celery.autodiscover_tasks(['schematic_api.api.tasks'])
    
    # handle exceptions in schematic when an exception gets raised
    @app.errorhandler(Exception)
    def handle_exception(e: Exception) -> Tuple[str, int]:
        """handle exceptions in schematic APIs"""
        # Ensure the application context is available
        with app.app_context():
            # Get the last line of error from the traceback
            last_line = traceback.format_exc().strip().split("\n")[-1]

            # Log the full trace
            app.logger.error(traceback.format_exc())

            # Return a JSON response with the last line of the error
            return last_line, 500

    @app.errorhandler(SynapseAuthenticationError)
    def handle_synapse_auth_error(e: Exception) -> Tuple[str, int]:
        """handle synapse authentication error"""
        return str(e), 401

    @app.errorhandler(AccessCredentialsError)
    def handle_synapse_access_error(e: Exception) -> Tuple[str, int]:
        """handle synapse access error"""
        return str(e), 403

    return app, celery


app, celery = create_app()

# def make_celery(app):
#     """
#     Create a new Celery object and tie the Celery config to the app's config.
#     Wrap all tasks in the context of the Flask application.
#     """

#     class ContextTask(celery.Task):
#         """
#         Make Celery tasks work with Flask app context.
#         """
#         abstract = True

#         def __call__(self, *args, **kwargs):
#             with app.app_context():
#                 return super(ContextTask, self).__call__(*args, **kwargs)

#     celery.Task = ContextTask
#     return celery
# celery = make_celery(app)

flask_tracer = FlaskTracer(
    jaeger_tracer, True, app, ["url", "url_rule", "environ.HTTP_X_REAL_IP", "path"]
)


# def route_code():
#     import flask_schematic as sc
#     sc.method1()
# ]
#
