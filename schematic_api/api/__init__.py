import os

import connexion

from schematic import CONFIG
from jaeger_client import Config
from flask_opentracing import FlaskTracer
import traceback
import jsonify

config = Config(
    config={
        'enabled': True,
        'sampler': {
            'type': 'const',
            'param': 1
        },
        'logging': True,
    },
    service_name="schema-api",
)
jaeger_tracer = config.initialize_tracer


def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml", arguments={"title": "Schematic REST API"}, pythonic_params=True)
    

    # get the underlying Flask app instance
    app = connexionapp.app

    # path to config.yml file saved as a Flask config variable
    default_config = os.path.abspath(os.path.join(__file__, "../../../config.yml"))
    schematic_config = os.environ.get("SCHEMATIC_CONFIG", default_config)
    schematic_config_content = os.environ.get("SCHEMATIC_CONFIG_CONTENT")

    app.config["SCHEMATIC_CONFIG"] = schematic_config
    app.config["SCHEMATIC_CONFIG_CONTENT"] = schematic_config_content

    # Configure flask app
    # app.config[] = schematic[]
    # app.config[] = schematic[]
    # app.config[] = schematic[]

    # Initialize extension schematic
    # import MyExtension
    # myext = MyExtension()
    # myext.init_app(app)

    return app

app = create_app()

flask_tracer = FlaskTracer(jaeger_tracer, True, app, ['url', 'url_rule', 'environ.HTTP_X_REAL_IP', 'path'])


# def route_code():
#     import flask_schematic as sc
#     sc.method1()
#]
