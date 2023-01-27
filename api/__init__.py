import os

import connexion

from schematic import CONFIG
from werkzeug.middleware.proxy_fix import ProxyFix

def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml")
    

    # get the underlying Flask app instance
    app = connexionapp.app
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_port=1)

    # path to config.yml file saved as a Flask config variable
    default_config = os.path.abspath(os.path.join(__file__, "../../config.yml"))
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


# def route_code():
#     import flask_schematic as sc
#     sc.method1()
#
