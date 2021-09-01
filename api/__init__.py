import os

import connexion

from schematic import CONFIG


def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml")

    # get the underlying Flask app instance
    app = connexionapp.app

    # path to config.yml file saved as a Flask config variable
    app.config["SCHEMATIC_CONFIG"] = os.path.abspath(
        os.path.join(__file__, "../../config.yml")
    )

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
