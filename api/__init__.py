import os

import connexion

from schematic import CONFIG

# path to `config.yml` file as constant
CONFIG_PATH = os.path.abspath(os.path.join(__file__, "../../config.yml"))


def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml")
    app = connexionapp.app

    # Configure schematic and do the error handling
    # check if file exists at the path created, i.e., CONFIG_PATH
    if os.path.isfile(CONFIG_PATH):
        CONFIG.load_config(CONFIG_PATH)
    else:
        FileNotFoundError(
            f"No configuration file was found at this path: {CONFIG_PATH}"
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
