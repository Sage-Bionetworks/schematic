import os
import traceback
from typing import Tuple

import connexion
from synapseclient.core.exceptions import SynapseAuthenticationError

from schematic.exceptions import AccessCredentialsError


def create_app() -> None:
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api(
        "api.yaml", arguments={"title": "Schematic REST API"}, pythonic_params=True
    )

    # get the underlying Flask app instance
    app = connexionapp.app

    # path to config.yml file saved as a Flask config variable
    default_config = os.path.abspath(os.path.join(__file__, "../../../config.yml"))
    schematic_config = os.environ.get("SCHEMATIC_CONFIG", default_config)
    schematic_config_content = os.environ.get("SCHEMATIC_CONFIG_CONTENT")

    app.config["SCHEMATIC_CONFIG"] = schematic_config
    app.config["SCHEMATIC_CONFIG_CONTENT"] = schematic_config_content

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

    return app


app = create_app()

# def route_code():
#     import flask_schematic as sc
#     sc.method1()
# ]
#
