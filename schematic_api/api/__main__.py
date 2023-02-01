import os
import connexion
from schematic import CONFIG
from flask_cors import CORS

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


def main(): 
    # Get app configuration
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = os.environ.get("APP_PORT", "3001")
    port = int(port)

    # Launch app
    app = create_app()
    CORS(app, resources={r"*": {"origins": "*"}})
    app.run(host=host, port=port, debug=False)

if __name__ == "__main__":
    main()