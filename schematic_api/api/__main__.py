import os
import connexion
from schematic import CONFIG
from flask_cors import CORS
from schematic_api.api import app


def main(): 
    # Get app configuration
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = os.environ.get("APP_PORT", "3001")
    port = int(port)

    # Launch app
    #TO DO: add a flag --debug to control debug parameter
    app.run(host=host, port=port, debug=True)

if __name__ == "__main__":
    main()