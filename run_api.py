#!/usr/bin/env python3

# import our application
# Run our application
from schematic_api.api import create_app
from flask_cors import CORS
import os

if __name__ == "__main__":
    # Get app configuration
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = os.environ.get("APP_PORT", "3001")
    port = int(port)

    # Launch app
    app = create_app()

    # default_config = os.path.abspath(os.path.join(__file__, "config.yml"))
    # schematic_config = os.environ.get("SCHEMATIC_CONFIG", default_config)
    # schematic_config_content = os.environ.get("SCHEMATIC_CONFIG_CONTENT")

    #CORS(app, resources={r"*": {"origins": "*"}})
    # default_config = os.path.abspath(os.path.join(__file__, "config.yml"))
    # SCHEMATIC_CONFIG = os.environ.get("SCHEMATIC_CONFIG",default_config)
    # SCHEMATIC_CONFIG_CONTENT = os.environ.get("SCHEMATIC_CONFIG_CONTENT")
   

    #print('can I see the environment variable', SCHEMATIC_CONFIG)
    app.run(host=host, port=port, debug=False)
