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
    #CORS(app, resources={r"*": {"origins": "*"}})
    app.run(host=host, port=port, debug=False)