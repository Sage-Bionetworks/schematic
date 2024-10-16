#!/usr/bin/env python3

# import our application
# Run our application
from schematic_api.api import create_app
import os

if __name__ == "__main__":
    # Get app configuration
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = os.environ.get("APP_PORT", "3001")
    port = int(port)

    # Launch app
    app = create_app()
    #TO DO: add a flag --debug to control debug parameter
    app.run(host=host, port=port, debug=False)