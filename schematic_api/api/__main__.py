import os
from schematic_api.api import app
import traceback
import jsonify


@app.errorhandler(Exception)
def handle_exception(e):
    # Get the last line of the traceback
    last_line = traceback.format_exc().strip().split("\n")[-1]

    # Log the full traceback (optional)
    app.logger.error(traceback.format_exc())

    # Return a JSON response with the last line of the error
    response = {"status": "error", "message": last_line}
    return jsonify(response), 500


def main():
    # Get app configuration
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = os.environ.get("APP_PORT", "3001")
    port = int(port)

    # Launch app
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
