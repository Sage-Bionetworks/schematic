# import our application
# Run our application
from api import create_app
from flask_cors import CORS

if __name__ == "__main__":
    app = create_app()
    CORS(app, support_credentials=True)
    app.run(port=3001, debug=True)
