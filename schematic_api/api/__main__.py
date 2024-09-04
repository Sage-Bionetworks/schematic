import os
from schematic_api.api import app
from celery import Celery


def main():
    # Get app configuration
    host = os.environ.get("APP_HOST", "0.0.0.0")
    port = os.environ.get("APP_PORT", "3001")
    port = int(port)
    # TODO: use env variable instead later
    app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
    app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
    
    celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)

    # Launch app
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
