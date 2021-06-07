import connexion


def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml")
    app = connexionapp.app

    return app
