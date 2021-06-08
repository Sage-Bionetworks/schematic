import connexion


def create_app():
    connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml")
    app = connexionapp.app

    # Configure schematic & do the error handling
    # Call schematic.load_config()
    # keys = schematic get_config()

    # Configure flask app
    # app.config[] = schematic[]
    # app.config[] = schematic[]
    # app.config[] = schematic[]

    # Initialize extension schematic
    # import MyExtension
    # myext = MyExtension()
    # myext.init_app(app)

    return app

# def route_code():
#     import flask_schematic as sc
#     sc.method1()
# 
