import os

import connexion
from connexion import AioHttpApp

from schematic import CONFIG
from aiohttp import web

# async def index(request):
#     return web.Response(text='Hello Aiohttp!')

# def setup_routes(app):
#     app.router.add_get('/', index)

def create_app():
    #connexionapp = connexion.FlaskApp(__name__, specification_dir="openapi/")
    connexionapp = AioHttpApp(__name__, specification_dir="openapi/")
    connexionapp.add_api("api.yaml", arguments={"title": "Schematic REST API"}, pythonic_params=True)
    

    # get the underlying Flask app instance
    app = connexionapp
    #app = connexionapp.app
    # app = web.Application()
    # setup_routes(app)

    # path to config.yml file saved as a Flask config variable
    default_config = os.path.abspath(os.path.join(__file__, "../../../config.yml"))
    # schematic_config = os.environ.get("SCHEMATIC_CONFIG", default_config)
    # schematic_config_content = os.environ.get("SCHEMATIC_CONFIG_CONTENT")

    # app.config["SCHEMATIC_CONFIG"] = schematic_config
    # app.config["SCHEMATIC_CONFIG_CONTENT"] = schematic_config_content
    SCHEMATIC_CONFIG = os.environ.get("SCHEMATIC_CONFIG",default_config)
    SCHEMATIC_CONFIG_CONTENT = os.environ.get("SCHEMATIC_CONFIG_CONTENT")
    print('schematic config path', SCHEMATIC_CONFIG)



    #web.run_app(app)
    # Configure flask app
    # app.config[] = schematic[]
    # app.config[] = schematic[]
    # app.config[] = schematic[]

    # Initialize extension schematic
    # import MyExtension
    # myext = MyExtension()
    # myext.init_app(app)

    return app

app = create_app()


# def route_code():
#     import flask_schematic as sc
#     sc.method1()
#
