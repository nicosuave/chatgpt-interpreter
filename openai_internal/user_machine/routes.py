from quart import Blueprint


def get_blueprint(send_callback):
    blueprint = Blueprint("routes", __name__)
    return blueprint
