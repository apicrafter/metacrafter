import logging
import os

from flask import (
    Flask,
)

from .api import add_api_rules, initialize_rules

MANAGE_PREFIX = ""
CLASSIFY_HOST = "127.0.0.1"
CLASSIFY_PORT = 10399
DEBUG = True

# Load secret key from environment variable, generate random one if not set
# Never commit actual secret keys to version control
SECRET_KEY = os.environ.get(
    'METACRAFTER_SECRET_KEY',
    os.urandom(32).hex() if hasattr(os, 'urandom') else 'change_this_a_very_unique_secret_key'
)


def run_server(host=CLASSIFY_HOST, port=CLASSIFY_PORT, debug=DEBUG):
    """Run classification server"""

    app = Flask("Metacrafter", static_url_path="/assets")
    app.config["SECRET_KEY"] = SECRET_KEY
    app.config["PROPAGATE_EXCEPTIONS"] = True

    add_api_rules(app)
    if debug:
        logging.getLogger().addHandler(logging.StreamHandler())
        logging.basicConfig(
            filename="metacrafter_server.log",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=logging.DEBUG,
        )

    initialize_rules()

    app.run(host=host, port=port, debug=debug)
