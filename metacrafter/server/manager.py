"""Server manager module for Metacrafter."""
import logging
import os

from flask import (
    Flask,
)

from .api import MetacrafterApp

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
    """Run classification server using MetacrafterApp.
    
    Args:
        host: Hostname or IP to bind the server to
        port: Port number to listen on
        debug: Enable debug mode if True
    """
    app_factory = MetacrafterApp()
    app = app_factory.app
    app.config["SECRET_KEY"] = SECRET_KEY
    app.config["PROPAGATE_EXCEPTIONS"] = True

    # Initialize rules (lazy initialization will happen on first request if not done here)
    app_factory.initialize_rules()
    
    if debug:
        logging.getLogger().addHandler(logging.StreamHandler())
        logging.basicConfig(
            filename="metacrafter_server.log",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=logging.DEBUG,
        )

    app.run(host=host, port=port, debug=debug)
