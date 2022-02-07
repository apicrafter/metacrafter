from flask import Flask, json, jsonify, redirect, render_template, send_file, send_from_directory, request, url_for, flash, Response

import logging

from .api import add_api_rules


MANAGE_PREFIX = ''
CLASSIFY_HOST = '127.0.0.1'
CLASSIFY_PORT = 1399
DEBUG=False

SECRET_KEY = 'change_this_a_very_unique_secret_key'



def run_server():
#    global app

    app = Flask("Datacrafter", static_url_path='/assets')
    app.config['SECRET_KEY'] = SECRET_KEY
    app.config['PROPAGATE_EXCEPTIONS'] = True

    add_api_rules(app)


    logging.getLogger().addHandler(logging.StreamHandler())
    logging.basicConfig(
        filename='metacrafter_server.log',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG,
    )

    app.run(host=CLASSIFY_HOST, port=CLASSIFY_PORT, debug=DEBUG)
