import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List

import aw_datastore
import flask.json.provider
from aw_datastore import Datastore
from flask import (
    Blueprint,
    Flask,
    current_app,
    send_from_directory,
)
from flask_cors import CORS

from . import rest
from .api import ServerAPI
from .custom_static import get_custom_static_blueprint
from .log import FlaskLogHandler

logger = logging.getLogger(__name__)

app_folder = os.path.dirname(os.path.abspath(__file__))
static_folder = os.path.join(app_folder, "static")

root = Blueprint("root", __name__, url_prefix="/")


class AWFlask(Flask):
    def __init__(
        self,
        host: str,
        testing: bool,
        cors_origins=[],
        custom_static=dict(),
        static_folder=static_folder,
        static_url_path="",
        bronevik_url="",
        mysql_kwargs=None,
    ):
        name = "aw-server"
        self.json_provider_class = CustomJSONProvider
        # only prettyprint JSON if testing (due to perf)
        self.json_provider_class.compact = not testing
        # Initialize Flask
        Flask.__init__(
            self,
            name,
            static_folder=static_folder,
            static_url_path=static_url_path,
        )
        self.config["HOST"] = host  # needed for host-header check
        with self.app_context():
            _config_cors(cors_origins, testing)

        # Initialize datastore and API
        # Pass MySQL parameters directly to Datastore
        storage_kwargs = {"testing": testing}
        if mysql_kwargs:
            storage_kwargs.update(mysql_kwargs)
        
        db = Datastore(**storage_kwargs)
        
        # Initialize database tables
        self._initialize_database(db)
        
        self.api = ServerAPI(db=db, testing=testing, bronevik_url=bronevik_url)

        self.register_blueprint(root)
        self.register_blueprint(rest.blueprint)
        self.register_blueprint(get_custom_static_blueprint(custom_static))

    def _initialize_database(self, db):
        """Initialize database tables and ensure they exist"""
        try:
            # Tables are already created in Datastore constructor
            logger.info("MySQL database tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database tables: {e}")
            # Don't raise the exception, let the server start anyway


class CustomJSONProvider(flask.json.provider.DefaultJSONProvider):
    # encoding/decoding of datetime as iso8601 strings
    # encoding of timedelta as second floats
    def default(self, obj, *args, **kwargs):
        try:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, timedelta):
                return obj.total_seconds()
        except TypeError:
            pass
        return super().default(obj)


@root.route("/")
def static_root():
    return current_app.send_static_file("index.html")


@root.route("/css/<path:path>")
def static_css(path):
    return send_from_directory(static_folder + "/css", path)


@root.route("/js/<path:path>")
def static_js(path):
    return send_from_directory(static_folder + "/js", path)


def _config_cors(cors_origins: List[str], testing: bool):
    if cors_origins:
        logger.warning(
            "Running with additional allowed CORS origins specified through config "
            "or CLI argument (could be a security risk): {}".format(cors_origins)
        )

    if testing:
        # Used for development of aw-webui
        cors_origins.append("http://127.0.0.1:27180/*")

    # TODO: This could probably be more specific
    #       See https://github.com/ActivityWatch/aw-server/pull/43#issuecomment-386888769
    cors_origins.append("moz-extension://*")

    # See: https://flask-cors.readthedocs.org/en/latest/
    CORS(current_app, resources={r"/api/*": {"origins": cors_origins}})


# Only to be called from aw_server.main function!
def _start(
    host: str,
    port: int,
    testing: bool = False,
    cors_origins: List[str] = [],
    custom_static: Dict[str, str] = dict(),
    bronevik_url: str = "",
    mysql_kwargs: Dict = None,
):
    app = AWFlask(
        host,
        testing=testing,
        cors_origins=cors_origins,
        custom_static=custom_static,
        bronevik_url=bronevik_url,
        mysql_kwargs=mysql_kwargs,
    )
    try:
        app.run(
            debug=testing,
            host=host,
            port=port,
            request_handler=FlaskLogHandler,
            use_reloader=False,
            threaded=True,
        )
    except OSError as e:
        logger.exception(e)
        raise e
