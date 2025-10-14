import logging
import sys

from aw_core.log import setup_logging

from . import __version__
from .config import config
from .server import _start

logger = logging.getLogger(__name__)


def main():
    """Called from the executable and __main__.py"""

    settings = parse_settings()

    # FIXME: The LogResource API endpoint relies on the log being in JSON format
    # at the path specified by aw_core.log.get_log_file_path(). We probably want
    # to write the LogResource API so that it does not depend on any physical file
    # but instead add a logging handler that it can use privately.
    # That is why log_file_json=True currently.
    # UPDATE: The LogResource API is no longer available so log_file_json is now False.
    setup_logging(
        "aw-server",
        testing=settings.testing,
        verbose=settings.verbose,
        log_stderr=True,
        log_file=True,
    )

    logger.info(f"Using storage method: {settings.storage}")

    if settings.testing:
        logger.info("Will run in testing mode")

    if settings.custom_static:
        logger.info(f"Using custom_static: {settings.custom_static}")

    logger.info("Starting up...")
    _start(
        host=settings.host,
        port=settings.port,
        testing=settings.testing,
        cors_origins=settings.cors_origins,
        custom_static=settings.custom_static,
        bronevik_url=settings.bronevik_url,
        mysql_kwargs=settings.mysql_kwargs,
    )


def parse_settings():
    import argparse

    """ CLI Arguments """
    parser = argparse.ArgumentParser(description="Starts an ActivityWatch server")
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Run aw-server in testing mode using different ports and database",
    )
    parser.add_argument("--verbose", action="store_true", help="Be chatty.")
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and quit",
    )
    parser.add_argument(
        "--log-json", action="store_true", help="Output the logs in JSON format"
    )
    parser.add_argument(
        "--host", dest="host", help="Which host address to bind the server to"
    )
    parser.add_argument(
        "--port", dest="port", type=int, help="Which port to run the server on"
    )
    parser.add_argument(
        "--storage",
        dest="storage",
        help="The method to use for storing data. Some methods (such as MongoDB) require specific Python packages to be available (in the MongoDB case: pymongo)",
    )
    parser.add_argument(
        "--cors-origins",
        dest="cors_origins",
        help="CORS origins to allow (as a comma separated list)",
    )
    parser.add_argument(
        "--custom-static",
        dest="custom_static",
        help="The custom static directories. Format: watcher_name=path,watcher_name2=path2,...",
    )
    parser.add_argument(
        "--bronevik-url",
        dest="bronevik_url",
        help="The URL to the Bronevik API",
    )
    args = parser.parse_args()
    if args.version:
        print(__version__)
        sys.exit(0)

    """ Parse config file """
    configsection = "server" if not args.testing else "server-testing"
    settings = argparse.Namespace()
    settings.host = config[configsection]["host"]
    settings.port = int(config[configsection]["port"])
    settings.storage = config[configsection]["storage"]
    settings.cors_origins = config[configsection]["cors_origins"]
    settings.custom_static = dict(config[configsection]["custom_static"])
    settings.bronevik_url = config[configsection]["bronevik_url"]
    
    # MySQL settings with defaults
    if "mysql" in config[configsection]:
        mysql_config = config[configsection]["mysql"]
        settings.mysql_host = mysql_config.get("host", "localhost")
        settings.mysql_port = int(mysql_config.get("port", 3306))
        settings.mysql_user = mysql_config.get("user", "root")
        settings.mysql_password = mysql_config.get("password", "")
        settings.mysql_database = mysql_config.get("database", "activitywatch")
    else:
        # Default MySQL settings
        settings.mysql_host = "localhost"
        settings.mysql_port = 3306
        settings.mysql_user = "root"
        settings.mysql_password = ""
        settings.mysql_database = "activitywatch"

    """ If a argument is not none, override the config value """
    for key, value in vars(args).items():
        if value is not None:
            if key == "custom_static":
                settings.custom_static = parse_str_to_dict(value)
            else:
                vars(settings)[key] = value

    settings.cors_origins = [o for o in settings.cors_origins.split(",") if o]

    # Always use MySQL settings (since we removed the abstraction)
    settings.mysql_kwargs = {
        "host": settings.mysql_host,
        "port": settings.mysql_port,
        "user": settings.mysql_user,
        "password": settings.mysql_password,
        "database": settings.mysql_database
    }

    return settings


def parse_str_to_dict(str_value):
    """Parses a dict from a string in format: key=value,key2=value2,..."""
    output = dict()
    key_value_pairs = str_value.split(",")

    for pair in key_value_pairs:
        pair_split = pair.split("=")

        if len(pair_split) != 2:
            raise ValueError(f"Cannot parse key value pair: {pair}")

        key, value = pair_split
        output[key] = value

    return output
