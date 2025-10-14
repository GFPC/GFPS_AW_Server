from aw_core.config import load_config_toml

default_config = """
[server]
host = "127.0.0.1"
port = "5700"
storage = "mysql"
cors_origins = "*"
bronevik_url = "https://ibronevik.ru/taxi/c/gruzvill/api/v1/"

[server.mysql]
host = "localhost"
port = 3306
user = "root"
password = "93029302"
database = "activitywatch"

[server.custom_static]

[server-testing]
host = "localhost"
port = "5777"
storage = "mysql"
cors_origins = "*"
bronevik_url = "https://ibronevik.ru/taxi/c/gruzvill/api/v1/"

[server-testing.mysql]
host = "localhost"
port = 3306
user = "root"
password = "93029302"
database = "activitywatch_test"

[server-testing.custom_static]
""".strip()

config = load_config_toml("aw-server", default_config)
