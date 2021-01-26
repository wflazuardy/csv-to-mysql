from os import getenv

# MySQL
MYSQL_HOST = getenv("MYSQL_HOST")
MYSQL_PORT = int(getenv("MYSQL_PORT"))
MYSQL_USER = getenv("MYSQL_USER")
MYSQL_PASSWORD = getenv("MYSQL_PASSWORD")
MYSQL_SCHEMA = getenv("MYSQL_SCHEMA")