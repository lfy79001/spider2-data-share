import os
from typing import Any

from snowflake.connector import DictCursor, SnowflakeConnection

DELIMITER = "__"


def get_environment_variable(name: str, default: str | None = None) -> str:
    try:
        return os.environ[name]
    except KeyError:
        if default is not None:
            return default
        else:
            raise ValueError(f"Environment variable '{name}' not found.")


def get_snowflake_conneciton_env_variables() -> tuple[str, str, str, str, str]:
    account = get_environment_variable("SPIDER_ACCOUNT")
    user = get_environment_variable("SPIDER_USER")
    password = get_environment_variable("SPIDER_PWD")
    warehouse = get_environment_variable(
        "SPIDER_WAREHOUSE", default="WH_MIGRATION"
    )
    role = get_environment_variable("SPIDER_ROLE", default="ACCOUNTADMIN")
    return account, user, password, warehouse, role


def get_database_names(connection: SnowflakeConnection) -> list[dict[str, Any]]:
    cursor = connection.cursor(DictCursor)
    cursor.execute("SHOW TERSE DATABASES")
    return [row for row in cursor.fetchall()]


def get_schema_names(connection: SnowflakeConnection, database_name: str) -> list[str]:
    cursor = connection.cursor(DictCursor)
    cursor.execute(f"SHOW SCHEMAS IN DATABASE {database_name}")
    return [row["name"] for row in cursor.fetchall()]


def get_table_names(
    connection: SnowflakeConnection, full_schema_name: str
) -> list[str]:
    cursor = connection.cursor(DictCursor)
    cursor.execute(f"SHOW TABLES IN SCHEMA {full_schema_name}")
    return [row["name"] for row in cursor.fetchall()]
