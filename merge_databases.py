# type: ignore
# (tzayats) adding this since this file is only triggers mypy error when running fmt_lint with docker.and not in the scope to fix warnings
import fire
from loguru import logger
from snowflake.connector import SnowflakeConnection
from utils import (
    DELIMITER,
    get_database_names,
    get_schema_names,
    get_snowflake_conneciton_env_variables,
)


def merge_database(
    connection: SnowflakeConnection,
    database_name: str,
    output_database: str,
    dry_run: bool = True,
) -> None:
    cursor = connection.cursor()

    # Create the database if it does not exist
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {output_database}"
    if dry_run:
        logger.debug(create_db_sql)
    else:
        cursor.execute(create_db_sql)
    schema_names = get_schema_names(connection, database_name)
    schema_names = [
        schema_name
        for schema_name in schema_names
        if schema_name != "INFORMATION_SCHEMA"
    ]

    logger.info(f"-- Working on {database_name} --")
    for schema_name in schema_names:
        if (
            DELIMITER in schema_name or DELIMITER in database_name
        ):  # to ensure that there will be no issues with unpacking later
            raise ValueError(
                f"Database or schema name contains the delimiter '{DELIMITER}'."
            )

        clone_db_sql = f"CREATE SCHEMA IF NOT EXISTS {output_database}.{database_name}{DELIMITER}{schema_name} CLONE {database_name}.{schema_name};"

        if dry_run:
            logger.debug(clone_db_sql)
        else:
            cursor.execute(clone_db_sql)

    logger.info(f"-- Database {database_name} merged into {output_database} --")


def main(output_database: str, dry_run: bool = True) -> None:
    account, user, password, warehouse, role = get_snowflake_conneciton_env_variables()

    connection = SnowflakeConnection(
        account=account, user=user, password=password, warehouse=warehouse, role=role
    )
    database_names = get_database_names(connection)
    snowflake_marketplace_dbs = []
    migration_dbs = []
    for row in database_names:
        kind = row["kind"]
        name = row["name"]
        if name in [
            "SNOWFLAKE",
            output_database,
        ]:  # if the database is system or output database, skip
            continue
        elif kind == "IMPORTED DATABASE":
            snowflake_marketplace_dbs.append(name)
        else:
            migration_dbs.append(name)
            merge_database(connection, name, output_database, dry_run)

    logger.debug("Snowflake Marketplace Databases (not included in migration):")
    logger.debug(snowflake_marketplace_dbs)

    logger.debug("Migrated Databases:")
    logger.debug(len(migration_dbs))
    connection.close()


if __name__ == "__main__":
    fire.Fire(main)
