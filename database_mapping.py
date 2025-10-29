import os
from typing import Any, Optional

import fire
import jsonlines
from loguru import logger
from snowflake import connector

from utils import (
    DELIMITER,
    get_schema_names,
    get_table_names,
)
from config import SnowflakeConfig, get_snowflake_connection


def get_sf_connection(config: Optional[SnowflakeConfig] = None) -> connector.SnowflakeConnection:
    """Get Snowflake connection using configuration."""
    if config is None:
        config = SnowflakeConfig.from_env()
    return get_snowflake_connection(config)


def main(
    source_database: str, 
    output_file: str,
    account: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    admin_role: Optional[str] = None,
    readonly_role: Optional[str] = None
) -> None:
    """
    Generate database mapping from source database to target databases.
    
    Args:
        source_database: Name of the source database to map
        output_file: Output JSONL file path
        account: Snowflake account (uses env SNOWFLAKE_ACCOUNT if not provided)
        role: Snowflake role (uses env SNOWFLAKE_ROLE if not provided)
        warehouse: Snowflake warehouse (uses env SNOWFLAKE_WAREHOUSE if not provided)
        admin_role: Admin role for permissions (uses env SNOWFLAKE_ADMIN_ROLE if not provided)
        readonly_role: Readonly role for permissions (uses env SNOWFLAKE_READONLY_ROLE if not provided)
    """
    config = SnowflakeConfig.from_env(
        account=account,
        role=role,
        warehouse=warehouse,
        admin_role=admin_role,
        readonly_role=readonly_role
    )
    connection = get_sf_connection(config)
    schema_names = get_schema_names(connection, source_database)

    jsons = []
    for schema_name in schema_names:
        try:
            new_database_name, real_schema_name = schema_name.split(DELIMITER)
        except ValueError as e:
            logger.warning(e)
        target_fqn_schema = f"{new_database_name}.{real_schema_name}"
        schema_json: dict[str, Any] = {
            "source_database": source_database,
            "source_schema": schema_name,
            "target_database_name": new_database_name,
            "target_schema": target_fqn_schema,
            "tables": [],
        }
        tables = get_table_names(connection, f"{source_database}.{schema_name}")
        for table in tables:
            schema_json["tables"].append(
                {
                    "source_table": f"{source_database}.{schema_name}.{table}",
                    "target_table": f"{target_fqn_schema}.{table}",
                }
            )
        jsons.append(schema_json)

    with jsonlines.open(output_file, "w") as writer:
        writer.write_all(jsons)

    logger.info(
        f"Done. Prepared json mapping for {len(schema_names)} schemas from the source database."
    )
    connection.close()


if __name__ == "__main__":
    fire.Fire(main)
