#!/usr/bin/env python3

import concurrent.futures
import json
import os
import time
from typing import Any, Optional

import snowflake.connector
from loguru import logger
from tqdm import tqdm

from config import SnowflakeConfig, get_snowflake_connection


def get_sf_connection(config: Optional[SnowflakeConfig] = None) -> snowflake.connector.SnowflakeConnection:
    """Get Snowflake connection using configuration."""
    if config is None:
        config = SnowflakeConfig.from_env()
    return get_snowflake_connection(config)


def get_db_setup_commands(db_name: str, config: SnowflakeConfig) -> list[str]:
    """Generate database setup commands with configurable roles."""
    return config.get_database_setup_commands(db_name)


def get_schema_setup_commands(json_data: dict[str, Any], config: SnowflakeConfig) -> list[str]:
    """Generate schema setup commands with configurable role."""
    schema_name = json_data["target_schema"]
    tables = json_data["tables"]

    schema_path = f"{schema_name}"
    out_cmds = [
        f"USE ROLE {config.admin_role}",
        f"CREATE SCHEMA IF NOT EXISTS {schema_path}",
    ]

    for table in tables:
        source_table = table["source_table"]
        target_table = table["target_table"]
        out_cmds.append(
            f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM {source_table}"
        )

    return out_cmds


def setup_dbs_cmds(json_data: dict[str, Any], config: SnowflakeConfig) -> list[str]:
    """Generate database setup commands."""
    db_name = json_data["target_database_name"]
    cmds = get_db_setup_commands(db_name, config)
    return cmds


def process_jsonl_file(file_path: str) -> list[dict[str, Any]]:
    sql_statements = []
    with open(file_path, "r") as file:
        for line in file:
            json_data = json.loads(line)
            if json_data["target_database_name"] == "EBI_CHEMBL":
                continue
            sql_statements.append(json_data)
    return sql_statements


def check_database_exists(
    conn: snowflake.connector.SnowflakeConnection, db_name: str
) -> bool:
    cursor = conn.cursor()
    query = f"SHOW DATABASES LIKE '{db_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result is not None


def check_schema_exists(
    snowflake_connector: snowflake.connector.SnowflakeConnection, 
    schema_path: str,
    config: SnowflakeConfig
) -> bool:
    """Check if schema exists using configurable admin role."""
    db_name, schema = schema_path.split(".")
    cursor = snowflake_connector.cursor()
    cursor.execute(f"USE ROLE {config.admin_role}")
    cursor.execute(f"USE DATABASE {db_name}")
    query = f"SHOW SCHEMAS LIKE '{schema}'"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result is not None


def exec_set_of_cmds(
    cmds: list[str], 
    config: SnowflakeConfig,
    db_to_check: str | None = None, 
    schema_to_check: str | None = None
) -> str:
    """Execute set of SQL commands with configurable connection."""
    time.sleep(3)
    sf_conn = get_sf_connection(config)
    if db_to_check is not None:
        if check_database_exists(sf_conn, db_to_check):
            return "SKIPPED"

    if schema_to_check is not None:
        if check_schema_exists(sf_conn, schema_to_check, config):
            return "SKIPPED"

    for cmd in cmds:
        try:
            _ = sf_conn.cursor().execute(cmd).fetchall()
        except Exception as e:
            logger.error(f"Error while executing {cmd}: {e}")
            return "FAILED"
    return "DONE"


def main(
    file_path: str,
    account: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    admin_role: Optional[str] = None,
    readonly_role: Optional[str] = None
) -> None:
    """
    Create databases from JSONL mapping file.
    
    Args:
        file_path: Path to JSONL mapping file
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
    
    all_input_jsonl = process_jsonl_file(file_path)

    dbs_setup_commands: dict[str, list[str]] = {}

    # Get commands for setting up dbs
    for json_data in all_input_jsonl:
        db_name = json_data["target_database_name"]
        if db_name not in dbs_setup_commands:
            dbs_setup_commands[db_name] = setup_dbs_cmds(json_data, config)

    logger.info(f"Will attempt to create {len(dbs_setup_commands)} dbs")
    db_create_statuses = {}

    # Execute database creation commands
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_db = {
            executor.submit(exec_set_of_cmds, cmds, config, db_to_check=db): db
            for db, cmds in dbs_setup_commands.items()
        }
        for future in tqdm(
            concurrent.futures.as_completed(future_to_db), total=len(dbs_setup_commands)
        ):
            try:
                db = future_to_db[future]
                status = future.result()
                db_create_statuses[db] = status
            except Exception as e:
                logger.error(f"{db}: {e}")
                db_create_statuses[db] = "ERROR"
                continue


if __name__ == "__main__":
    import fire

    fire.Fire(main)
