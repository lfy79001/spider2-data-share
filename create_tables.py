import concurrent.futures
import os
from collections import defaultdict
from enum import Enum
from typing import Any, Optional

import jsonlines
from loguru import logger
from snowflake import connector
from tqdm import tqdm

from config import SnowflakeConfig, get_snowflake_connection

# ideally MAX_SCHEMA_THREADS * MAX_TABLE_THREADS should be equal to warehouse max_concurrency
MAX_SCHEMA_THREADS = 2
MAX_TABLE_THREADS = 4


class Status(Enum):
    SKIP = "SKIP"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


def get_sf_connection(config: Optional[SnowflakeConfig] = None) -> connector.SnowflakeConnection:
    """Get Snowflake connection using configuration."""
    if config is None:
        config = SnowflakeConfig.from_env()
    return get_snowflake_connection(config)


def get_json_setup(
    source_file: str = "data/unpack_spider2.jsonl",
) -> list[dict[str, Any]]:
    with jsonlines.open(source_file, "r") as reader:
        return [row for row in reader]


def prepare_table_creation_sqls(source_table: str, target_table: str) -> str:
    return f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM {source_table}"


def execute_sql(sql: str, cursor) -> None:
    cursor.execute(sql)


def execute_sqls(sqls: list[str], conn: connector.SnowflakeConnection) -> None:
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_TABLE_THREADS
    ) as executor:
        completed = 0
        cursor = conn.cursor(connector.DictCursor)
        futures = [executor.submit(execute_sql, sql, cursor) for sql in sqls]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(sqls)):
            completed += 1


def _check_schema_and_execute(
    schema: str, sqls: list[str], conn: connector.SnowflakeConnection
) -> Status:
    logger.debug(f"Checking schema {schema} with {len(sqls)} tables")
    cursor = conn.cursor(connector.DictCursor)
    if len(sqls) == 0:
        logger.info(f"Source schema {schema} is empty. Skipping...")
        return Status.SKIP  # skip as all tables are already created

    try:
        created_tables = cursor.execute(f"SHOW TABLES IN SCHEMA {schema}").fetchall()
    except Exception:
        logger.info(f"Creating schema {schema}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        created_tables = []

    created_tables = [table["name"] for table in created_tables]
    if len(created_tables) == len(sqls):
        logger.info(
            f"Schema {schema} already created and contain all {len(sqls)} tables. Skipping..."
        )
        return Status.SKIP  # skip as all tables are already created

    try:
        execute_sqls(sqls, conn=conn)
        logger.info(f"All tables in schema {schema} created!")
        return Status.SUCCESS
    except Exception as e:
        logger.error(f"Error while creating tables in schema {schema}: {e}")
        return Status.ERROR


def safe_check_schema_and_execute(
    schema: str, sqls: list[str], conn: connector.SnowflakeConnection
) -> Status:
    try:
        return _check_schema_and_execute(schema, sqls, conn)
    except Exception as e:
        logger.error(f"Error while processing schema {schema}: {e}")
        return Status.ERROR


def main(
    source_file: str,
    account: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    admin_role: Optional[str] = None,
    readonly_role: Optional[str] = None
) -> None:
    """
    Create tables from JSONL mapping file.
    
    Args:
        source_file: Path to JSONL mapping file
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
    
    conn = get_sf_connection(config)
    json_data = get_json_setup(source_file=source_file)

    schema_to_table_sqls = dict()

    for row in tqdm(json_data):
        logger.info(
            f"Processing schema: {row['target_schema']} from {row['source_schema']}"
        )
        table_sqls = [
            prepare_table_creation_sqls(table["source_table"], table["target_table"])
            for table in row["tables"]
        ]
        schema_to_table_sqls[row["target_schema"]] = table_sqls

    statuses: dict[Status, int] = defaultdict(int)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_SCHEMA_THREADS
    ) as executor:
        futures = [
            executor.submit(safe_check_schema_and_execute, schema, sqls, conn)
            for schema, sqls in schema_to_table_sqls.items()
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures), total=len(schema_to_table_sqls)
        ):
            status = future.result()
            statuses[status] += 1

    logger.info("All schemas processed! 🎉")
    for status, count in statuses.items():
        logger.info(f"{status}: {count}")


if __name__ == "__main__":
    import fire

    fire.Fire(main)
