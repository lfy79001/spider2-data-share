#!/usr/bin/env python3
"""
Configuration module for Spider2 data migration scripts.
Supports configuration via environment variables and command line arguments.
"""

import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class SnowflakeConfig:
    """Snowflake connection and role configuration."""
    account: str
    role: str
    warehouse: str
    admin_role: str
    readonly_role: str
    user: Optional[str] = None
    protocol: str = "https"
    port: int = 443
    authenticator: str = "snowflake"

    @classmethod
    def from_env(cls, 
                 account: Optional[str] = None,
                 role: Optional[str] = None,
                 warehouse: Optional[str] = None,
                 admin_role: Optional[str] = None,
                 readonly_role: Optional[str] = None) -> "SnowflakeConfig":
        """Create configuration from environment variables with optional overrides."""
        
        # Get values from parameters or environment variables
        final_account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        final_role = role or os.getenv("SNOWFLAKE_ROLE")
        final_warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
        final_admin_role = admin_role or os.getenv("SNOWFLAKE_ADMIN_ROLE")
        final_readonly_role = readonly_role or os.getenv("SNOWFLAKE_READONLY_ROLE")
        final_user = os.getenv("DESTINATION_USER")
        
        # Validate required configuration
        missing_configs = []
        if not final_account:
            missing_configs.append("SNOWFLAKE_ACCOUNT")
        if not final_role:
            missing_configs.append("SNOWFLAKE_ROLE")
        if not final_warehouse:
            missing_configs.append("SNOWFLAKE_WAREHOUSE")
        if not final_admin_role:
            missing_configs.append("SNOWFLAKE_ADMIN_ROLE")
        if not final_readonly_role:
            missing_configs.append("SNOWFLAKE_READONLY_ROLE")
        
        if missing_configs:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing_configs)}. "
                f"Please set environment variables or provide command line arguments."
            )
        
        return cls(
            account=final_account,
            role=final_role,
            warehouse=final_warehouse,
            admin_role=final_admin_role,
            readonly_role=final_readonly_role,
            user=final_user
        )

    def get_connection_params(self) -> dict:
        """Get connection parameters for Snowflake connector."""
        params = {
            "protocol": self.protocol,
            "port": self.port,
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
            "role": self.role,
            "warehouse": self.warehouse,
        }

        # Add password if using snowflake authenticator
        if self.authenticator == "snowflake":
            password = os.getenv("SNOWFLAKE_PASSWORD")
            if password:
                params["password"] = password

        return params

    def get_database_setup_commands(self, db_name: str) -> list[str]:
        """Generate database setup commands with configurable roles."""
        sql_template = f"""
USE ROLE accountadmin;
CREATE DATABASE IF NOT EXISTS {db_name};
GRANT OWNERSHIP ON DATABASE {db_name} TO ROLE {self.admin_role};
GRANT ALL PRIVILEGES ON DATABASE {db_name} TO ROLE {self.admin_role};
GRANT USAGE ON DATABASE  {db_name} TO ROLE {self.readonly_role};
GRANT USAGE ON ALL SCHEMAS IN DATABASE {db_name}  TO ROLE {self.readonly_role};
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {db_name}  TO ROLE {self.readonly_role};
GRANT SELECT ON ALL TABLES IN DATABASE {db_name}  TO ROLE {self.readonly_role};
GRANT SELECT ON FUTURE TABLES IN DATABASE {db_name}  TO ROLE {self.readonly_role};
"""
        return sql_template.strip().split("\n")


def get_snowflake_connection(config: SnowflakeConfig):
    """Create Snowflake connection using the provided configuration."""
    from snowflake import connector
    return connector.connect(**config.get_connection_params())
