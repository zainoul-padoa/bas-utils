"""Configuration management for the firm matching process."""

import os
from dotenv import load_dotenv


def load_config():
    """Load environment variables from .env file."""
    load_dotenv("../.env")


def get_table_name() -> str:
    """Get the target table name from environment or use default."""
    return os.getenv('TABLE_FIRMS_ZOHO', 'pg.medisoft.table_firms_zoho')


def get_enabled_functions() -> list:
    """Get list of enabled matching functions from environment."""
    enabled_functions_env = os.getenv('ENABLED_MATCHING_FUNCTIONS', '')
    if enabled_functions_env:
        return [f.strip() for f in enabled_functions_env.split(',')]
    return []


def get_db_config() -> dict:
    """Get database configuration from environment variables."""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'postgres')
    }
