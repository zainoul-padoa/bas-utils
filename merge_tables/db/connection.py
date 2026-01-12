"""Database connection management."""

import duckdb
from config import load_config, get_db_config


def connect_to_postgres_via_duckdb():
    """
    Connect DuckDB to PostgreSQL database.
    
    Returns:
        duckdb.DuckDBPyConnection: DuckDB connection with PostgreSQL attached
    """
    # Load environment variables
    load_config()
    
    # Create DuckDB connection
    duck = duckdb.connect()
    
    # Load the postgres extension
    duck.execute("load postgres")
    
    # Get database configuration
    db_config = get_db_config()
    
    # Create secret and attach PostgreSQL database to DuckDB
    duck.execute(
        f"""
        create or replace secret (
            type postgres,
            host '{db_config['host']}',
            port {db_config['port']},
            database '{db_config['database']}',
            user '{db_config['user']}',
            password '{db_config['password']}'
        );
        
        attach '' as pg (type postgres);
        """
    )
    
    print(f"✓ Successfully connected DuckDB to PostgreSQL database '{db_config['database']}'")
    return duck
