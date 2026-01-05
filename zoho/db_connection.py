import psycopg2
import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# === CONFIGURATION ===
# Loads from .env file or environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}
# ======================


def connect_to_db(config: Optional[dict] = None) -> psycopg2.extensions.connection:
    """
    Connect to a PostgreSQL database.
    
    Args:
        config: Optional dictionary with connection parameters.
                If None, uses DB_CONFIG from above.
    
    Returns:
        psycopg2 connection object
    
    Raises:
        psycopg2.Error: If connection fails
    """
    if config is None:
        config = DB_CONFIG
    
    try:
        conn = psycopg2.connect(**config)
        print(f"✓ Successfully connected to database '{config['database']}'")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        raise


def test_connection(conn: psycopg2.extensions.connection):
    """
    Test the database connection by executing a simple query.
    
    Args:
        conn: psycopg2 connection object
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"PostgreSQL version: {version[0]}")
        
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()
        print(f"Current database: {db_name[0]}")
        
        cursor.close()
    except psycopg2.Error as e:
        print(f"Error testing connection: {e}")


if __name__ == "__main__":
    # Example usage
    try:
        # Connect to the database
        conn = connect_to_db()
        
        # Test the connection
        test_connection(conn)
        
        # Example: Execute a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        current_time = cursor.fetchone()
        print(f"Current time: {current_time[0]}")
        
        # Don't forget to close the connection
        cursor.close()
        conn.close()
        print("✓ Connection closed")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

