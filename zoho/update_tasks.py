#!/usr/bin/env python3
"""
Update Tasks data in PostgreSQL from CSV file.

This script reads a CSV file containing Tasks data, connects to PostgreSQL via DuckDB,
and updates existing Tasks records by matching on Id and updating What_Id and Who_id fields.
"""

import sys
import os
import duckdb
from dotenv import load_dotenv


def connect_to_postgres_via_duckdb():
    """
    Connect DuckDB to PostgreSQL database.
    
    Returns:
        duckdb.DuckDBPyConnection: DuckDB connection with PostgreSQL attached
    """
    # Load environment variables
    load_dotenv()
    
    # Create DuckDB connection
    duck = duckdb.connect()
    
    # Install and load the postgres extension
    duck.install_extension('postgres')
    duck.load_extension('postgres')
    
    # Database configuration
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'postgres')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')
    
    # Attach PostgreSQL database to DuckDB
    duck.sql(f"""
        ATTACH 'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}' 
        AS postgres_db (TYPE POSTGRES)
    """)
    
    print(f"✓ Successfully connected DuckDB to PostgreSQL database '{db_name}'")
    return duck


def update_tasks(duck, csv_path):
    """
    Update Tasks table in PostgreSQL with data from CSV file.
    
    Args:
        duck: DuckDB connection with PostgreSQL attached
        csv_path: Path to the CSV file containing Tasks data
    """
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Get count before update
    original_count = duck.sql('SELECT COUNT(*) FROM postgres_db.zoho.Tasks').fetchone()[0]
    print(f"Original Tasks table row count: {original_count}")
    
    # Update Tasks table
    print(f"Updating Tasks from CSV file: {csv_path}")
    result = duck.sql(f"""
        UPDATE postgres_db.zoho.Tasks t1
        SET What_Id = t2.What_Id,
            Who_id = t2.Who_id
        FROM (
            SELECT 
                try_cast(Id as int64) as Id, 
                try_cast(What_Id as int64) as What_Id, 
                try_cast(Who_id as int64) as Who_id 
            FROM read_csv('{csv_path}')
        ) t2
        WHERE t1.Id = t2.Id
    """)
    
    # Get count of updated rows
    updated_count = result.fetchone()[0] if hasattr(result, 'fetchone') else None
    
    print("✓ Update completed")
    if updated_count is not None:
        print(f"✓ Rows updated: {updated_count}")


def main():
    """Main function to run the update process."""
    # Default CSV path
    csv_path = 'data/Tasks - Tasks.csv'
    
    # Allow CSV path to be passed as command-line argument
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    # Check if file is readable
    if not os.access(csv_path, os.R_OK):
        print(f"❌ Error: Cannot read CSV file: {csv_path}")
        print("Please check file permissions.")
        sys.exit(1)
    
    duck = None
    try:
        # Connect to PostgreSQL via DuckDB
        print("Connecting to PostgreSQL via DuckDB...")
        duck = connect_to_postgres_via_duckdb()
        
        # Update Tasks table
        update_tasks(duck, csv_path)
        
        print("\n✓ Update completed successfully!")
        
    except FileNotFoundError as e:
        print(f"\n❌ File error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Close the DuckDB connection
        if duck:
            duck.close()
            print("✓ DuckDB connection closed")


if __name__ == "__main__":
    main()

