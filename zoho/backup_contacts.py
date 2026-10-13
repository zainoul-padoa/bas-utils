#!/usr/bin/env python3
"""
Backup zoho.Contacts table from PostgreSQL using DuckDB.

This script connects to PostgreSQL via DuckDB and creates a backup of the zoho.Contacts table.
The backup can be saved in multiple formats: Parquet (default), CSV, or DuckDB database.
"""

import sys
import os
import duckdb
from datetime import datetime
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


def backup_contacts(duck, output_path=None, format='parquet'):
    """
    Backup zoho.Contacts table from PostgreSQL.
    
    Args:
        duck: DuckDB connection with PostgreSQL attached
        output_path: Path for the backup file (optional, will generate timestamped name if not provided)
        format: Backup format - 'parquet', 'csv', or 'duckdb' (default: 'parquet')
    """
    # Check if table exists
    try:
        count_result = duck.sql('SELECT COUNT(*) FROM postgres_db.zoho.Contacts').fetchone()
        row_count = count_result[0]
        print(f"Found {row_count:,} rows in zoho.Contacts table")
    except Exception as e:
        raise Exception(f"Error accessing zoho.Contacts table: {e}")
    
    # Generate output path if not provided
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if format == 'parquet':
            output_path = f'data/Contacts_backup_{timestamp}.parquet'
        elif format == 'csv':
            output_path = f'data/Contacts_backup_{timestamp}.csv'
        elif format == 'duckdb':
            output_path = f'data/Contacts_backup_{timestamp}.duckdb'
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    
    # Create backup based on format
    print(f"Creating backup in {format.upper()} format...")
    
    if format == 'parquet':
        duck.sql(f"""
            COPY (SELECT * FROM postgres_db.zoho.Contacts)
            TO '{output_path}' (FORMAT PARQUET)
        """)
    elif format == 'csv':
        duck.sql(f"""
            COPY (SELECT * FROM postgres_db.zoho.Contacts)
            TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',')
        """)
    elif format == 'duckdb':
        # Create a new DuckDB database file and copy the table
        backup_duck = duckdb.connect(output_path)
        # Attach PostgreSQL to the backup connection
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'postgres')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', '')
        backup_duck.install_extension('postgres')
        backup_duck.load_extension('postgres')
        backup_duck.sql(f"""
            ATTACH 'host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}' 
            AS postgres_db (TYPE POSTGRES)
        """)
        # Copy the table to the backup database
        backup_duck.sql(f"""
            CREATE TABLE contacts_backup AS 
            SELECT * FROM postgres_db.zoho.Contacts
        """)
        backup_duck.close()
    
    # Get file size
    file_size = os.path.getsize(output_path)
    file_size_mb = file_size / (1024 * 1024)
    
    print(f"✓ Backup completed successfully!")
    print(f"  Output file: {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    print(f"  Rows backed up: {row_count:,}")
    
    return output_path


def main():
    """Main function to run the backup process."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Backup zoho.Contacts table from PostgreSQL using DuckDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backup to Parquet (default)
  python backup_contacts.py
  
  # Backup to CSV
  python backup_contacts.py --format csv
  
  # Backup to specific file
  python backup_contacts.py --output data/my_backup.parquet
  
  # Backup to DuckDB format
  python backup_contacts.py --format duckdb
        """
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output file path (default: auto-generated with timestamp)'
    )
    
    parser.add_argument(
        '--format', '-f',
        type=str,
        choices=['parquet', 'csv', 'duckdb'],
        default='parquet',
        help='Backup format (default: parquet)'
    )
    
    args = parser.parse_args()
    
    duck = None
    try:
        # Connect to PostgreSQL via DuckDB
        print("Connecting to PostgreSQL via DuckDB...")
        duck = connect_to_postgres_via_duckdb()
        
        # Create backup
        backup_contacts(duck, args.output, args.format)
        
        print("\n✓ Backup completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
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
