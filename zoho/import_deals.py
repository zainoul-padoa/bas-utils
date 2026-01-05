#!/usr/bin/env python3
"""
Import Deals data from CSV to PostgreSQL database.

This script reads a CSV file containing Deals data, processes it,
and inserts it into the PostgreSQL database.
"""

import sys
import os
import pandas as pd
import numpy as np
import duckdb
from psycopg2.extras import execute_values
from psycopg2 import Error as Psycopg2Error
from db_connection import connect_to_db


def process_dataframe(df):
    """
    Process the DataFrame to prepare it for database insertion.
    
    Args:
        df: pandas DataFrame to process
        
    Returns:
        Processed pandas DataFrame
        
    Raises:
        ValueError: If DataFrame is empty or required columns are missing
    """
    if df.empty:
        raise ValueError("Error: CSV file is empty. Please check the file and try again.")
    
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {len(df.columns)}")
    
    try:        
        df[['via_angebote', 'Locked__s', 'AP_ist_Entscheider']] = df[['via_angebote', 'Locked__s', 'AP_ist_Entscheider']].astype(float)
        return df
    except Exception as e:
        raise ValueError(f"Error processing DataFrame: {str(e)}")


def insert_data(conn, df):
    """
    Insert DataFrame data into PostgreSQL database.
    
    Args:
        conn: psycopg2 database connection
        df: pandas DataFrame to insert
        
    Raises:
        Psycopg2Error: If database operation fails
        ValueError: If DataFrame is empty
    """
    if df.empty:
        raise ValueError("Error: Cannot insert empty DataFrame into database.")
    
    # Get column names in the correct order (matching database schema)
    columns = df.columns.tolist()
    columns_str = ', '.join([f'"{col}"' for col in columns])
    
    # Prepare data for insertion
    # Replace NaN/None with None (SQL NULL)
    df = df.replace({np.nan: None, pd.NA: None})
    
    # Convert DataFrame to list of tuples
    values = [tuple(row) for row in df.values]
    
    # Build the INSERT query
    insert_query = f'INSERT INTO zoho."Deals" ({columns_str}) VALUES %s'
    
    try:
        # Use execute_values for efficient bulk insert
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                insert_query,
                values,
                template=None,
                page_size=1000  # Insert in batches of 1000
            )
            conn.commit()
        
        print(f"✓ Inserted {len(df)} rows into PostgreSQL using psycopg2")
    except Psycopg2Error as e:
        raise Psycopg2Error(f"Database error during insert: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error during insert: {str(e)}")


def main():
    """Main function to run the import process."""
    duck = duckdb.connect()

    # Default CSV path
    csv_path = 'data/Deals (business opportunities) - Deals.csv'
    
    # Allow CSV path to be passed as command-line argument
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found: {csv_path}")
        print("\nPlease download the CSV file from:")
        print("https://docs.google.com/spreadsheets/d/155P7KYbCBtwf9PQeE6ZnjrTnbHl_vNktoLm6dJ7zr5M/edit?gid=518266150#gid=518266150")
        print(f"\nSave it as: {csv_path}")
        sys.exit(1)
    
    # Check if file is readable
    if not os.access(csv_path, os.R_OK):
        print(f"❌ Error: Cannot read CSV file: {csv_path}")
        print("Please check file permissions.")
        sys.exit(1)
    
    conn = None
    try:
        # Connect to database
        print("Connecting to database...")
        conn = connect_to_db()
        print("✓ Successfully connected to database")
        
        # Truncate the table first
        print("Truncating table...")
        try:
            with conn.cursor() as cursor:
                cursor.execute('TRUNCATE TABLE zoho."Deals"')
                conn.commit()
            print("✓ Table truncated")
        except Psycopg2Error as e:
            raise Psycopg2Error(f"Error truncating table: {str(e)}\n"
                              f"Please verify that the table 'zoho.\"Deals\"' exists in your database.")
        
        # Read CSV file directly with pandas
        print(f"Reading CSV file: {csv_path}")
        df = duck.sql(f"select * from read_csv('{csv_path}')").df()
        
        # Process the DataFrame
        print("Processing DataFrame...")
        df = process_dataframe(df)
        
        # Insert data into database
        print("Inserting data into database...")
        insert_data(conn, df)
        
        print("\n✓ Import completed successfully!")
        
    except Psycopg2Error as e:
        print(f"\n❌ Database error: {str(e)}")
        if conn:
            conn.rollback()
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ {str(e)}")
        if conn:
            conn.rollback()
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\n❌ File error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
        sys.exit(1)
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("✓ Database connection closed")


if __name__ == "__main__":
    main()

