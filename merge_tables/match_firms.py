#!/usr/bin/env python3
"""
Match firms from Medisoft and Zoho based on names and addresses.

This script:
1. Creates the table_firms_zoho table if it doesn't exist
2. Matches firms using a series of configurable matching functions
3. Updates table_firms_zoho with all matches

Matching functions can be dynamically added to the process.
"""

from config import load_config, get_table_name, get_enabled_functions
from db import (
    connect_to_postgres_via_duckdb,
    create_table_firms_zoho,
    setup_temp_tables,
    create_clean_account_name_macro,
    ensure_all_firms_in_table,
    print_summary
)
from matchers import (
    setup_default_matching_functions,
    run_matching_functions,
    MATCHING_FUNCTIONS
)


def main():
    """Main function to orchestrate the matching process."""
    # Load configuration
    load_config()
    table_name = get_table_name()
    enabled_functions_list = get_enabled_functions()
    
    print("Starting firm matching process...")
    print(f"Using table: {table_name}\n")
    
    try:
        # Setup default matching functions
        setup_default_matching_functions()
        
        # Optionally filter by enabled functions from environment
        if enabled_functions_list:
            for match_func in MATCHING_FUNCTIONS:
                if match_func['name'] not in enabled_functions_list:
                    match_func['enabled'] = False
            print(f"Enabled matching functions: {', '.join(enabled_functions_list)}\n")
        
        # Connect to database
        duck = connect_to_postgres_via_duckdb()
        
        # Create table if needed
        create_table_firms_zoho(duck, table_name)
        
        # Setup temporary tables
        setup_temp_tables(duck)
        
        # Create cleaning macro
        create_clean_account_name_macro(duck)
        
        # Run all matching functions
        run_matching_functions(duck, table_name)
        
        # Ensure all firms are in the table
        ensure_all_firms_in_table(duck, table_name)
        
        # Print summary
        print_summary(duck, table_name)
        
        print("\n✓ Firm matching process completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error during firm matching: {e}")
        raise


if __name__ == "__main__":
    main()
