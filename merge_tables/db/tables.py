"""Table management and setup functions."""

import duckdb


def create_table_firms_zoho(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Create the table_firms_zoho table if it doesn't exist."""
    duck.execute(f"""
        create table if not exists {table_name} (
            rec_id varchar,
            id_zoho varchar
        )
    """)
    print(f"✓ Created/verified {table_name}")


def setup_temp_tables(duck: duckdb.DuckDBPyConnection):
    """Create temporary tables for medisoft_firms and zoho_accounts."""
    duck.execute("""
        begin transaction;

        create or replace temp table medisoft_firms as
        select * replace (trim(name) as name, trim(kuerzel) as kuerzel) 
        from pg.medisoft.table_firmenstruktur;

        create or replace temp table zoho_accounts as
        select Id, trim(Account_Name) as Account_Name 
        from pg.zoho.Accounts;

        commit;
    """)
    print("✓ Created temporary tables for medisoft_firms and zoho_accounts")


def create_clean_account_name_macro(duck: duckdb.DuckDBPyConnection):
    """Create the clean_account_name macro for name cleaning."""
    duck.execute("""
        CREATE OR REPLACE MACRO clean_account_name(str) AS (
            regexp_replace(
                regexp_replace(
                    lower(strip_accents(replace(str, '&#38;', ''))), 
                    '\\b(llc|inc|ltd|corp|corporation|plc|gmbh)\\b', 
                    '', 'g'
                ), 
                '[^a-z0-9]', 
                '', 'g'
            )
        );
    """)
    print("✓ Created clean_account_name macro")


def ensure_all_firms_in_table(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Ensure all medisoft firms have an entry in table_firms_zoho."""
    duck.execute(f"""
        insert into {table_name} (rec_id, id_zoho)
        select rec_id, null
        from medisoft_firms
        where rec_id not in (select rec_id from {table_name})
    """)
    print(f"✓ Ensured all firms have entries in {table_name}")


def print_summary(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Print a summary of matching results."""
    summary = duck.sql(f"""
        select 
            count(*) as total_firms,
            count(id_zoho) as matched_firms,
            count(*) - count(id_zoho) as unmatched_firms
        from {table_name}
    """).fetchone()
    
    total, matched, unmatched = summary
    match_rate = (matched / total * 100) if total > 0 else 0
    
    print("\n--- Summary ---")
    print(f"Total firms: {total}")
    print(f"Matched firms: {matched} ({match_rate:.1f}%)")
    print(f"Unmatched firms: {unmatched}")
