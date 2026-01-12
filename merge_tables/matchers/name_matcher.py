"""Name-based matching functions."""

import duckdb


def match_by_name(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Match firms by name using cleaned names and Jaro-Winkler similarity."""
    print("\n--- Matching firms by name ---")
    
    # Ensure all firms have entries in the table first
    duck.execute(f"""
        insert into {table_name} (rec_id, id_zoho)
        select rec_id, null
        from medisoft_firms
        where rec_id not in (select rec_id from {table_name})
    """)
    
    # Create text_matched table
    duck.execute("""
        create or replace temp table text_matched as
        with medisoft_cleaned as (
            select 
                rec_id,
                clean_account_name(coalesce(name, kuerzel)) as clean_name
            from medisoft_firms
        ), zoho_cleaned as (
            select
                Id,
                clean_account_name(Account_Name) as clean_name
            from pg.zoho.Accounts
        )
        select 
            m.clean_name as mc, 
            z.clean_name as zc, 
            jaro_winkler_similarity(m.clean_name, z.clean_name) as sim, 
            rec_id, 
            Id
        from medisoft_cleaned as m
            inner join zoho_cleaned as z
            on (m.clean_name = z.clean_name 
                or jaro_winkler_similarity(m.clean_name, z.clean_name) > 0.95)
        QUALIFY row_number() OVER (PARTITION BY m.rec_id ORDER BY sim DESC) = 1
        order by sim
    """)
    
    # Count matches
    match_count = duck.sql("select count(*) from text_matched").fetchone()[0]
    print(f"✓ Found {match_count} name matches")
    
    # Update only unmatched firms
    duck.execute(f"""
        update {table_name}
        set id_zoho = text_matched.Id
        from text_matched
        where {table_name}.rec_id = text_matched.rec_id
        and {table_name}.id_zoho is null
    """)
    
    updated_count = duck.sql(f"""
        select count(*) 
        from {table_name} 
        where id_zoho is not null
    """).fetchone()[0]
    print(f"✓ Updated {match_count} name matches in {table_name} (total matched: {updated_count})")
