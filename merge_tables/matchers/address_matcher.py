"""Address-based matching functions."""

import duckdb
import pandas as pd
from postal.parser import parse_address
from utils.address_utils import clean_german_road, split_and_clean_house_number


def match_by_address(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Match remaining firms by address."""
    print("\n--- Matching firms by address ---")
    
    # Get unmatched medisoft firms
    med_df = duck.sql(f"""
        select rec_id, name, plz, strasse 
        from medisoft_firms 
        where rec_id not in (
            select rec_id 
            from {table_name} 
            where id_zoho is not null
        )
    """).df()
    
    if len(med_df) == 0:
        print("✓ No unmatched firms to match by address")
        return
    
    print(f"  Processing {len(med_df)} unmatched medisoft firms")
    
    # Clean medisoft data
    med_clean_df = med_df.dropna(ignore_index=True).copy()
    med_clean_df['raw_address'] = (
        med_clean_df['strasse'] + ", " + 
        med_clean_df['plz'] + ", Deutschland"
    )
    med_clean_df['parsed_address'] = (
        med_clean_df['raw_address']
        .dropna()
        .apply(parse_address)
    )
    
    # Extract address components
    med_clean_df = pd.concat([
        med_clean_df, 
        med_clean_df['parsed_address']
        .apply(lambda x: {k: v for v, k in x})
        .apply(pd.Series)[['road', 'house_number', 'postcode']]
    ], axis=1)
    
    med_clean_df['road_cleaned'] = med_clean_df['road'].apply(clean_german_road)
    med_clean_df[['house_num_1', 'house_num_2']] = (
        med_clean_df['house_number'].apply(split_and_clean_house_number)
    )
    
    # Get zoho accounts
    zoho_df = duck.sql("""
        select Id, Account_Name, Billing_Code, Billing_Street 
        from pg.zoho.Accounts
    """).df()
    
    # Clean zoho data
    zoho_df = zoho_df.dropna(ignore_index=True).copy()
    zoho_df['raw_address'] = (
        zoho_df['Billing_Street'] + ", " + 
        zoho_df['Billing_Code'] + ", Deutschland"
    )
    zoho_df['parsed_address'] = zoho_df['raw_address'].apply(parse_address)
    
    # Extract address components
    zoho_df = pd.concat([
        zoho_df, 
        zoho_df['parsed_address']
        .apply(lambda x: {k: v for v, k in x})
        .apply(pd.Series)[['road', 'house_number', 'postcode']]
    ], axis=1)
    
    zoho_df['road_cleaned'] = zoho_df['road'].apply(clean_german_road)
    zoho_df[['house_num_1', 'house_num_2']] = (
        zoho_df['house_number'].apply(split_and_clean_house_number)
    )
    
    # Register DataFrames with DuckDB
    duck.register("med_clean_df", med_clean_df)
    duck.register("zoho_df", zoho_df)
    
    # Match by address
    address_match_df = duck.sql("""
        select 
            name,
            Account_Name,
            jaro_winkler_similarity(
                clean_account_name(name), 
                clean_account_name(Account_Name)
            ) as similarity, 
            med_clean_df.* exclude (name),
            zoho_df.* exclude (Account_Name)
        from med_clean_df
        join zoho_df
        on med_clean_df.road_cleaned = zoho_df.road_cleaned
        and med_clean_df.plz = zoho_df.postcode
        and (
            med_clean_df.house_number = zoho_df.house_number
            or med_clean_df.house_num_1 = zoho_df.house_num_1
            or med_clean_df.house_num_2 = zoho_df.house_num_2
        )
        where (
            similarity > 0.7 
            or (
                clean_account_name(name) in clean_account_name(Account_Name) 
                or clean_account_name(Account_Name) in clean_account_name(name)
            )
        )
        QUALIFY row_number() OVER (
            PARTITION BY med_clean_df.rec_id
            ORDER BY similarity DESC
        ) = 1
        order by similarity desc
    """).df()
    
    if len(address_match_df) == 0:
        print("✓ No address matches found")
        return
    
    print(f"✓ Found {len(address_match_df)} address matches")
    
    # Register address_match_df and update table
    duck.register("address_match_df", address_match_df)
    
    duck.execute(f"""
        update {table_name}
        set id_zoho = address_match_df.Id
        from address_match_df
        where {table_name}.rec_id = address_match_df.rec_id
        and {table_name}.id_zoho is null
    """)
    
    print(f"✓ Updated {table_name} with address matches")
