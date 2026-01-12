# Firm Matching Script

This script matches firms from Medisoft and Zoho databases based on names and addresses, with support for dynamically adding custom matching functions.

## Project Structure

```
merge_tables/
├── match_firms.py                   # Main entry point
├── config.py                        # Configuration management
├── README.md                        # This file
├── db/                              # Database operations
│   ├── __init__.py
│   ├── connection.py                # Database connection
│   └── tables.py                   # Table setup and management
├── matchers/                        # Matching functions
│   ├── __init__.py                 # Registry and base functions
│   ├── name_matcher.py             # Name-based matching
│   └── address_matcher.py          # Address-based matching
└── utils/                          # Utility functions
    ├── __init__.py
    └── address_utils.py            # Address processing utilities
```

## Features

- **Pluggable matching functions**: Easily add new matching strategies
- **Configurable table name**: Set via `TABLE_FIRMS_ZOHO` environment variable
- **Selective function execution**: Enable/disable specific matching functions
- **Sequential matching**: Functions run in order, only matching unmatched firms
- **Modular architecture**: Clean separation of concerns

## Usage

### Basic Usage

```bash
python match_firms.py
```

### Configuration via Environment Variables

Create a `.env` file in the parent directory or set environment variables:

```bash
# Database connection
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password

# Table name (default: pg.medisoft.table_firms_zoho)
TABLE_FIRMS_ZOHO=pg.medisoft.table_firms_zoho

# Enable specific matching functions (comma-separated, optional)
ENABLED_MATCHING_FUNCTIONS=name_match,address_match
```

## Default Matching Functions

1. **name_match**: Matches firms by name using cleaned names and Jaro-Winkler similarity (>0.95)
2. **address_match**: Matches firms by address (road, postcode, house number) with name similarity check

## Adding Custom Matching Functions

### Method 1: Create a new matcher file

Create a new file in the `matchers/` directory:

```python
# matchers/phone_matcher.py
import duckdb
from matchers import register_matching_function

def match_by_phone(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Match firms by phone number."""
    print("\n--- Matching firms by phone ---")
    # Your matching logic here
    duck.execute(f"""
        update {table_name}
        set id_zoho = ...
        where {table_name}.id_zoho is null
    """)

# Auto-register when imported
register_matching_function(
    name="phone_match",
    func=match_by_phone,
    description="Match by phone number"
)
```

Then import it in `matchers/__init__.py`:

```python
# In matchers/__init__.py, add:
from .phone_matcher import *  # This registers the function
```

### Method 2: Register programmatically

```python
from matchers import register_matching_function

def my_custom_matcher(duck, table_name):
    # Your logic
    pass

register_matching_function(
    name="my_matcher",
    func=my_custom_matcher,
    description="My custom matching"
)
```

## Matching Function Requirements

Each matching function must:

1. **Accept two parameters**: `(duck: duckdb.DuckDBPyConnection, table_name: str)`
   - `duck`: DuckDB connection object
   - `table_name`: Name of the target table

2. **Only match unmatched firms**: Always check `where id_zoho is null` when updating

3. **Handle errors gracefully**: Use try/except for optional columns or features

4. **Provide feedback**: Print progress messages

Example template:

```python
def my_matcher(duck: duckdb.DuckDBPyConnection, table_name: str):
    """Match firms by some criteria."""
    print("\n--- Matching by X ---")
    
    # Ensure all firms have entries
    duck.execute(f"""
        insert into {table_name} (rec_id, id_zoho)
        select rec_id, null
        from medisoft_firms
        where rec_id not in (select rec_id from {table_name})
    """)
    
    # Your matching logic - only update unmatched firms
    duck.execute(f"""
        update {table_name}
        set id_zoho = ...
        where {table_name}.id_zoho is null
        and ... your conditions ...
    """)
    
    # Report results
    count = duck.sql(f"select count(*) from {table_name} where id_zoho is not null").fetchone()[0]
    print(f"✓ Updated matches (total matched: {count})")
```

## Module Overview

### `config.py`
- Environment variable loading
- Configuration getters (table name, DB config, enabled functions)

### `db/connection.py`
- DuckDB to PostgreSQL connection setup

### `db/tables.py`
- Table creation and management
- Temporary table setup
- SQL macro creation
- Summary printing

### `matchers/__init__.py`
- Matching function registry
- Function execution orchestrator
- Default function registration

### `matchers/name_matcher.py`
- Name-based matching using Jaro-Winkler similarity

### `matchers/address_matcher.py`
- Address parsing and matching
- Road, postcode, and house number matching

### `utils/address_utils.py`
- German road name cleaning
- House number parsing (handles ranges)

## Available Helper Functions

- `clean_account_name(str)`: SQL macro for cleaning account names
- `clean_german_road(text)`: Python function for cleaning German road names
- `split_and_clean_house_number(val)`: Python function for parsing house numbers

## Available Temporary Tables

After `setup_temp_tables()`:
- `medisoft_firms`: All medisoft firms with trimmed names
- `zoho_accounts`: All zoho accounts with trimmed names

## Execution Order

1. Database connection setup
2. Table creation
3. Temporary table setup
4. Macro creation
5. **Matching functions** (in registration order)
6. Ensure all firms have entries
7. Print summary

## Output

The script provides:
- Progress messages for each step
- Match counts for each function
- Final summary with total, matched, and unmatched counts

## Troubleshooting

- **No matches found**: Check that your matching criteria aren't too strict
- **Functions not running**: Verify they're registered and enabled
- **Database errors**: Check connection settings in `.env`
- **Column errors**: Ensure columns exist in both source tables
- **Import errors**: Make sure you're running from the `merge_tables/` directory or adjust import paths

## Development

To add a new matching function:

1. Create a new file in `matchers/` (e.g., `matchers/email_matcher.py`)
2. Implement your matching function
3. Register it in the file or in `matchers/__init__.py`
4. Import it in `matchers/__init__.py` if needed
5. Test by running `python match_firms.py`
