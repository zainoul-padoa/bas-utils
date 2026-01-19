# Zoho Import Script

These scripts import Deals data from a CSV file into a PostgreSQL database, and import all the tables also.

## Prerequisites

1. Python 3.8 or higher
2. PostgreSQL database with the `zoho."Deals"` table created
3. Required Python packages (install with `pip install -r ../requirements.txt`)

## Setup

1. **Download the CSV file:**
   - Go to: https://docs.google.com/spreadsheets/d/155P7KYbCBtwf9PQeE6ZnjrTnbHl_vNktoLm6dJ7zr5M/edit?gid=518266150#gid=518266150
   - Click on **File → Download → Comma-separated values (.csv)**
   - Save the file as `Deals (business opportunities) - Deals.csv` in the `data/` directory

2. **Configure database connection:**
   - Copy `.env.example` to `.env` in the project root
   - Update the database connection settings in `.env`:
     ```
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=your_database_name
     DB_USER=your_username
     DB_PASSWORD=your_password
     ```

3. **Install dependencies:**
   ```bash
   pip install -r ../requirements.txt
   ```

4. Update parameters in zohoCRM.py from 1Password (Padoa-integration-BAS)

## Refresh the whole Zoho database
1. Go to project root directory
```bash
cd /Users/padoa/Desktop/BAS/bas-utils
```
2. Run zohoCRM.py to get the csv from Zoho API
```bash
python3 -m zoho.zohoCRM
```

3. Move the files into zoho/data
```bash
mv *csv zoho/data
```

4. Check .env file in the parent directory (refer to Setup above)

5. Create a schema zoho_new in the database

6. Import these csv files into database
```bash
python3 -m zoho.import_zoho_tables
```

7. Rename schema zoho to zoho_2026_xx and zoho_new to zoho


## Usage (used before for Deals)

### Basic usage (uses default CSV path):
```bash
python import_deals.py
```

### Specify a custom CSV path:
```bash
python import_deals.py path/to/your/file.csv
```

### Run as executable:
```bash
./import_deals.py
```

## What the script does

1. Connects to the PostgreSQL database
2. Truncates the `zoho."Deals"` table (removes all existing data)
3. Reads the CSV file
4. Processes the data:
   - Converts `Id` column to preserve precision for large integers
   - Converts boolean columns (`AP_ist_Entscheider`, `via_angebote`, `Locked__s`) from false/true to 0/1
5. Inserts all rows into the database
6. Closes the database connection

## File Structure

```
zoho/
├── README.md                    # This file
├── zohoCRM.py                   # Retrieves csv from zoho API
├── import_zoho_tables.py        # Import into database csv files
├── import_deals.py              # Main import script for deals
├── db_connection.py             # Database connection utility for deals
├── data/                        # CSV files directory
│   └── Deals (business opportunities) - Deals.csv
│   └── Accounts.csv
│   └── etc.
└── test.ipynb                   # Jupyter notebook (for testing)
```

## Troubleshooting

### CSV file not found
- Make sure you've downloaded the CSV file from the Google Sheets link
- Verify the file is saved in the `data/` directory
- Check the file name matches exactly: `Deals (business opportunities) - Deals.csv`

### Database connection errors
- Verify your `.env` file has the correct database credentials
- Check that PostgreSQL is running
- Ensure the database and schema exist

### Import errors
- Check that the `zoho."Deals"` table exists in your database
- Verify the CSV file is not corrupted
- Check that you have write permissions on the database

## Notes

- The script will **replace all existing data** in the `zoho."Deals"` table
- Make sure to backup your data before running the import if needed
- The script processes data in batches of 1000 rows for efficiency

