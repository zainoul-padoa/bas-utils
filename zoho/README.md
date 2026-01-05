# Zoho Deals Import Script

This script imports Deals data from a CSV file into a PostgreSQL database.

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

## Usage

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
├── import_deals.py              # Main import script
├── db_connection.py             # Database connection utility
├── data/                        # CSV files directory
│   └── Deals (business opportunities) - Deals.csv
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

