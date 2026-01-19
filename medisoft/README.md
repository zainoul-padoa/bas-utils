# Medisoft Import xml to Postgres

These scripts transform the xml files into a sqlite database, and then we import the output.db in the postgres database.

## Prerequisites

1. Python 3.8 or higher
2. Required Python packages (install with `pip install -r ../requirements.txt`)
3. pgloader must be installed on the computer
4. db.load file must be updated

## Setup

1. **Download the XML files:**
   - Medisoft sends to us a dump of their database in the form of xml files (one per table)
   - Move the files under the Archiv folder

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

### Generate the SQLite database
```bash
python3 xml_to_db.py
```
### Load it with pgloader into the postgres database
```bash
PGSSLMODE=allow pgloader --verbose db.load
```

### Data is loaded in schema "public"
- Rename medisoft to medisoft_2026_xx and public to medisoft (with DBeaver or Datagrip)
- Create a new public schema

## What the script does

1. Creates a SQLite database with a file output.db
2. Iterates over the xml files in Archiv folder
3. Creates a table and insert data from each xml file
4. pgloader allows to load the data in 

## File Structure

```
medisoft/
├── README.md                    # This file
├── xml_to_db.py                 # Transforms xml files to SQLite db
├── db.load                      # pgloader config file
├── import_deals.py              # Main import script for deals
├── xml_to_db_inefficient.py     # Loads directly from xml to postgres but inefficient
├── Archiv/                      # CSV files directory
│   └── Beschaeftigte.xml
│   └── Anhang.xml
│   └── Firmenstruktur.xml
│   └── etc.
└── test.ipynb                   # Jupyter notebook (for testing)
```

## Troubleshooting

### Database connection errors
- Verify your `.env` file has the correct database credentials
- Check that PostgreSQL is running
- Ensure the database and schema exist