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

### Prio to Firm-linked resources first

#### Move xml to Archiv folder
First we inject only these xml:
- Firmenstruktur.xml
- Beschaeftigte.xml
- Untersuchungen.xml
- Kartei.xml
- Erbrachte_Werte.xml
- Ap_Historie.xml
- Impfungen.xml

These are all tables that have a column referencing to firm's medisoft ID

Move only these to Archiv folder.

#### Apply next section steps 
Apply steps from 
Generate the SQLite database
until 
Data is loaded in schema "public"

This will inject the tables 

#### Pre-clean 
Apply merge medisoft
```
medisoft/merge_medisoft_duplicate_firms.sql
```

#### Generate Easybill_Medisoft delta
Now we can generate the Excel sheet for BAS for them to match the new Medisoft to Easybill firms.

#### Go to the next section for the rest of xml files, excluding the ones above already injected

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

### Post-clean

#### Generate IDs for independent

#### Generate IDs for new Medisoft (children or no_easybill_match)

#### Update cleaned_medisoft

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
