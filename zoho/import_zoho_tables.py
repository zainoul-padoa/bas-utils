import pandas as pd
import os
import connection_alchemy

DATA_PATH = './zoho/data'
SCHEMA_NAME = 'zoho_new'

if __name__ == "__main__":
	connection = connection_alchemy.connect_to_db()

	# iterate over csv files located in the directory data
	for csv_file in os.listdir(DATA_PATH):
		# check that it's a csv file
		if csv_file[-3:] == 'csv':
			df = pd.read_csv(f'{DATA_PATH}/{csv_file}', dtype=str)
			table_name = os.path.splitext(csv_file)[0]
			print(f"Inserting {table_name} into database...")
			df.to_sql(table_name, con=connection, schema=SCHEMA_NAME, index=False)
			print(f"✓ Table {table_name} inserted")

	# Add "Formula Status (Victor)" to ACCOUNTS
	print(f"Add columln Formula Status (Victor) to Accounts...")
		# create column
	connection.execute('''alter table zoho."Accounts" 
						add "Formula Status (Victor)" text''')
		# no active Deal by default
	connection.execute('''update zoho."Accounts" a 
						set "Formula Status (Victor)" = ''No Active Deal''
					''')
		# active deal if there is a Deal around today
	connection.execute('''update zoho."Accounts" 
						set "Formula Status (Victor)" = ''Active Deal''
						where "Id" in (
						select a."Id"
						from  zoho."Accounts" a left join zoho."Deals" d  on d."Account_Name" = a."Id" 
						where cast(now() as timestamp) between cast(d."Datum_Vertragsbeginn" as timestamp) and cast(d."Datum_Vertragsende" as timestamp) ) 
					''')
	print("✓ Column Formula Status (Victor) inserted in Accounts")

	# Add "Account Status (Formula Victor)" to DEALS
		# create column
	connection.execute('''alter table zoho."Deals" 
							add "Account Status (Formula Victor)" text''')
		# no active deal by default
	connection.execute('''update zoho."Deals" 
							set "Account Status (Formula Victor)"  = ''No Active Deal''
						''')
		# active deal is the company has an active deal (cf. Accounts)
	connection.execute('''update zoho."Deals" 
						set "Account Status (Formula Victor)" = ''Active Deal''
						where "Account_Name" in (
						select a."Id"  from zoho."Accounts" a where "Formula Status (Victor)" = ''Active Deal''
						)
					''')

	connection.close()
