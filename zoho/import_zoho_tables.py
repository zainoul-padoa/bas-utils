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
			df = pd.read_csv(f'{DATA_PATH}/{csv_file}')
			table_name = os.path.splitext(csv_file)[0]
			print(f"Inserting {table_name} into database...")
			df.to_sql(table_name, con=connection, schema=SCHEMA_NAME, index=False)
			print(f"✓ Table {table_name} inserted")
	connection.close()
