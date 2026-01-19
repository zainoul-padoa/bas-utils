import os 

from sqlalchemy import create_engine, engine
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()

# === CONFIGURATION ===
# Loads from .env file or environment variables
DB_CONFIG = {
	'host': os.getenv('DB_HOST', 'localhost'),
	'port': os.getenv('DB_PORT', '5432'),
	'database': os.getenv('DB_NAME', 'medisoft'),
	'user': os.getenv('DB_USER', 'postgres'),
	'password': os.getenv('DB_PASSWORD', '')
}
# ======================

def connect_to_db(config: Optional[dict] = None) -> engine:

	if config is None:
		config = DB_CONFIG

	url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
	eng = create_engine(url)
	conn = eng.connect()

	return conn
