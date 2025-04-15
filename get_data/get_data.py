# Get data from: 
# Detail:

import requests
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

try:
    # If running from a script
    env_path = Path(__file__).resolve().parent.parent / "cred.env"
except NameError:
    # If running from Jupyter Notebook or interactive shell
    env_path = Path().resolve().parent / "cred.env"

load_dotenv(dotenv_path=env_path)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Optional: Specify schema name
API_URL = "https://data.jabarprov.go.id/api-backend/bigdata/disdukcapil_2/od_15922_jml_penduduk__kelompok_pekerjaan_jk_v2?limit=500"
SCHEMA_NAME = "dbt"
TABLE_NAME = "westjava_occupation"

# === 2. Fetch Data from API ===
def fetch_data(url):
    print("Fetching data from API...")
    response = requests.get(url)
    if response.status_code == 200:
        print("Data fetched successfully.")
        json_data = response.json()
        
        # Try common keys: 'data', 'result', etc.
        if isinstance(json_data, dict):
            if 'data' in json_data:
                return json_data['data']
            elif 'result' in json_data:
                return json_data['result']
            else:
                raise ValueError("Expected 'data' key in JSON response.")
        elif isinstance(json_data, list):
            return json_data
        else:
            raise ValueError("Unexpected JSON structure.")
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# === 3. Connect to PostgreSQL ===
def connect_db(config):
    return psycopg2.connect(**config)

# === 4. Create Schema & Table ===
def create_table(cursor, schema, table):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id SERIAL PRIMARY KEY,
            province_name TEXT,
            occupation_group TEXT,
            gender TEXT,
            value INTEGER,
            unit TEXT,
            year INTEGER
        )
    """)

# === 5. Insert Data ===
def insert_data(cursor, schema, table, data):
    for item in data:
        cursor.execute(f"""
            INSERT INTO {schema}.{table} (id, province_name, occupation_group, gender, value, unit, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            item.get('id'),
            item.get('nama_provinsi'),
            item.get('kelompok_pekerjaan'),
            item.get('jenis_kelamin'),
            item.get('jumlah_penduduk'),
            item.get('satuan'),
            item.get('tahun')
        ))

# === 6. Main ===
def main():
    try:
        data = fetch_data(API_URL)
        conn = connect_db(DB_CONFIG)
        cursor = conn.cursor()

        create_table(cursor, SCHEMA_NAME, TABLE_NAME)
        insert_data(cursor, SCHEMA_NAME, TABLE_NAME, data)
        conn.commit()

        print("✅ Data successfully inserted into PostgreSQL.")
    except Exception as e:
        print("❌ Error:", e)
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    main()