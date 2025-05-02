# import library
import requests
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

# load environment
try:
    env_path = Path(__file__).resolve().parent.parent / "cred.env"
except NameError:
    env_path = Path().resolve().parent / "cred.env"

load_dotenv(dotenv_path=env_path)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

API_URL = "https://data.jabarprov.go.id/api-backend/bigdata/disdukcapil_2/od_15922_jml_penduduk__kelompok_pekerjaan_jk_v2?limit=500"
SCHEMA_NAME = "dbt"
TABLE_NAME = "westjava_occupation"

# get data
def fetch_data(url):
    print("Fetching data from API...")
    response = requests.get(url)
    if response.status_code == 200:
        print("Data fetched")
        json_data = response.json()
        if isinstance(json_data, dict):
            return json_data.get('data') or json_data.get('result')
        elif isinstance(json_data, list):
            return json_data
        else:
            raise ValueError("Unexpected JSON structure.")
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# connect to db
def connect_db(config):
    return psycopg2.connect(**config)

# create table
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
            year INTEGER,
            updated_at TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
            UNIQUE (province_name, occupation_group, gender, year)
        )
    """)

# insert data
def insert_data(cursor, schema, table, data):
    for item in data:
        cursor.execute(f"""
            INSERT INTO {schema}.{table} 
            (province_name, occupation_group, gender, value, unit, year)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (province_name, occupation_group, gender, year)
            DO UPDATE SET updated_at = (NOW() AT TIME ZONE 'UTC')
        """, (
            item.get('nama_provinsi'),
            item.get('kelompok_pekerjaan'),
            item.get('jenis_kelamin'),
            item.get('jumlah_penduduk'),
            item.get('satuan'),
            item.get('tahun')
        ))
        
# get the data
def main():
    try:
        data = fetch_data(API_URL)
        conn = connect_db(DB_CONFIG)
        cursor = conn.cursor()

        create_table(cursor, SCHEMA_NAME, TABLE_NAME)
        insert_data(cursor, SCHEMA_NAME, TABLE_NAME, data)
        conn.commit()

        print("Data inserted")
    except Exception as e:
        print("Error:", e)
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    main()