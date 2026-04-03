import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import json

# Load credentials from .env file
load_dotenv()

# --- Pull hero data from OpenDota API ---
print("Fetching heroes from OpenDota API...")
response = requests.get("https://api.opendota.com/api/heroes")
heroes = response.json()

# Convert to a pandas DataFrame
df = pd.DataFrame(heroes)
print(f"Fetched {len(df)} heroes")
print(df.head())

# --- Connect to Snowflake ---
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
)

cursor = conn.cursor()

# --- Drop and recreate table in Snowflake ---
cursor.execute("DROP TABLE IF EXISTS raw.heroes")
cursor.execute("""
    CREATE TABLE raw.heroes (
        id INTEGER,
        name VARCHAR,
        localized_name VARCHAR,
        primary_attr VARCHAR,
        attack_type VARCHAR,
        roles VARCHAR
    )
""")

# --- Insert data row by row ---
print("Loading data into Snowflake...")
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO raw.heroes 
        (id, name, localized_name, primary_attr, attack_type, roles)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row.get("id"),
        row.get("name"),
        row.get("localized_name"),
        row.get("primary_attr"),
        row.get("attack_type"),
        json.dumps(row.get("roles"))
    ))

conn.commit()
cursor.close()
conn.close()
print("Done! Heroes loaded into Snowflake successfully.")