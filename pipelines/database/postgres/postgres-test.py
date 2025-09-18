import dlt
import os
import psycopg2
import pandas as pd
from dlt.sources.sql_database import sql_database

# --- Configuration ---
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", 5432)
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

S3_BUCKET_URL = os.environ.get("DESTINATION__FILESYSTEM__BUCKET_URL")

# --- Définir le pipeline DLT ---
pipeline = dlt.pipeline(
    pipeline_name="postgres_to_s3_pipeline",
    destination="filesystem",
    dataset_name="postgres_data",
    full_refresh=False
)

# --- Connexion à PostgreSQL ---
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

# # --- Exemple pour une table "customers" ---
# query = "select * from pokemon_showdown_latest.full_ability;"
# df = pd.read_sql(query, conn)

# # --- Charger les données avec DLT ---
# pipeline.run(df, table_name="full_abilities")

source = sql_database(
    credentials=f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
    schema="pokemon_showdown_latest",
    table_names=["full_ability"],   # DLT gère incrémental
)

pipeline.run(source)

# --- Optionnel : upload vers S3 en utilisant DLT FilesystemDestination avec bucket S3 ---
# Si tu veux écrire directement sur S3, tu peux faire :
# pipeline = dlt.pipeline(
#     pipeline_name="postgres_to_s3_pipeline",
#     destination="filesystem",
#     dataset_name="postgres_data",
#     full_refresh=False
# )
# pipeline.run(df, table_name="customers", destination=S3_BUCKET_URL)