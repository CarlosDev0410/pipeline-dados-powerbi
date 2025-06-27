import pandas as pd
import os
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
data_hoje = date.today()

def get_sqlalchemy_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('ORIGEM_USER')}:{os.getenv('ORIGEM_PASS')}@{os.getenv('ORIGEM_HOST')}:{os.getenv('ORIGEM_PORT')}/{os.getenv('ORIGEM_DB')}"
    )

def get_postgres_engine_dest():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def fetch_dados_estoque():
    engine = get_sqlalchemy_engine()
    query = open("sql/query_estoque.sql", encoding="utf-8").read()
    df = pd.read_sql_query(text(query), con=engine)
    return df

def insert_estoque(df, engine):
    if df.empty:
        print("Nenhum dado de devolução encontrado.")
        return
    df.to_sql("estoque", con=engine, index=False, if_exists="append", method="multi")
    print(f"{len(df)} registros de devolução inseridos.")

def run_etl_estoque():
    print("🔹 Iniciando ETL de devoluções...")
    engine_dest = get_postgres_engine_dest()
    df = fetch_dados_estoque()
    insert_estoque(df, engine_dest)
    print(f"✅ ETL de estoque finalizado. {data_hoje}")
