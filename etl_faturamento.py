import pandas as pd
import os
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_sqlalchemy_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('ORIGEM_USER')}:{os.getenv('ORIGEM_PASS')}@{os.getenv('ORIGEM_HOST')}:{os.getenv('ORIGEM_PORT')}/{os.getenv('ORIGEM_DB')}"
    )

def get_postgres_engine_dest():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def fetch_dados_faturamento(data_inicio, data_fim):
    engine = get_sqlalchemy_engine()
    query = open("sql/query_faturamento.sql", encoding="utf-8").read()
    df = pd.read_sql_query(text(query), con=engine, params={"data_inicio": data_inicio, "data_fim": data_fim})
    return df

def excluir_faturamento_por_intervalo(engine, data_inicio, data_fim):
    with engine.connect() as conn:
        conn.execute(text("""
            DELETE FROM faturamento
            WHERE data_faturamento BETWEEN :inicio AND :fim
        """), {"inicio": data_inicio, "fim": data_fim})
        conn.commit()
    print(f"🗑️ Registros de faturamento entre {data_inicio} e {data_fim} foram excluídos.")

def insert_faturamento(df, engine):
    if df.empty:
        print("Nenhum dado de faturamento encontrado.")
        return
    df.to_sql("faturamento", con=engine, index=False, if_exists="append", method="multi")
    print(f"{len(df)} registros de faturamento inseridos.")

def run_etl_faturamento():
    print("🔹 Iniciando ETL de faturamento...")
    engine_dest = get_postgres_engine_dest()

    data_fim = date.today()
    data_inicio = data_fim - timedelta(days=60)

    excluir_faturamento_por_intervalo(engine_dest, data_inicio, data_fim)

    df = fetch_dados_faturamento(data_inicio, data_fim)
    insert_faturamento(df, engine_dest)
    print(f"✅ ETL de faturamento finalizado {data_fim}.")
