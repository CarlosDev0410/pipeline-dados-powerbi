import pandas as pd
import os
import time
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_sqlalchemy_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('ORIGEM_USER')}:{os.getenv('ORIGEM_PASS')}@{os.getenv('ORIGEM_HOST')}:{os.getenv('ORIGEM_PORT')}/{os.getenv('ORIGEM_DB')}"
    )

def get_postgres_engine_dest():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def fetch_dados_completos():
    engine = get_sqlalchemy_engine()
    query = open("sql/query_incremental.sql", encoding="utf-8").read()
    data_hoje = date.today()
    df = pd.read_sql_query(text(query), con=engine, params={"dataref": data_hoje})
    if "data_faturamento" in df.columns:
        df["data_faturamento"] = pd.to_datetime(df["data_faturamento"]).dt.date
    return df

def limpar_tabela_faturamento(engine):
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM faturamento;"))
        conn.commit()
    print("⚠️ Tabela faturamento limpa com sucesso.")

def insert_new_data(df, engine):
    if df.empty:
        print("Nenhum dado encontrado.")
        return
    df.to_sql(
        "faturamento",
        con=engine,
        index=False,
        if_exists="append",
        method="multi"
    )
    print(f"{len(df)} registros inseridos via SQLAlchemy.")

def main():
    engine_dest = get_postgres_engine_dest()
    print("🔄 Iniciando carga completa...")

    inicio = time.time()
    df = fetch_dados_completos()
    print(f"✅ Extração finalizada. {len(df)} registros extraídos em {round(time.time() - inicio, 2)}s")

    inicio_insert = time.time()
    insert_new_data(df, engine_dest)
    print(f"✅ Inserção concluída em {round(time.time() - inicio_insert, 2)}s")

if __name__ == "__main__":
    main()
