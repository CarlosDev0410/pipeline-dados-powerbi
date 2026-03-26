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

def fetch_dados_faturamento(data_inicio, data_ref):
    engine = get_sqlalchemy_engine()
    query = open("sql/query_faturamento.sql", encoding="utf-8").read()
    # Puxa o 60-day window mas filtrando por dtaltera >= data_ref (incremental)
    df = pd.read_sql_query(text(query), con=engine, params={"data_inicio": data_inicio, "data_ref": data_ref})
    return df

def upsert_faturamento(df, engine):
    if df.empty:
        print("Nenhum dado novo ou alterado para faturamento.")
        return
    
    # 1. Carrega o delta para uma tabela de staging (temporária)
    df.to_sql("faturamento_staging", con=engine, index=False, if_exists="replace")
    
    with engine.connect() as conn:
        # 2. Deleta da principal os registros que vamos atualizar
        # Usamos a combinação de nota e identificacao (material) como chave única
        conn.execute(text("""
            DELETE FROM faturamento
            WHERE (id_pedido, identificacao) IN (
                SELECT id_pedido, identificacao FROM faturamento_staging
            )
        """))
        
        # 3. Insere os registros novos/atualizados da staging para a principal
        conn.execute(text("INSERT INTO faturamento SELECT * FROM faturamento_staging"))
        conn.commit()
        
    print(f"✅ {len(df)} registros de faturamento upserted com sucesso.")

def run_etl_faturamento():
    print("🔹 Iniciando ETL de faturamento (v1.3 - Somente Hoje)...")
    engine_dest = get_postgres_engine_dest()

    # Foca apenas no dia de hoje para carga rápida
    data_inicio = date.today()
    # data_ref pode ser hoje também, pois dtaltera >= hoje pega os novos de hoje
    data_ref = date.today()

    df = fetch_dados_faturamento(data_inicio, data_ref)
    upsert_faturamento(df, engine_dest)
    print(f"✅ ETL de faturamento do dia finalizado.")
