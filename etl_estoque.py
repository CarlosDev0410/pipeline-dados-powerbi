import pandas as pd
import os
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_sqlalchemy_engine():
    # Adicionado um statement_timeout de 5 minutos (300.000 ms) para proteger contra queries infinitas
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('ORIGEM_USER')}:{os.getenv('ORIGEM_PASS')}@{os.getenv('ORIGEM_HOST')}:{os.getenv('ORIGEM_PORT')}/{os.getenv('ORIGEM_DB')}",
        connect_args={'options': '-c statement_timeout=300000'}
    )

def get_postgres_engine_dest():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def fetch_dados_estoque():
    print(f"   [DB] Buscando posição de estoque atual (origem)...", flush=True)
    engine = get_sqlalchemy_engine()
    query = open("sql/query_estoque.sql", encoding="utf-8").read()
    df = pd.read_sql_query(text(query), con=engine)
    
    # Garantir que colunas numéricas sejam tratadas como tal pelo Pandas
    colunas_numericas = ['quantidade_disponivel', 'valor_unitario', 'valor_total']
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    print(f"   [DB] Download concluído! {len(df)} materiais em estoque.", flush=True)
    return df

def insert_estoque(df, engine):
    if df.empty:
        print("Nenhum dado de estoque encontrado.")
        return
    
    with engine.connect() as conn:
        # Em vez de 'replace', deletamos o conteúdo para manter a estrutura (colunas extras como created_at)
        conn.execute(text("DELETE FROM estoque"))
        conn.commit()
        
    df.to_sql("estoque", con=engine, index=False, if_exists="append", method="multi")
    print(f"✅ {len(df)} registros de estoque atualizados no destino.")

def run_etl_estoque():
    hoje = date.today().strftime('%Y-%m-%d')
    print(f"🔹 Iniciando ETL de Estoque (Data Ref: {hoje})...")
    engine_dest = get_postgres_engine_dest()
    df = fetch_dados_estoque()
    insert_estoque(df, engine_dest)
    print(f"✅ ETL de estoque finalizado.")
