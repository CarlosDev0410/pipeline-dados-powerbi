import pandas as pd
import os
from datetime import date, timedelta
from sqlalchemy import create_engine, text, Numeric, Integer
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

def fetch_dados_devolucao(data_hoje):
    print(f"   [DB] Buscando devoluções para a data {data_hoje}...", flush=True)
    engine = get_sqlalchemy_engine()
    
    # 1. Parte 1
    print("      -> Buscando Parte 1...", flush=True)
    query_p1 = open("sql/query_devolucao_p1.sql", encoding="utf-8").read()
    df1 = pd.read_sql_query(text(query_p1), con=engine, params={"dataref": data_hoje})
    
    # 2. Parte 2
    print("      -> Buscando Parte 2...", flush=True)
    query_p2 = open("sql/query_devolucao_p2.sql", encoding="utf-8").read()
    df2 = pd.read_sql_query(text(query_p2), con=engine, params={"dataref": data_hoje})
    
    # Unir
    df = pd.concat([df1, df2], ignore_index=True)
    
    # Normalizar nomes de colunas
    df.columns = df.columns.str.lower()
    
    # Garantir que colunas numéricas sejam tratadas como tal pelo Pandas (evita DatatypeMismatch)
    colunas_numericas = ['cmv', 'valor', 'valor_unitario', 'frete', 'frete_temperare', 'custo', 'outras_despesas']
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    print(f"   [DB] Download concluído! {len(df)} devoluções totais obtidas.", flush=True)
    return df

def upsert_devolucao(df, engine):
    if df.empty:
        print("Nenhum dado de devolução encontrado para upsert.")
        return
    
    # Normalizar nomes de colunas
    df.columns = df.columns.str.lower()
    
    # Lista de colunas permitidas no destino (conforme schema Supabase)
    cols_permitidas = [
        'dtemissao', 'dtpedidovenda', 'parceiro', 'grupo_material', 'identificacao', 
        'nome', 'valor', 'valor_unitario', 'outras_despesas', 'cmv', 'custo', 
        'pedidos', 'qtde_vendida', 'nota', 'transportador', 'frete', 'frete_temperare', 
        'uf', 'cidade', 'bairro', 'cliente', 'fabricante', 'forma_pagamento', 
        'prazo_pagamento', 'id_pedido', 'empresa'
    ]
    
    # Mapeamento de tipos
    dtype_map = {
        'valor': Numeric,
        'valor_unitario': Numeric,
        'frete': Numeric,
        'frete_temperare': Numeric,
        'cmv': Numeric,
        'custo': Numeric,
        'outras_despesas': Numeric,
        'pedidos': Integer,
        'qtde_vendida': Integer
    }
    
    with engine.begin() as conn:
        # 1. Deletar os registros que serão substituídos (chave: nota + identificacao)
        print(f"   [DB] Removendo registros antigos de devolução...", flush=True)
        # Garantir tipos corretos no DataFrame de chaves
        keys_df = df[['nota', 'identificacao']].copy()
        keys_df['nota'] = keys_df['nota'].astype(int)
        keys_df.to_sql("temp_keys_devolucao", con=conn, index=False, if_exists="replace")
        
        conn.execute(text("""
            DELETE FROM devolucao
            WHERE (nota, identificacao) IN (
                SELECT nota::integer, identificacao::varchar FROM temp_keys_devolucao
            )
        """))
        
        # 2. Inserir os registros novos/atualizados diretamente
        print(f"   [DB] Inserindo novas devoluções...", flush=True)
        df_to_insert = df[[c for c in cols_permitidas if c in df.columns]]
        df_to_insert.to_sql("devolucao", con=conn, index=False, if_exists="append", dtype=dtype_map)
        
        # Limpar
        conn.execute(text("DROP TABLE temp_keys_devolucao"))
        
    print(f"✅ {len(df)} registros de devolução upserted com sucesso.")

def run_etl_devolucao():
    print("🔹 Iniciando ETL de devoluções...")
    engine_dest = get_postgres_engine_dest()
    
    hoje = date.today().strftime('%Y-%m-%d')
    df = fetch_dados_devolucao(hoje)
    
    upsert_devolucao(df, engine_dest)
    print(f"✅ ETL de devoluções finalizado. Data: {hoje}")
