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

def fetch_dados_faturamento(data_inicio, data_ref):
    print(f"   [DB] Conectando na origem e baixando dados...", flush=True)
    engine = get_sqlalchemy_engine()
    
    # 1. Carregar Notas Fiscais
    print("      -> Buscando Notas Fiscais...", flush=True)
    query_nf = open("sql/query_faturamento_nf.sql", encoding="utf-8").read()
    df_nf = pd.read_sql_query(text(query_nf), con=engine, params={"data_inicio": data_inicio, "data_ref": data_ref})
    
    # 2. Carregar Vendas Diretas
    print("      -> Buscando Vendas Diretas...", flush=True)
    query_vd = open("sql/query_faturamento_venda_direta.sql", encoding="utf-8").read()
    df_vd = pd.read_sql_query(text(query_vd), con=engine, params={"data_inicio": data_inicio, "data_ref": data_ref})
    
    # Unir
    df = pd.concat([df_nf, df_vd], ignore_index=True)
    
    # Normalizar nomes de colunas para minúsculas logo aqui
    df.columns = df.columns.str.lower()

    # Cálculo de frete_temperare (rateado por quantidade)
    if 'frete_raw' in df.columns and 'quantidade' in df.columns:
        df['frete_temperare'] = df.apply(
            lambda x: (x['frete_raw'] / x['quantidade']) if x['quantidade'] > 0 else 0, 
            axis=1
        )
    
    # Garantir que colunas numéricas sejam tratadas como tal pelo Pandas
    colunas_numericas = ['cmv', 'valor', 'valor_unitario', 'frete', 'frete_temperare', 'custo', 'outras_despesas', 'difal', 'comissao_do_canal']
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    print(f"   [DB] Download concluído! {len(df)} linhas totais obtidas.", flush=True)
    return df

def upsert_faturamento(df, engine):
    if df.empty:
        print("Nenhum dado novo ou alterado para faturamento.")
        return
    
    # Normalizar nomes de colunas para minúsculas para bater com o destino
    df.columns = df.columns.str.lower()
    
    # Lista de colunas permitidas no destino (obtida via inspeção)
    cols_permitidas = [
        'dtaltera', 'data_pedido', 'valor', 'valor_unitario', 'outras_despesas', 'cmv', 
        'custo', 'pedidos', 'qtde_vendida', 'frete', 'frete_temperare', 'difal', 
        'comissao_do_canal', 'data_faturamento', 'nota', 'transportador', 'vendedor', 
        'via_trafego', 'uf', 'cidade', 'bairro', 'cliente', 'parceiro', 'grupo_material', 
        'identificacao', 'nome', 'fabricante', 'forma_pagamento', 'prazo_pagamento', 
        'empresa', 'contribuinte_icms', 'local_destino', 'cliente_prop', 'id_pedido'
    ]
    
    # Mapeamento de tipos para garantir compatibilidade no destino
    dtype_map = {
        'valor': Numeric,
        'valor_unitario': Numeric,
        'outras_despesas': Numeric,
        'cmv': Numeric,
        'custo': Numeric,
        'frete': Numeric,
        'frete_temperare': Numeric,
        'difal': Numeric,
        'comissao_do_canal': Numeric,
        'pedidos': Integer,
        'qtde_vendida': Integer
    }
    
    with engine.begin() as conn:
        print(f"   [DB] Removendo registros antigos para UPSERT...", flush=True)
        keys_df = df[['id_pedido', 'identificacao']].copy()
        keys_df.to_sql("temp_keys_faturamento", con=conn, index=False, if_exists="replace")
        
        conn.execute(text("""
            DELETE FROM faturamento
            WHERE (id_pedido, identificacao) IN (
                SELECT id_pedido, identificacao FROM temp_keys_faturamento
            )
        """))
        
        # 2. Inserir os novos dados diretamente
        print(f"   [DB] Inserindo novos registros...", flush=True)
        # Filtramos apenas as colunas que existem no banco de destino
        df_to_insert = df[[c for c in cols_permitidas if c in df.columns]]
        
        df_to_insert.to_sql("faturamento", con=conn, index=False, if_exists="append", dtype=dtype_map)
        
        # Limpar tabela temporária
        conn.execute(text("DROP TABLE temp_keys_faturamento"))
        
    print(f"✅ {len(df)} registros de faturamento upserted com sucesso.")
    
def run_etl_faturamento():
    print("🔹 Iniciando ETL de faturamento (v1.3 - Somente Hoje)...")
    engine_dest = get_postgres_engine_dest()

    # Usa a data de hoje e janela incremental conforme o planejado
    hoje = date.today()
    data_inicio = (hoje - timedelta(days=60)).strftime('%Y-%m-%d')
    data_ref = (hoje - timedelta(days=2)).strftime('%Y-%m-%d') # Atualiza os últimos 2 dias ou desde que parou (ex: dia 11)

    df = fetch_dados_faturamento(data_inicio, data_ref)
    upsert_faturamento(df, engine_dest)
    print(f"✅ ETL de faturamento do dia finalizado.")
