import pandas as pd
import os
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega as configurações do arquivo .env.local
load_dotenv(".env.local")

# Configura a data limite para 2 dias atrás
data_limite = date.today() - timedelta(days=2)

def get_sqlalchemy_engine():
    """Conexão com o banco de dados de Origem."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('ORIGEM_USER')}:{os.getenv('ORIGEM_PASS')}@{os.getenv('ORIGEM_HOST')}:{os.getenv('ORIGEM_PORT')}/{os.getenv('ORIGEM_DB')}"
    )

def get_postgres_engine_dest():
    """Conexão com o banco de dados de Destino (Supabase)."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def fetch_dados_pedidos():
    """Extrai os dados de pedidos da origem usando a query SQL."""
    engine = get_sqlalchemy_engine()
    query_path = "sql/query_order.sql"
    
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"Arquivo de query não encontrado: {query_path}")
        
    query = open(query_path, encoding="utf-8").read()
    
    # Executa a query filtrando pela data limite (2 dias atrás)
    df = pd.read_sql_query(text(query), con=engine, params={"data_inicio": data_limite})
    return df

def fetch_dest_data(engine):
    """Busca o que já existe no destino (Supabase) para o intervalo dos últimos 2 dias."""
    query = text("SELECT * FROM pedidos WHERE data >= :limite")
    df = pd.read_sql_query(query, con=engine, params={"limite": data_limite})
    # Remove colunas de metadados do Supabase se houver (ex: created_at)
    if 'created_at' in df.columns:
        df = df.drop(columns=['created_at'])
    return df

def upsert_pedidos(df, engine):
    """Realiza o Update + Insert (Upsert) dos dados no banco de destino."""
    if df.empty:
        return

    # Ajusta os nomes das colunas para minúsculo
    df.columns = [col.lower() for col in df.columns]
    
    # Colunas que formam a chave única
    conflict_cols = ['codigo', 'sku']
    
    # Prepara o comando SQL de Upsert
    cols = ', '.join(df.columns)
    vals = ', '.join([':' + col for col in df.columns])
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in conflict_cols])
    
    query_upsert = text(f"""
        INSERT INTO pedidos ({cols})
        VALUES ({vals})
        ON CONFLICT ({', '.join(conflict_cols)}) 
        DO UPDATE SET {updates}
    """)

    with engine.begin() as conn:
        for idx, row in df.iterrows():
            conn.execute(query_upsert, row.to_dict())

def run_etl_pedidos():
    """Executa o fluxo de ETL de Pedidos com comparação Delta."""
    print(f"🔹 Iniciando ETL Delta de pedidos (Janela: {data_limite} até hoje)...")
    try:
        engine_dest = get_postgres_engine_dest()
        
        # 1. Busca dados na origem
        df_origem = fetch_dados_pedidos()
        df_origem.columns = [col.lower() for col in df_origem.columns]
        
        if df_origem.empty:
            print("Nenhum dado encontrado na origem.")
            return

        # 2. Busca o que já existe no destino
        df_destino = fetch_dest_data(engine_dest)
        
        # 3. Identifica as diferenças (Delta)
        if df_destino.empty:
            df_delta = df_origem
        else:
            # Garante tipos consistentes para comparação
            # Convertendo datas e floats para strings normalizadas para evitar falsos positivos
            for df in [df_origem, df_destino]:
                df['data'] = pd.to_datetime(df['data']).dt.date
                # Preenche NaNs com strings vazias para comparação
                df.fillna('', inplace=True)

            # Faz o merge para identificar mudanças
            # Left join da origem no destino para ver o que mudou ou é novo
            df_delta = pd.merge(df_origem, df_destino, on=['codigo', 'sku'], how='left', suffixes=('', '_dest'))

            # Filtro: Pedidos que não existem no destino OU que tiveram alguma coluna alterada
            # Comparamos as colunas da origem com as colunas vindo do destino (_dest)
            mask_novos = df_delta['data_dest'].isna()
            
            # Compara campos relevantes para ver se houve alteração
            # Ex: situacao, valor_final, etc.
            mudou = False
            for col in df_origem.columns:
                if col not in ['codigo', 'sku', 'data']: # data e chaves ignoramos na alteração base
                    dest_col = f"{col}_dest"
                    if dest_col in df_delta.columns:
                        mudou |= (df_delta[col].astype(str) != df_delta[dest_col].astype(str))

            df_delta = df_delta[mask_novos | mudou]
            
            # Limpa as colunas auxiliares do merge
            df_delta = df_delta[df_origem.columns]

        # 4. Executa Upsert apenas no Delta
        if not df_delta.empty:
            print(f"🔄 Detectadas {len(df_delta)} alterações ou novos registros.")
            upsert_pedidos(df_delta, engine_dest)
            print(f"✅ Delta processado com sucesso.")
        else:
            print("✨ Nenhuma alteração detectada. Nenhum dado enviado ao Banco!")
            
        print(f"🎉 Ciclo finalizado. {date.today()}")
        
    except Exception as e:
        print(f"❌ Erro no ETL de pedidos: {e}")

if __name__ == "__main__":
    run_etl_pedidos()
