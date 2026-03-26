import pandas as pd
from datetime import date, timedelta
from sqlalchemy import text
from etl_faturamento import get_postgres_engine_dest, get_sqlalchemy_engine

def run_atualiza_pedido():
    print("🔹 Iniciando Atualização de Fretes (v1.4 - Lean Sync)...")
    engine_dest = get_postgres_engine_dest()
    engine_origem = get_sqlalchemy_engine() # Importado do etl_faturamento

    # Filtramos pedidos modificados nas últimas 48h
    data_ref = date.today() - timedelta(days=2)

    query = open("sql/query_update_frete.sql", encoding="utf-8").read()
    df = pd.read_sql_query(text(query), con=engine_origem, params={"data_ref": data_ref})
    
    if df.empty:
        print("Nenhuma alteração de frete encontrada recentemente.")
        return
    
    # 1. Carrega apenas as colunas necessárias para staging
    df.to_sql("faturamento_staging", con=engine_dest, index=False, if_exists="replace")
    
    with engine_dest.connect() as conn:
        # 2. Update direcionado apenas nos campos que mudam (frete e dtaltera)
        # Mantemos a lógica de dividir o frete total pela quantidade de itens no pedido
        conn.execute(text("""
            UPDATE faturamento f
            SET 
                frete_temperare = s.frete_total / (
                    SELECT NULLIF(count(*), 0) 
                    FROM faturamento f2 
                    WHERE f2.id_pedido = f.id_pedido
                ),
                dtaltera = s.dtaltera
            FROM faturamento_staging s
            WHERE f.id_pedido = s.id_pedido
        """))
        conn.commit()
        
    print(f"✅ Frete de {len(df)} pedidos atualizados com sucesso.")

if __name__ == "__main__":
    run_atualiza_pedido()
