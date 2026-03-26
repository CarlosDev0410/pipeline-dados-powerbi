import time
import os
from etl_faturamento import run_etl_faturamento
from etl_devolucao import run_etl_devolucao
from etl_estoque import run_etl_estoque
from analise_e_notificacao import run_analise
from atualiza_pedido import run_atualiza_pedido


def main():
    print("🔄 Iniciando processo...")

    try:
        print("\n▶️ Etapa 1: Faturamento")
        run_etl_faturamento()
    except Exception as e:
        print(f"❌ Erro ao executar ETL de faturamento: {e}")
        return

    try:
        print("\n▶️ Etapa 2: Atualização de Faturamento")
        run_atualiza_pedido()
    except Exception as e:
        print(f"❌ Erro ao executar Atualização de Pedidos: {e}")
        return

    try:
        print("\n▶️ Etapa 3: Devoluções")
        run_etl_devolucao()
    except Exception as e:
        print(f"❌ Erro ao executar ETL de devoluções: {e}")
        return

    try:
        print("\n▶️ Etapa 4: Estoque")
        run_etl_estoque()
    except Exception as e:
        print(f"❌ Erro ao executar ETL de Estoque: {e}")
        return

    print("\n✅ ETL completo finalizado com sucesso.")

    run_analise()

if __name__ == "__main__":
    main()
