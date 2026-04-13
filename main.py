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

        print("\n▶️ Etapa 2: Devolução")
        run_etl_devolucao()

        print("\n▶️ Etapa 3: Estoque")
        run_etl_estoque()

        print("\n▶️ Etapa 4: Análise")
        run_analise()

    except Exception as e:
        print(f"\n❌ Erro crítico no pipeline: {e}")


if __name__ == "__main__":
    main()
