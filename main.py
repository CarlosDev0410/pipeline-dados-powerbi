import time
from etl_faturamento import run_etl_faturamento
from etl_devolucao import run_etl_devolucao
from etl_estoque import run_etl_estoque
from analise_e_notificacao import run_analise


def main():
    print("🔄 Iniciando processo...")

    try:
        print("\n▶️ Etapa 1: Faturamento")
        run_etl_faturamento()
    except Exception as e:
        print(f"❌ Erro ao executar ETL de faturamento: {e}")
        return

    try:
        print("\n▶️ Etapa 2: Devoluções")
        run_etl_devolucao()
    except Exception as e:
        print(f"❌ Erro ao executar ETL de devoluções: {e}")
        return

    try:
        print("\n▶️ Etapa 3: Estoque")
        run_etl_estoque()
    except Exception as e:
        print(f"❌ Erro ao executar ETL de Estoque: {e}")
        return

    print("\n✅ ETL completo finalizado com sucesso.")

    run_analise()

if __name__ == "__main__":
    main()
