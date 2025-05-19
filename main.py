import time
from etl_faturamento import run_etl_faturamento
from etl_devolucao import run_etl_devolucao
from analise_e_notificacao import run_analise

def main():


    run_analise()

if __name__ == "__main__":
    main()
