import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def formatar_reais(valor):
    valor = float(valor or 0)
    valor /= 100  # Corrige escala para exibir como no sistema
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_reais_direto(valor):
    valor = float(valor or 0)
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def get_postgres_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def coletar_dados_resumo():
    engine = get_postgres_engine()
    data_hoje = date.today().strftime("%m/%d/%Y")

    faturamento_query = text("""
        SELECT COUNT(DISTINCT nota) as pedidos, SUM(valor+frete+outras_despesas) as total
        FROM faturamento
        WHERE TO_CHAR(data_faturamento, 'MM/DD/YYYY') = :data
    """)
    devolucao_query = text("""
        SELECT COUNT(DISTINCT nota) as pedidos, SUM(valor+frete) as total
        FROM devolucao
        WHERE TO_CHAR(dtemissao, 'MM/DD/YYYY') = :data
    """)

    estoque_queries = {
        "CD_RJ": "CD RJ - JD OLIMPO",
        "CD_MG": "CD MG - ORMIFRIO",
        "CD_ES": "CD ES - MERCOCAMP",
        "CD_SP_FULL": "CD - MELI SP (FULFILLMENT)",
        "CD_RJ_PENDENCIA": "CD RJ - JO PENDENCIA",
        "CD_ES_PENDENCIA": "CD ES - MC PENDENCIA",
    }

    estoque_resumo = {}
    for chave, local in estoque_queries.items():
        query = text("""
            SELECT
                SUM(quantidade_disponivel) AS qtde_total,
                SUM(valor_total) AS valor_total
            FROM estoque
            WHERE local_armazenagem = :local
        """)
        df = pd.read_sql_query(query, engine, params={"local": local})
        estoque_resumo[chave] = df.iloc[0]

    faturamento = pd.read_sql_query(faturamento_query, engine, params={"data": data_hoje})
    devolucao = pd.read_sql_query(devolucao_query, engine, params={"data": data_hoje})

    return faturamento.iloc[0], devolucao.iloc[0], estoque_resumo

def enviar_email(resumo_html):
    from email.utils import formataddr

    remetente = os.getenv("EMAIL_FROM")
    smtp_user = os.getenv("EMAIL_SMTP_USER")
    senha = os.getenv("EMAIL_PASS")
    destinatarios = os.getenv("EMAIL_TO").split(",")
    smtp_host = os.getenv("EMAIL_SMTP", "smtp-relay.brevo.com")
    smtp_port = int(os.getenv("EMAIL_PORT", 587))

    msg = MIMEMultipart("alternative")
    msg['From'] = formataddr(("Temperare Relatórios", remetente))
    msg['To'] = ", ".join(destinatarios)
    msg['Subject'] = f"Resumo Diário do ETL - {date.today().strftime('%d/%m/%Y')}"

    msg.attach(MIMEText(resumo_html, 'html'))

    with smtplib.SMTP(smtp_host, smtp_port) as servidor:
        servidor.starttls()
        servidor.login(smtp_user, senha)
        servidor.sendmail(remetente, destinatarios, msg.as_string())

    print("📧 Email enviado com sucesso.")

def run_analise():
    faturamento, devolucao, estoque_resumo = coletar_dados_resumo()

    html = f"""
    <html>
    <body>
    <h2>✅ RESUMO DIÁRIO DOS PEDIDOS - {date.today().strftime('%d/%m/%Y')}</h2>
    <p><strong>FATURAMENTO:</strong><br>
    - Pedidos faturados: {int(faturamento['pedidos'] or 0)}<br>
    - Valor total bruto: {formatar_reais_direto(faturamento['total'])}</p>

    <p><strong>DEVOLUÇÕES:</strong><br>
    - Pedidos devolvidos: {int(devolucao['pedidos'] or 0)}<br>
    - Valor total devolvido: {formatar_reais_direto(devolucao['total'])}</p>

    <hr>

    <h2>✅ RESUMO DIÁRIO DO ESTOQUE - {date.today().strftime('%d/%m/%Y')}</h2>

    <p><strong>ESTOQUE REGULAR:</strong><br>
"""
    total_qtde_regular = 0
    total_valor_regular = 0
    for chave in ["CD_RJ", "CD_ES", "CD_MG"]:
        dados = estoque_resumo.get(chave)
        qtde = dados['qtde_total'] or 0
        valor = dados['valor_total'] or 0
        total_qtde_regular += qtde
        total_valor_regular += valor
        html += f"- {chave}: {qtde:.0f} unidades | {formatar_reais(valor)}<br>"

    html += "</p><p><strong>ESTOQUE FULFILLMENT:</strong><br>"
    dados = estoque_resumo.get("CD_SP_FULL")
    qtde_full = dados['qtde_total'] or 0
    valor_full = dados['valor_total'] or 0
    html += f"- FULL MELI RJ: {qtde_full:.0f} unidades | {formatar_reais(valor_full)}<br>"

    total_geral_qtde = total_qtde_regular + qtde_full
    total_geral_valor = total_valor_regular + valor_full
    html += f"<br><b style='color: #0009FF;'>Total Regular + Fulfillment: {total_geral_qtde:.0f} unidades | {formatar_reais(total_geral_valor)}</b></p>"

    html += "<p><strong>ESTOQUE DE AVARIADOS:</strong><br>"
    total_qtde_pend = 0
    total_valor_pend = 0
    for chave in ["CD_RJ_PENDENCIA", "CD_ES_PENDENCIA"]:
        dados = estoque_resumo.get(chave)
        qtde = dados['qtde_total'] or 0
        valor = dados['valor_total'] or 0
        total_qtde_pend += qtde
        total_valor_pend += valor
        html += f"- {chave}: {qtde:.0f} unidades | {formatar_reais(valor)}<br>"

    html += f"<br><b style='color: #0009FF;'>Total em Pendências: {total_qtde_pend:.0f} unidades | {formatar_reais(total_valor_pend)}</b></p>"

    html += "<p><em>Pipeline executado com sucesso. Version 1.09</em></p></body></html>"

    print(html)
    enviar_email(html)

if __name__ == "__main__":
    run_analise()
