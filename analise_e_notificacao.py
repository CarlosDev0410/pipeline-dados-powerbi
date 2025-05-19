import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_postgres_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASS')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def coletar_dados_resumo():
    engine = get_postgres_engine()
    data_hoje = date.today().strftime("%m/%d/%Y")

    faturamento_query = text("""
        SELECT COUNT(DISTINCT nota) as pedidos, SUM(valor) as total
        FROM faturamento
        WHERE TO_CHAR(data_faturamento, 'MM/DD/YYYY') = :data
    """)
    devolucao_query = text("""
        SELECT COUNT(DISTINCT nota) as pedidos, SUM(valor) as total
        FROM devolucao
        WHERE TO_CHAR(dtemissao, 'MM/DD/YYYY') = :data
    """)

    faturamento = pd.read_sql_query(faturamento_query, engine, params={"data": data_hoje})
    devolucao = pd.read_sql_query(devolucao_query, engine, params={"data": data_hoje})

    return faturamento.iloc[0], devolucao.iloc[0]

def enviar_email(resumo):
    from email.utils import formataddr

    remetente = os.getenv("EMAIL_FROM")
    smtp_user = os.getenv("EMAIL_SMTP_USER")  # novo campo
    senha = os.getenv("EMAIL_PASS")
    destinatarios = os.getenv("EMAIL_TO").split(",")  # transforma em lista
    smtp_host = os.getenv("EMAIL_SMTP", "smtp-relay.brevo.com")
    smtp_port = int(os.getenv("EMAIL_PORT", 587))

    msg = MIMEMultipart()
    msg['From'] = formataddr(("Temperare Relatórios", remetente))
    msg['To'] = ", ".join(destinatarios)
    msg['Subject'] = f"Resumo Diário do ETL - {date.today().strftime('%d/%m/%Y')}"

    msg.attach(MIMEText(resumo, 'plain'))

    with smtplib.SMTP(smtp_host, smtp_port) as servidor:
        servidor.starttls()
        servidor.login(smtp_user, senha)
        servidor.sendmail(remetente, destinatarios, msg.as_string())

    print("📧 Email enviado com sucesso.")

def run_analise():
    faturamento, devolucao = coletar_dados_resumo()

    texto = f"""
✅ RESUMO DIÁRIO DOS PEDIDOS - {date.today().strftime('%d/%m/%Y')}

FATURAMENTO:
- Pedidos faturados: {faturamento['pedidos'] or 0}
- Valor total bruto: R$ {faturamento['total'] or 0:,.2f}

DEVOLUÇÕES:
- Pedidos devolvidos: {devolucao['pedidos'] or 0}
- Valor total devolvido: R$ {devolucao['total'] or 0:,.2f}

Pipeline executado com sucesso.
"""

    print(texto)
    enviar_email(texto)

