import os
import psycopg2
import requests
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv


# Tenta carregar o .env da raiz ou do diretório atual
load_dotenv() # Procura no diretório atual
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env')) # Procura um nível acima

def get_query():
    query_path = os.path.join(os.path.dirname(__file__), 'sql', 'query.sql')
    if not os.path.exists(query_path):
        # Fallback para quando o script roda dentro da pasta alert_stock no Railway
        query_path = os.path.join(os.path.dirname(__file__), 'query.sql')
        
    with open(query_path, 'r', encoding='utf-8') as f:
        return f.read()

def send_email(results):
    api_key = os.getenv("BREVO_API_KEY", "").strip()
    sender_email = os.getenv("EMAIL_FROM", "").strip()
    receiver_email = os.getenv("EMAIL_TO", "").strip()
    
    receiver_email = receiver_email.replace('"', '').replace("'", "")
    sender_email = sender_email.replace('"', '').replace("'", "")
    
    if not receiver_email or not api_key:
        print("Erro: Credenciais ou destinatário não configurados.")
        return False
    
    email_list = [e.strip() for e in receiver_email.split(",") if e.strip()]
    to_field = [{"email": email} for email in email_list]
    
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    # Agrupamento por Local
    estoque_por_local = defaultdict(list)
    has_critical_stock = False
    
    for row in results:
        # Estrutura esperada: 0: Nome, 1: Qtde, 2: Local
        nome = row[0]
        qtde = float(row[1]) if row[1] is not None else 0
        local = row[2] if len(row) > 2 else "Local Não Informado"
        
        estoque_por_local[local].append({"nome": nome, "qtde": qtde})
        if qtde <= 3:
            has_critical_stock = True

    # Cores por Local
    cores_locais = {
        "CD RJ - JD OLIMPO": "#1e88e5", # Azul
        "CD ES - MERCOCAMP": "#43a047"  # Verde
    }
    cor_padrao = "#555555"

    # Construção do HTML
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2 style="color: #444; border-bottom: 2px solid #eee; padding-bottom: 10px;">Relatório de Alerta de Estoque</h2>
        <p>Olá, seguem as informações atualizadas do estoque físico:</p>
    """

    for local, itens in estoque_por_local.items():
        cor_header = cores_locais.get(local, cor_padrao)
        html_content += f"""
        <div style="margin-bottom: 30px; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
            <div style="background-color: {cor_header}; color: white; padding: 12px 20px; font-weight: bold; font-size: 18px;">
                📍 {local}
            </div>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background-color: #fafafa; text-align: left; font-size: 12px; color: #888;">
                        <th style="padding: 10px 20px; border-bottom: 1px solid #eee;">Produto</th>
                        <th style="padding: 10px; border-bottom: 1px solid #eee; text-align: center;">Quantidade</th>
                        <th style="padding: 10px; border-bottom: 1px solid #eee; text-align: center;">Status</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for item in itens:
            cor_qtde = "#d32f2f" if item['qtde'] <= 3 else ("#f57c00" if item['qtde'] <= 5 else "#333")
            status = "CRÍTICO" if item['qtde'] <= 3 else "OK"
            html_content += f"""
                <tr>
                    <td style="padding: 12px 20px; border-bottom: 1px solid #f5f5f5; font-size: 14px;">{item['nome']}</td>
                    <td style="padding: 12px 20px; border-bottom: 1px solid #f5f5f5; text-align: center; color: {cor_qtde}; font-weight: bold;">{int(item['qtde'])} UND</td>
                    <td style="padding: 12px 20px; border-bottom: 1px solid #f5f5f5; text-align: center;">
                        <span style="background-color: {'#ffebee' if item['qtde'] <= 3 else '#e8f5e9'}; 
                                     color: {'#c62828' if item['qtde'] <= 3 else '#2e7d32'}; 
                                     padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: bold;">
                            {status}
                        </span>
                    </td>
                </tr>
            """
            
        html_content += "</tbody></table></div>"

    html_content += f"""
        <p style="font-size: 12px; color: #999; margin-top: 30px; text-align: center;">
            Relatório gerado automaticamente em: <strong>{agora}</strong><br>
            <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
            <i>Este é um sistema de monitoramento automático. Não responda a este e-mail.</i>
        </p>
    </body>
    </html>
    """



    subject = "⚠️ ALERTA: Estoque Crítico Identificado" if has_critical_stock else "Relatório Semanal de Estoque"
    
    payload = {
        "sender": {"email": sender_email, "name": "Sistema de Alerta de Estoque"},
        "to": to_field,
        "subject": subject,
        "htmlContent": html_content
    }
    
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key
    }

    url = "https://api.brevo.com/v3/smtp/email"
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code in [200, 201, 202]:
        print(f"E-mail consolidado enviado com sucesso. (Crítico: {has_critical_stock})")
        return True
    else:
        print(f"Erro ao enviar e-mail: {response.status_code} - {response.text}")
        return False

def main():
    try:
        def get_env(var):
            val = os.getenv(var, "")
            return val.strip().replace('"', '').replace("'", "") if val else val

        conn = psycopg2.connect(
            host=get_env("ORIGEM_HOST"),
            database=get_env("ORIGEM_DB"),
            user=get_env("ORIGEM_USER"),
            password=get_env("ORIGEM_PASS"),
            port=get_env("ORIGEM_PORT")
        )
        cur = conn.cursor()
        
        query = get_query()
        cur.execute(query)
        results = cur.fetchall()
        
        if not results:
            print("Nenhum dado encontrado para a query informada.")
            return

        # Envia um único e-mail com todos os resultados
        send_email(results)

        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro durante a execução do script: {e}")


if __name__ == "__main__":
    main()
