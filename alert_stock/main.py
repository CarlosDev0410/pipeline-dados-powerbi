import os
import psycopg2
import requests
from datetime import datetime
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env na raiz do projeto
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_query():
    query_path = os.path.join(os.path.dirname(__file__), 'sql', 'query.sql')
    with open(query_path, 'r', encoding='utf-8') as f:
        return f.read()

def send_email(nome, qtde):
    api_key = os.getenv("BREVO_API_KEY")
    sender_email = os.getenv("EMAIL_FROM")
    receiver_email = os.getenv("EMAIL_TO")
    
    url = "https://api.brevo.com/v3/smtp/email"
    
    # Data e hora atual
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    # Lógica de cor
    if qtde > 5:
        color = "blue"
    elif qtde < 5:
        color = "red"
    else:
        color = "black"

    # Montagem da mensagem (Texto e HTML)
    text_message = f"O produto {nome} possui {qtde} UND em estoque físico.\nData/Hora: {agora}"
    html_message = (
        f"<p>O produto <strong>{nome}</strong> possui "
        f"<span style='color: {color}; font-weight: bold;'>{qtde}</span> em estoque físico.</p>"
        f"<p><small>Relatório gerado em: {agora}</small></p>"
    )
    
    if qtde <= 3:
        alert_text = "\n\nMovimente o produto imediatamente."
        alert_html = "<p style='color: red; font-weight: bold;'>Movimente o produto imediatamente.</p>"
        text_message += alert_text
        html_message += alert_html
        subject = f"ALERTA DE ESTOQUE CRÍTICO: {nome}"
    else:
        subject = f"Alerta de Estoque: {nome}"

    # Adicionando Rodapé
    footer_text = "\n\n---\nEsta é uma mensagem automática. Por favor, não responda."
    footer_html = "<hr><p style='color: gray; font-size: 12px;'><i>Esta é uma mensagem automática. Por favor, não responda.</i></p>"
    
    text_message += footer_text
    html_message += footer_html

    payload = {
        "sender": {"email": sender_email, "name": "Alerta de Estoque"},
        "to": [{"email": receiver_email}],
        "subject": subject,
        "textContent": text_message,
        "htmlContent": html_message
    }
    
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key
    }

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code in [200, 201, 202]:
        print(f"E-mail enviado com sucesso para {nome} (Qtde: {qtde}, Cor: {color})")
    else:
        print(f"Erro ao enviar e-mail: {response.status_code} - {response.text}")

def main():
    # Configurações do banco de dados (origem)
    try:
        conn = psycopg2.connect(
            host=os.getenv("ORIGEM_HOST"),
            database=os.getenv("ORIGEM_DB"),
            user=os.getenv("ORIGEM_USER"),
            password=os.getenv("ORIGEM_PASS"),
            port=os.getenv("ORIGEM_PORT")
        )
        cur = conn.cursor()
        
        query = get_query()
        cur.execute(query)
        
        results = cur.fetchall()
        
        if not results:
            print("Nenhum dado encontrado para a query informada.")
            return

        for row in results:
            nome = row[0]
            qtde = row[1]
            
            # Chama a função de envio de e-mail
            send_email(nome, qtde)

        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro durante a execução do script: {e}")

if __name__ == "__main__":
    main()
