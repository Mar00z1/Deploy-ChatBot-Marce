import os
import json
import gdown
import logging
import nest_asyncio
import pandas as pd
import threading

from flask import Flask, request, Response
from dotenv import load_dotenv
from agents import Agent, Runner
from collections import defaultdict
from twilio.rest import Client

# ------------------- Setup inicial -------------------
load_dotenv()
nest_asyncio.apply()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Claves de entorno necesarias
env = lambda var: os.getenv(var) or (_ for _ in ()).throw(RuntimeError(f"Falta definir {var}"))
openai_key = env("OPENAI_API_KEY")
refresh_token = os.getenv("REFRESH_TOKEN")
twilio_sid = env("TWILIO_ACCOUNT_SID")
twilio_token = env("TWILIO_AUTH_TOKEN")
twilio_from = env("TWILIO_WHATSAPP_NUMBER")  # formato "whatsapp:+1234..."

# Cliente Twilio
twilio_client = Client(twilio_sid, twilio_token)

# ------------------- Variables globales -------------------
MEMORY_LIMIT = 100
json_text = None
agent = None
user_histories = defaultdict(list)

# ------------------- Cargar datos una sola vez -------------------
def cargar_dataframe():
    global json_text
    if json_text is None:
        logging.info("Descargando y procesando archivo Excel...")
        url = "https://drive.google.com/uc?id=1zSbeJRK2tBTQOQmbAkipfBccLbb4LL_1"
        output = "MarcePrueba.xlsx"
        gdown.download(url, output, quiet=True)
        df_dict = pd.read_excel(output, sheet_name=None)
        serializable = {key: df.to_dict(orient="records") for key, df in df_dict.items()}
        json_text = json.dumps(serializable, ensure_ascii=False, indent=2, default=str)
        logging.info("Excel procesado correctamente.")
    return json_text

# ------------------- Inicializar agente una vez -------------------
def inicializar_agente():
    global agent
    if agent is None:
        datos = cargar_dataframe()
        agent = Agent(
            name="Excel_Read",
            instructions=f"Sos un asistente de Llamas ventas. Usa este dataframe en texto plano para responder preguntas: {datos}",
            model="gpt-4.1-mini"
        )
        logging.info("Agente inicializado.")
    return agent

# ------------------- Historial por usuario -------------------
def actualizar_historial(user_id, user_input):
    history = user_histories[user_id]
    history.append({'role': 'user', 'content': user_input})
    if len(history) > MEMORY_LIMIT * 2:
        history = history[-MEMORY_LIMIT * 2:]
    user_histories[user_id] = history
    return history

# ------------------- Procesamiento en segundo plano -------------------
def process_and_send(user_input, user_id):
    try:
        agent = inicializar_agente()
        actualizar_historial(user_id, user_input)
        result = Runner.run_sync(agent, user_input)
        respuesta = result.final_output
        twilio_client.messages.create(
            body=respuesta,
            from_=twilio_from,
            to=user_id
        )
        logging.info(f"[Background][{user_id}] Respuesta enviada: {respuesta}")
    except Exception:
        logging.exception(f"[Background][{user_id}] Error procesando mensaje")

# ------------------- Webhook principal (responde inmediato) -------------------
@app.route('/webhook', methods=['POST'])
def webhook():
    user_input = request.values.get('Body', '').strip()
    user_id = request.values.get('From', '')
    if not user_input:
        return Response(status=400)

    logging.info(f"[{user_id}] Mensaje recibido: {user_input}")

    # Enviar mensaje de espera inmediato
    twilio_client.messages.create(
        body="Un momento, estoy procesando tu solicitud...",
        from_=twilio_from,
        to=user_id
    )

    # Ejecutar procesamiento en segundo plano
    threading.Thread(target=process_and_send, args=(user_input, user_id), daemon=True).start()

    # Retornar 200 OK, sin cuerpo (evita timeout)
    return Response(status=200)

# ------------------- Endpoint para refrescar el Excel -------------------
@app.route('/refresh', methods=['POST'])
def refresh_excel():
    token = request.headers.get("Authorization")
    if refresh_token and token != refresh_token:
        return "No autorizado", 403

    global json_text, agent
    json_text = None
    agent = None
    logging.info("Se forzó la recarga del Excel y el agente.")
    return "Excel recargado correctamente", 200

# ------------------- Servidor local (opcional) -------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))




