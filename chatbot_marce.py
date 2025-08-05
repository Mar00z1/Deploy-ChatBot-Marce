import os
import json
import gdown
import logging
import nest_asyncio
import pandas as pd
import threading
import time
import httpx

from flask import Flask, request, Response
from dotenv import load_dotenv
from agents import Agent, Runner
from collections import defaultdict

# ------------------- Setup inicial -------------------
load_dotenv()
nest_asyncio.apply()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Helper para obtener variables de entorno o lanzar error
env = lambda var: os.getenv(var) or (_ for _ in ()).throw(RuntimeError(f"Falta definir {var}"))
openai_key = env("OPENAI_API_KEY")
refresh_token = os.getenv("REFRESH_TOKEN")
twilio_sid = env("TWILIO_ACCOUNT_SID")
twilio_token = env("TWILIO_AUTH_TOKEN")
twilio_from = env("TWILIO_WHATSAPP_NUMBER")  # ej. "whatsapp:+1415XXXXXXX"

# ------------------- Variables globales -------------------
MEMORY_LIMIT = 100
json_text = None
agent = None
user_histories = defaultdict(list)

# ------------------- Función para enviar mensajes WhatsApp vía HTTP -------------------
def send_whatsapp_message(to: str, body: str):
    url = f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Messages.json"
    data = {"From": twilio_from, "To": to, "Body": body}
    auth = (twilio_sid, twilio_token)
    response = httpx.post(url, data=data, auth=auth)
    response.raise_for_status()
    logging.info(f"[Twilio] Mensaje enviado a {to}: {body[:50]}{'...' if len(body)>50 else ''}")

# ------------------- Normalizar prefijo WhatsApp -------------------
def normalize_whatsapp(number: str) -> str:
    n = number.strip()
    return n if n.startswith("whatsapp:") else f"whatsapp:{n}"

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
def process_and_send(user_input, user_id_raw, done_event: threading.Event):
    user_id = normalize_whatsapp(user_id_raw)
    try:
        agent = inicializar_agente()
        actualizar_historial(user_id, user_input)
        result = Runner.run_sync(agent, user_input)
        respuesta = result.final_output or "Lo siento, no pude generar una respuesta."
        max_len = 1500
        for i in range(0, len(respuesta), max_len):
            chunk = respuesta[i:i+max_len]
            send_whatsapp_message(user_id, chunk)
    except Exception:
        logging.exception(f"[Background][{user_id}] Error procesando mensaje")
        send_whatsapp_message(user_id, "Ocurrió un error procesando tu solicitud. Intenta nuevamente más tarde.")
    finally:
        done_event.set()

# ------------------- Enviar mensaje de espera tras demora -------------------
def delayed_send(user_id_raw, done_event: threading.Event, delay: int = 5):
    user_id = normalize_whatsapp(user_id_raw)
    time.sleep(delay)
    if not done_event.is_set():
        try:
            send_whatsapp_message(user_id, "Un momento, estoy procesando tu solicitud...")
        except Exception:
            logging.exception(f"[Delayed][{user_id}] Error enviando mensaje de espera")

# ------------------- Webhook principal (dispara threads) -------------------
@app.route('/webhook', methods=['POST'])
def webhook():
    user_input = request.values.get('Body', '').strip()
    user_id_raw = request.values.get('From', '')
    if not user_input:
        return Response(status=400)

    logging.info(f"[Webhook] Mensaje recibido de {user_id_raw}: {user_input}")
    done_event = threading.Event()

    # Thread para mensaje de espera tras demora
    threading.Thread(target=delayed_send, args=(user_id_raw, done_event), daemon=True).start()

    # Thread para procesamiento y respuesta final
    threading.Thread(target=process_and_send, args=(user_input, user_id_raw, done_event), daemon=True).start()

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






