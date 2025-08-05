import os
import json
import gdown
import logging
import nest_asyncio
import pandas as pd
import threading
import time
import httpx
import queue

from flask import Flask, request, Response
from dotenv import load_dotenv
from agents import Agent, Runner
from collections import defaultdict
from httpx import HTTPStatusError, RequestError

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
message_queue = queue.Queue()

# ------------------- Worker para manejar mensajes con rate limit -------------------
def message_worker():
    while True:
        to, body = message_queue.get()
        try:
            send_whatsapp_message(to, body)
        except Exception:
            logging.exception(f"[Worker] Error al enviar mensaje a {to}")
        time.sleep(1)  # Límite de 1 msg/seg
        message_queue.task_done()

threading.Thread(target=message_worker, daemon=True).start()

# ------------------- Función para enviar mensajes (usa cola) -------------------
def enqueue_whatsapp_message(to: str, body: str):
    message_queue.put((to, body))

# ------------------- Envío real con retry (llamado por el worker) -------------------
def send_whatsapp_message(to: str, body: str, max_retries: int = 5, backoff_base: float = 1.0):
    url = f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Messages.json"
    data = {"From": twilio_from, "To": to, "Body": body}
    auth = (twilio_sid, twilio_token)
    
    for attempt in range(max_retries):
        try:
            response = httpx.post(url, data=data, auth=auth, timeout=10)
            response.raise_for_status()
            logging.info(f"[Twilio] Mensaje enviado a {to}: {body[:50]}{'...' if len(body)>50 else ''}")
            return
        except HTTPStatusError as e:
            status = e.response.status_code
            if status == 429:
                wait = backoff_base * (2 ** attempt)
                logging.warning(f"[Twilio] 429 Too Many Requests. Retry en {wait:.1f}s (intento {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                logging.exception(f"[Twilio] Error {status} al enviar mensaje a {to}")
                break
        except RequestError as e:
            logging.warning(f"[Twilio] Error de red, intento {attempt+1}/{max_retries}: {e}")
            time.sleep(backoff_base * (2 ** attempt))
    
    logging.error(f"[Twilio] No se pudo enviar mensaje a {to} tras {max_retries} intentos")

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
            model="gpt-4.1"
        )
        logging.info("Agente inicializado con GPT-4.1.")
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
            enqueue_whatsapp_message(user_id, chunk)
    except Exception:
        logging.exception(f"[Background][{user_id}] Error procesando mensaje")
        enqueue_whatsapp_message(user_id, "Ocurrió un error procesando tu solicitud. Intenta nuevamente más tarde.")
    finally:
        done_event.set()

# ------------------- Enviar mensaje de espera tras demora -------------------
def delayed_send(user_id_raw, done_event: threading.Event, delay: int = 5):
    user_id = normalize_whatsapp(user_id_raw)
    time.sleep(delay)
    if not done_event.is_set():
        try:
            enqueue_whatsapp_message(user_id, "Un momento, estoy procesando tu solicitud...")
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

    threading.Thread(target=delayed_send, args=(user_id_raw, done_event), daemon=True).start()
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











