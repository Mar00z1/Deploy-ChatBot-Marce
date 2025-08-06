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

# Load environment and apply async patch
load_dotenv()
nest_asyncio.apply()

# Initialize Flask app
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Environment helper
def env(var):
    value = os.getenv(var)
    if not value:
        raise RuntimeError(f"Falta definir {var}")
    return value

# Twilio credentials
TWILIO_SID = env("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = env("TWILIO_AUTH_TOKEN")
TWILIO_FROM = env("TWILIO_WHATSAPP_NUMBER")

# Constants
MEMORY_LIMIT = 100
MAX_RETRIES = 5
BASE_DELAY = 1  # segundo
json_text = None
agent = None
user_histories = defaultdict(list)
message_queue = queue.Queue()

# Worker: sends one message every 2 seconds, handles 429 with exponential back-off
def message_worker():
    while True:
        item = message_queue.get()
        if len(item) == 2:
            to, body = item
            retries = 0
        else:
            to, body, retries = item
        try:
            resp = httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                data={"From": TWILIO_FROM, "To": to, "Body": body},
                auth=(TWILIO_SID, TWILIO_TOKEN),
                timeout=10
            )
            if resp.status_code == 429 and retries < MAX_RETRIES:
                retries += 1
                delay = BASE_DELAY * (2 ** (retries - 1))
                logging.warning(f"429 for {to}. Retry {retries}/{MAX_RETRIES} after {delay}s")
                threading.Timer(delay, lambda: message_queue.put((to, body, retries))).start()
            elif resp.status_code == 429:
                logging.error(f"Dropped message to {to} after {retries} retries due to rate limit.")
            elif resp.is_success:
                logging.info(f"Mensaje enviado a {to}: {body[:50]}")
            else:
                logging.error(f"Error {resp.status_code} enviando a {to}: {resp.text}")
        except Exception:
            logging.exception(f"Error enviando a {to}")
        finally:
            message_queue.task_done()
            time.sleep(2)

# Start the worker thread
threading.Thread(target=message_worker, daemon=True).start()

# Enqueue helper
def enqueue_message(to, body, retries=0):
    if retries:
        message_queue.put((to, body, retries))
    else:
        message_queue.put((to, body))

# Normalize number format
def normalize(number):
    return number if number.startswith("whatsapp:") else f"whatsapp:{number}"

# Load and cache Excel data, serializing timestamps
def cargar():
    global json_text
    if json_text is None:
        logging.info("Descargando y procesando archivo Excel...")
        url = "https://drive.google.com/uc?id=1zSbeJRK2tBTQOQmbAkipfBccLbb4LL_1"
        output = "data.xlsx"
        gdown.download(url, output, quiet=True)
        df_dict = pd.read_excel(output, sheet_name=None)
        serializable = {}
        for sheet, df in df_dict.items():
            for col in df.select_dtypes(include=["datetime64[ns]"]):
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
            serializable[sheet] = df.to_dict(orient="records")
        json_text = json.dumps(serializable, ensure_ascii=False, default=str)
        logging.info("Excel procesado correctamente.")
    return json_text

# Initialize or retrieve agent
def get_agent():
    global agent
    if agent is None:
        agent = Agent(
            name="Excel_Read",
            instructions=f"Usa este JSON: {cargar()}",
            model="gpt-4.1"
        )
        logging.info("Agente inicializado con GPT-4.1.")
    return agent

# Record user history with memory limit
def record_history(uid, msg):
    h = user_histories[uid]
    h.append({"role": "user", "content": msg})
    user_histories[uid] = h[-MEMORY_LIMIT:]

# Background message processing
def process_input(msg, uid):
    record_history(uid, msg)
    res = Runner.run_sync(get_agent(), msg).final_output
    text = res or "Error generando respuesta"
    for i in range(0, len(text), 1500):
        enqueue_message(uid, text[i:i + 1500])

# Webhook endpoint for incoming messages
@app.route('/webhook', methods=['POST'])
def webhook():
    msg = request.values.get('Body', '').strip()
    uid = normalize(request.values.get('From', ''))
    if not msg:
        return Response(status=400)
    logging.info(f"Recibido de {uid}: {msg}")
    # Eliminado mensaje de 'Procesando...'
    threading.Thread(target=process_input, args=(msg, uid), daemon=True).start()
    return Response(status=200)

# Refresh endpoint to reload data and agent
@app.route('/refresh', methods=['POST'])
def refresh():
    token = request.headers.get('Authorization')
    if os.getenv('REFRESH_TOKEN') and token != os.getenv('REFRESH_TOKEN'):
        return "No autorizado", 403
    global json_text, agent
    json_text = None
    agent = None
    return "Refrescado", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
























