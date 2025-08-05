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

# Setup
load_dotenv()
nest_asyncio.apply()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

env = lambda var: os.getenv(var) or (_ for _ in ()).throw(RuntimeError(f"Falta definir {var}"))
twilio_sid = env("TWILIO_ACCOUNT_SID")
twilio_token = env("TWILIO_AUTH_TOKEN")
twilio_from = env("TWILIO_WHATSAPP_NUMBER")

MEMORY_LIMIT = 100
json_text = None
agent = None
user_histories = defaultdict(list)
message_queue = queue.Queue()

# Worker: sends one message every 2 seconds
def message_worker():
    while True:
        to, body = message_queue.get()
        try:
            httpx.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Messages.json",
                data={"From": twilio_from, "To": to, "Body": body},
                auth=(twilio_sid, twilio_token),
                timeout=10
            )
            logging.info(f"Mensaje enviado a {to}: {body[:50]}")
        except Exception:
            logging.exception(f"Error enviando a {to}")
        time.sleep(2)
        message_queue.task_done()

threading.Thread(target=message_worker, daemon=True).start()

# Utility
def enqueue_message(to, body): message_queue.put((to, body))
def normalize(number): return number if number.startswith("whatsapp:") else f"whatsapp:{number}"

# Load and cache Excel data
 def cargar():
    global json_text
    if not json_text:
        gdown.download(
            "https://drive.google.com/uc?id=1zSbeJRK2tBTQOQmbAkipfBccLbb4LL_1",
            "data.xlsx", quiet=True
        )
        dfs = pd.read_excel("data.xlsx", sheet_name=None)
        json_text = json.dumps({k:v.to_dict(orient="records") for k,v in dfs.items()}, ensure_ascii=False)
    return json_text

# Initialize agent once
 def get_agent():
    global agent
    if not agent:
        agent = Agent(
            name="Excel_Read",
            instructions=f"Usa este JSON: {cargar()}",
            model="gpt-4.1"
        )
    return agent

# History per user
def record_history(uid, msg):
    h = user_histories[uid]
    h.append({"role":"user","content":msg})
    user_histories[uid] = h[-MEMORY_LIMIT:]

# Background processing
def process_input(msg, uid):
    record_history(uid, msg)
    res = Runner.run_sync(get_agent(), msg).final_output
    text = res or "Error generando respuesta"
    # split if long\ n
    for i in range(0, len(text), 1500):
        enqueue_message(uid, text[i:i+1500])

# Webhook
@app.route('/webhook', methods=['POST'])
def webhook():
    msg = request.values.get('Body','').strip()
    uid = normalize(request.values.get('From',''))
    if not msg: return Response(status=400)
    logging.info(f"Recibido de {uid}: {msg}")
    enqueue_message(uid, "Procesando...")
    threading.Thread(target=process_input, args=(msg,uid), daemon=True).start()
    return Response(status=200)

# Refresh endpoint
@app.route('/refresh', methods=['POST'])
def refresh():
    token = request.headers.get('Authorization')
    if os.getenv('REFRESH_TOKEN') and token!=os.getenv('REFRESH_TOKEN'):
        return "No autorizado",403
    global json_text, agent
    json_text = None; agent = None
    return "Refrescado",200

if __name__=='__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT',5000)))
















