import os
import json
import gdown
import logging
import nest_asyncio
import pandas as pd

from flask import Flask, request
from dotenv import load_dotenv
from agents import Agent, Runner
from collections import defaultdict

# ------------------- Setup inicial -------------------
load_dotenv()
nest_asyncio.apply()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

openai_key = os.getenv("OPENAI_API_KEY")
if not openai_key:
    raise RuntimeError("Falta definir OPENAI_API_KEY como variable de entorno.")

refresh_token = os.getenv("REFRESH_TOKEN")
if not refresh_token:
    logging.warning("No se definió REFRESH_TOKEN. La ruta /refresh será insegura.")

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
        serializable = {
            key: df.to_dict(orient="records") for key, df in df_dict.items()
        }
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

# ------------------- Webhook principal -------------------
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        user_input = request.values.get('Body', '').strip()
        user_id = request.values.get('From', 'anon')

        if not user_input:
            return "Mensaje vacío", 400

        logging.info(f"[{user_id}] Mensaje recibido: {user_input}")

        agent = inicializar_agente()
        historial = actualizar_historial(user_id, user_input)

        result = Runner.run_sync(agent, user_input)
        respuesta = result.final_output

        logging.info(f"[{user_id}] Respuesta enviada: {respuesta}")

        return f"""<?xml version="1.0" encoding="UTF-8"?>
<Response><Message>{respuesta}</Message></Response>""", 200, {'Content-Type': 'application/xml'}

    except Exception as e:
        logging.exception("Error en el webhook:")
        return "Error interno", 500

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


