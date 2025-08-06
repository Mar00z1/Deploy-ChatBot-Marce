import os
from flask import Flask, request
import pandas as pd
import gdown
import json
import openai
from agents import Agent, Runner
import nest_asyncio
from dotenv import load_dotenv

nest_asyncio.apply()
app = Flask(__name__)

# Configurar clave de OpenAI desde variable de entorno
load_dotenv('apikey.env')
openai.api_key = os.getenv("OPENAI_API_KEY")

# Límite de memoria para historial conversacional
MEMORY_LIMIT = 100
history = []

# Función para descargar y procesar el archivo Excel
def cargar_dataframe():
    url = "https://drive.google.com/uc?id=1zSbeJRK2tBTQOQmbAkipfBccLbb4LL_1"
    output = "MarcePrueba.xlsx"
    gdown.download(url, output, quiet=True)

    df_dict = pd.read_excel(output, sheet_name=None)
    serializable = {
        key: df.to_dict(orient="records") for key, df in df_dict.items()
    }
    json_text = json.dumps(serializable, ensure_ascii=False, indent=2, default=str)
    return json_text

# Función para actualizar historial de conversación
def actualizar_historial(user_input):
    global history
    history.append({'role': 'user', 'content': user_input})
    if len(history) > MEMORY_LIMIT * 2:
        history = history[-MEMORY_LIMIT * 2:]
    return history

# Ruta del webhook para WhatsApp
@app.route('/webhook', methods=['POST'])
def webhook():
    user_input = request.values.get('Body', '').strip()
    if not user_input:
        return "Mensaje vacío", 400

    json_text = cargar_dataframe()

    # Crear agente con los datos del Excel
    agent = Agent(
        name="Excel_Read",
        instructions=f"Sos un asistente de Llamas ventas. Usa este dataframe en texto plano para responder preguntas: {json_text}",
        model="gpt-4.1"
    )

    # Armar historial
    historial = actualizar_historial(user_input)

    # Ejecutar agente sincrónicamente
    result = Runner.run_sync(agent, user_input)
    respuesta = result.final_output

    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <Response><Message>{respuesta}</Message></Response>""", 200, {'Content-Type': 'application/xml'}

# Correr servidor
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))




























