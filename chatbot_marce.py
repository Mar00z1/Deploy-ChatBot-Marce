import os
import json
import logging
import gdown
import pandas as pd
import nest_asyncio
from flask import Flask, request, Response
from dotenv import load_dotenv
from agents import Agent, Runner

# Aplicar patch de asyncio para Jupyter/entornos embebidos
nest_asyncio.apply()

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Crear aplicación Flask
app = Flask(__name__)
app.debug = True  # Modo debug para ver peticiones

# Cargar variables de entorno
load_dotenv('apikey.env')
import openai
openai.api_key = os.getenv("OPENAI_API_KEY")

# Límite de memoria para historial conversacional
MEMORY_LIMIT = 100
history = []

# Función para descargar y procesar el archivo Excel
def cargar_dataframe():
    logging.info("Descargando Excel desde Google Drive...")
    url = "https://drive.google.com/uc?id=1zSbeJRK2tBTQOQmbAkipfBccLbb4LL_1"
    output = "MarcePrueba.xlsx"
    gdown.download(url, output, quiet=True)
    df_dict = pd.read_excel(output, sheet_name=None)
    serializable = {
        key: df.to_dict(orient="records") for key, df in df_dict.items()
    }
    json_text = json.dumps(serializable, ensure_ascii=False, indent=2, default=str)
    logging.info("Excel procesado con %d hojas", len(df_dict))
    return json_text

# Función para actualizar historial de conversación
def actualizar_historial(user_input):
    global history
    history.append({'role': 'user', 'content': user_input})
    # Mantener máximo MEMORY_LIMIT intercambios
    if len(history) > MEMORY_LIMIT * 2:
        history = history[-MEMORY_LIMIT * 2:]
    return history

# Ruta del webhook para WhatsApp
def log_request_values():
    data = request.values.to_dict()
    logging.info("Request values: %s", data)

@app.route('/webhook', methods=['POST'])
def webhook():
    log_request_values()
    user_input = request.values.get('Body', '').strip()
    if not user_input:
        logging.warning("Mensaje vacio recibido")
        return "Mensaje vacío", 400
    logging.info("Mensaje de usuario: %s", user_input)

    # Cargar datos y crear agente
    json_text = cargar_dataframe()
    agent = Agent(
        name="Excel_Read",
        instructions=(
            f"Sos un asistente de Llamas ventas. Usa este dataframe en texto plano "
            f"para responder preguntas: {json_text}"
        ),
        model="gpt-4.1"
    )

    # Actualizar historial y ejecutar agente
    historial = actualizar_historial(user_input)
    result = Runner.run_sync(agent, user_input)
    respuesta = result.final_output or "Lo siento, no pude generar respuesta."
    logging.info("Respuesta generada: %s", respuesta)

    # Devolver XML a Twilio
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Response><Message>' + respuesta + '</Message></Response>'
    )
    return xml, 200, {'Content-Type': 'application/xml'}

# Correr servidor
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logging.info("Arrancando servidor en 0.0.0.0:%d", port)
    app.run(host='0.0.0.0', port=port)






























