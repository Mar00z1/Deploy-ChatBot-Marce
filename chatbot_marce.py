def cargar():
    global json_text
    if json_text is None:
        logging.info("Descargando y procesando archivo Excel...")
        url = "https://drive.google.com/uc?id=1zSbeJRK2tBTQOQmbAkipfBccLbb4LL_1"
        output = "data.xlsx"
        gdown.download(url, output, quiet=True)
        df_dict = pd.read_excel(output, sheet_name=None)
        # Convert any datetime columns to ISO strings
        serializable = {}
        for sheet, df in df_dict.items():
            for col in df.select_dtypes(include=["datetime64[ns]"]):
                # use strftime for series-level conversion
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
            serializable[sheet] = df.to_dict(orient="records")
        # Use default=str to catch any leftover non-serializable types
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

# Record user history with a memory limit
def record_history(uid, msg):
    h = user_histories[uid]
    h.append({"role": "user", "content": msg})
    user_histories[uid] = h[-MEMORY_LIMIT:]

# Process incoming message in background
def process_input(msg, uid):
    record_history(uid, msg)
    res = Runner.run_sync(get_agent(), msg).final_output
    text = res or "Error generando respuesta"
    # Split into chunks for WhatsApp
    for i in range(0, len(text), 1500):
        enqueue_message(uid, text[i:i + 1500])

# Webhook endpoint
@app.route('/webhook', methods=['POST'])
def webhook():
    msg = request.values.get('Body', '').strip()
    uid = normalize(request.values.get('From', ''))
    if not msg:
        return Response(status=400)
    logging.info(f"Recibido de {uid}: {msg}")
    enqueue_message(uid, "Procesando...")
    threading.Thread(target=process_input, args=(msg, uid), daemon=True).start()
    return Response(status=200)

# Refresh endpoint to reload Excel and agent
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





















