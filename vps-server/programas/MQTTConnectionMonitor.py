######################################### ~Funciones~ #################################################
import json
import telebot
import paho.mqtt.client as mqtt
from threading import Thread
import time
import datetime
#######################################################################################################

##################################### ~Variables globales~ ############################################
bot = None
id_chat = None
id_grupo = None
topic_status = None
#######################################################################################################

######################################### ~Funciones~ #################################################
# Funcion para leer el archivo de configuracion JSON
def read_fileJSON(nameFile):
    try:
        with open(nameFile, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Archivo {nameFile} no encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"Error al decodificar el archivo {nameFile}.")
        return None

# Función que inicia el bot de Telegram
def recibir_mensajes_telegram():
    global bot
    # Manejador para el comando /help
    @bot.message_handler(commands=['help'])
    def send_help(message):
        bot.reply_to(message, "¿Cómo puedo ayudarte? Simplemente escribe tus preguntas.")
        print(message.chat.id)
    try:
        bot.polling(none_stop=True, interval=0, timeout=50)
    except Exception as e:
        print(f"Error al realizar polling: {e}")

# Definir la función para extraer la fecha, hora y duración del payload JSON
def procesar_mensaje(mensaje):
    try:
        # Intentar decodificar el payload JSON
        payload = json.loads(mensaje.payload.decode('utf-8'))  # Asegurarse de decodificar el payload de bytes a string
        id = payload.get("id", "ID no encontrado")  # Usar el método get para manejar la ausencia de 'id'
        status = payload.get("status", "Status no encontrado")  # Usar el método get para manejar la ausencia de 'status'
        print(f"Id: {id}, status: {status}")
        return id, status
    except json.JSONDecodeError:
        # Manejar el error específico de decodificación de JSON
        print("Error al decodificar JSON. Verifique el formato del mensaje.")
        return None, None
    except Exception as e:
        # Manejar cualquier otro tipo de error
        print(f"Error al procesar el mensaje: {e}")
        return None, None

# Función para enviar mensajes a Telegram
def enviar_mensaje_telegram(id, mensaje):
    global bot
    bot.send_message(chat_id=id, text=mensaje)

# Función que se llama cuando el cliente se conecta al broker
def on_connect(client, userdata, flags, rc):
    global topic_status
    print("Conectado al broker MQTT con código de resultado: " + str(rc))
    if rc == 0:
        # Suscribir a los tópicos de conexión y desconexión
        client.subscribe(topic_status)  
    else:
        print("Conexion fallida")

# Función que se llama cuando se recibe un mensaje del broker
def on_message(client, userdata, msg):
    id_cliente, status_cliente = procesar_mensaje(msg)
    if id_cliente is None or status_cliente is None:
        return  # No hacer nada si hay un error en el procesamiento del mensaje

    # Determinar el tópico del mensaje recibido
    if status_cliente == "on":
        event = "encendido"
    elif status_cliente == "online":
        event = "conectado"
    elif status_cliente == "offline":
        event = "desconectado"
    else:
        return  # Si el mensaje no es de los tópicos esperados, no hacer nada
    
    # Formatear la hora y fecha actuales
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Preparar y enviar el mensaje a Telegram
    mensaje_telegram = f"{id_cliente} {event} a las {current_time}"
    enviar_mensaje_telegram(id_grupo, mensaje_telegram)

# Función para iniciar el cliente MQTT
def iniciar_cliente_mqtt(config_mqtt):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message    
    try:
        client.username_pw_set(config_mqtt["username"], config_mqtt["password"])
        client.connect(config_mqtt["server_address"], 1883, 60)
     
        client.loop_start()
        while True:
            time.sleep(1)
    except Exception as e:
        print(f"Error al conectar o publicar en el broker MQTT: {e}")

    return client

#######################################################################################################

############################################ ~Main~ ###################################################
def main():

    global topic_status, bot, id_chat, id_grupo

    config_mqtt_path = "/home/rsa/configuracion/mqtt-configuracion.json"
    config_telegram_path = "/home/rsa/configuracion/telegram-configuracion.json"

    # Lee el archivo de configuración MQTT
    config_mqtt = read_fileJSON(config_mqtt_path)
    if config_mqtt is None:
        print("No se pudo leer el archivo de configuración MQTT. Terminando el programa.")
        return
    
    # Lee el archivo de configuración de Telegram
    config_telegram = read_fileJSON(config_telegram_path)
    if config_telegram is None:  # Verificar config_telegram en lugar de config_telegram_path
        print("No se pudo leer el archivo de configuración de Telegram. Terminando el programa.")
        return
        
    # Obtiene el token y los id de Telegram
    TOKEN = config_telegram.get("token", "Unknown")
    id_chat = config_telegram.get("chat-id", "Unknown")
    id_grupo = config_telegram.get("group-id", "Unknown")
    
    # Obtiene el topic de status 
    topic_status = config_mqtt.get("topic_status", "Unknown")

    # Inicializa el bot de Telegram
    bot = telebot.TeleBot(TOKEN)

    # Inicia la recepción de mensajes de Telegram en un hilo separado
    thread_bot_telegram = Thread(target=recibir_mensajes_telegram)
    thread_bot_telegram.start()

    # Espera que el bot esté listo antes de enviar el mensaje inicial
    time.sleep(1)  # Asegura un pequeño delay para que el bot inicie correctamente
    enviar_mensaje_telegram(id_grupo, "Server-Pi en linea")

    # Iniciar el cliente MQTT 
    client = iniciar_cliente_mqtt(config_mqtt)

    # Mantener el programa en ejecución
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupción del usuario recibida. Cerrando...")
        client.loop_stop()  # Detiene el loop de MQTT de forma limpia
        thread_bot_telegram.join()  # Espera que el hilo de Telegram termine

#######################################################################################################
if __name__ == '__main__':
    main()
#######################################################################################################
