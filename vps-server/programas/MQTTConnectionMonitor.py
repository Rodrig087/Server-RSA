######################################### ~Funciones~ #################################################
import json
import telebot
import paho.mqtt.client as mqtt
from threading import Thread
import time
import datetime
#######################################################################################################

##################################### ~Variables globales~ ############################################

#######################################################################################################

######################################### ~Funciones~ #################################################
# Función que inicia el bot de Telegram
def recibir_mensajes_telegram():
    # Manejador para el comando /help
    @bot.message_handler(commands=['help'])
    def send_help(message):
        bot.reply_to(message, "¿Cómo puedo ayudarte? Simplemente escribe tus preguntas.")
        print(message.chat.id)
    #bot.polling()
    try:
        bot.polling(none_stop=True, interval=0, timeout=50)
    except Exception as e:
        print(f"Error al realizar polling: {e}")

# Definir la función para extraer la fecha, hora y duración del payload JSON
def procesar_mensaje(mensaje):
    payload = json.loads(mensaje.payload)
    id = payload["id"]
    status = payload["status"]
    print(f"Id: {id}, status: {status}")
    return id, status

# Función para enviar mensajes a Telegram
def enviar_mensaje_telegram(id,mensaje):
    #group_id = -871014226  # ID de tu grupo
    bot.send_message(chat_id=id, text=mensaje)

# Funcion para leer el archivo de configuracion JSON
def read_fileJSON(nameFile):
    with open(nameFile, 'r') as f:
        data = json.load(f)
    return data 

# Función que se llama cuando el cliente se conecta al broker
def on_connect(client, userdata, flags, rc):
    global topic_status, topic_desconexion
    print("Conectado al broker MQTT con código de resultado: " + str(rc))
    if rc == 0:
        # Suscribir a los tópicos de conexión y desconexión
        client.subscribe(topic_status)  
    else:
        print("Conexion faliida")

# Función que se llama cuando se recibe un mensaje del broker
def on_message(client, userdata, msg):

    id_cliente, status_cliente = procesar_mensaje(msg)
        
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
def iniciar_cliente_mqtt(username, password, server_address):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username, password)
    client.connect(server_address, 1883, 60)
    client.loop_forever()

#######################################################################################################

############################################ ~Main~ ###################################################
def main():

    global topic_status, topic_desconexion
    global bot, id_chat, id_grupo

    # Lee el archivo de configuracion de telegram
    config_telegram = read_fileJSON("/home/rsa/configuracion/telegram-configuracion.json")
    TOKEN = config_telegram["token"]
    id_chat = config_telegram["chat-id"]
    id_grupo = config_telegram["group-id"]
    
    # Lee el archivo de configuracion mqtt
    config_mqtt = read_fileJSON("/home/rsa/configuracion/mqtt-configuracion.json")
    server_address = config_mqtt["server_address"]
    username = config_mqtt["username"]
    password = config_mqtt["password"]
    topic_status = config_mqtt["topic_status"]

    # Inicializa el bot de Telegram
    bot = telebot.TeleBot(TOKEN)

    # Inicia la recepcion de mensajes de Telegram en un hilo separado
    thread_bot_telegram = Thread(target=lambda: recibir_mensajes_telegram())
    thread_bot_telegram.start()

    # Espera que el bot esté listo antes de enviar el mensaje inicial
    time.sleep(1)  # Asegura un pequeño delay para que el bot inicie correctamente
    enviar_mensaje_telegram(id_grupo, "Server-Pi en linea")

    # Iniciar el cliente MQTT 
    iniciar_cliente_mqtt(username, password, server_address)

#######################################################################################################
if __name__ == '__main__':
    main()
#######################################################################################################