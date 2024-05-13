######################################### ~Funciones~ #################################################
import json
import paho.mqtt.client as mqtt
#######################################################################################################

##################################### ~Variables globales~ ############################################
variable = 0
#######################################################################################################

######################################### ~Funciones~ #################################################
# Funcion para leer el archivo de configuracion JSON
def read_fileJSON(nameFile):
    with open(nameFile, 'r') as f:
        data = json.load(f)
    return data 

# Función para iniciar el cliente MQTT
def iniciar_cliente_mqtt(username, password, server_address):
    client = mqtt.Client()

    # Establecer Last Will and Testament (LWT)
    lwt_topic = "status/desconectado"  # Tópico LWT para notificar desconexiones
    lwt_message = f"SER01"
    client.will_set(lwt_topic, payload=lwt_message, qos=1, retain=False)

    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username, password)
    client.connect(server_address, 1883, 60)
    # Publicar mensaje de inicio
    publicar_mensaje(client, topic_status, "SER01")
    client.loop_forever()

# Función que se llama cuando el cliente se conecta al broker
def on_connect(client, userdata, flags, rc):
    global topic_status, topic_desconexion
    print("Conectado al broker MQTT con código de resultado: " + str(rc))
    
# Función que se llama cuando se recibe un mensaje del broker
'''
def on_message(client, userdata, msg):
    # Determinar el tópico del mensaje recibido
    if msg.topic == topic_conexion:
        event = "conectado"
    elif msg.topic == topic_desconexion:
        event = "desconectado"
    else:
        return  # Si el mensaje no es de los tópicos esperados, no hacer nada
    
    # Formatear la hora y fecha actuales
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Preparar y enviar el mensaje a Telegram
    id_cliente = msg.payload.decode()  # Recupera el ID del cliente del contenido del mensaje
    mensaje_telegram = f"{id_cliente} {event} a las {current_time}"
    enviar_mensaje_telegram(id_grupo, mensaje_telegram)
'''

# Funcion para publicar mensajes por MQTT
def publicar_mensaje(client, topic, mensaje):
    client.publish(topic, mensaje)

#######################################################################################################

############################################ ~Main~ ###################################################
def main():

    global client, topic_status, topic_desconexion

    # Lee el archivo de configuracion mqtt
    config_mqtt = read_fileJSON("/home/rsa/configuracion/mqtt-configuracion.json")
    server_address = config_mqtt["server_address"]
    username = config_mqtt["username"]
    password = config_mqtt["password"]
    topic_status = config_mqtt["topic_status"]

    # Iniciar el cliente MQTT 
    iniciar_cliente_mqtt(username, password, server_address)

    

#######################################################################################################
if __name__ == '__main__':
    main()
#######################################################################################################
