############################################# ~ LIBRERIAS ~ ##################################################
import paho.mqtt.client as mqtt
import json
import os
import subprocess
import time
import sys
import random
from datetime import datetime
import csv
##############################################################################################################

############################################# ~ FUNCIONES ~ ##################################################
# Definir los callbacks para los eventos de conexión
def on_connect(client, userdata, flags, rc):
    print("Conectado al broker MQTT con código de resultado: " + str(rc))
    # Suscribirse al topic al conectarse
    client.subscribe(topicChanlud)
    client.subscribe(topicRegistroContinuo)
    client.subscribe(topicPromas)

# Funcion para crear el directorio y el archivo donde se guardaran los datos recuperados
def create_sensor_directory(base_path, sensor_type):
    directory_path = os.path.join(base_path, sensor_type)
    os.makedirs(directory_path, exist_ok=True)
    print('Se ha creado el archivo ' + base_path + sensor_type)
    return directory_path

# Acciones a ejecutar una vez que se recibió un mensaje
def on_message(client, userdata, msg):

    topic = msg.topic
    payload_str = msg.payload.decode('utf-8')    

    # El topic determina el directorio base donde se guardarán los archivos
    basePath = ''
    print(f"Valor de topic: {topic}")  
    if topic.startswith("chanlud/") and topic.endswith("/respuesta"):
        basePath = '/home/rsa/proyecto-chanlud/resultados/'
        print(basePath)
    elif topic.startswith("registrocontinuo/") and topic.endswith("/respuesta"):
        basePath = '/home/rsa/registro-continuo/resultados/'
        print(basePath)
    elif topic.startswith("promas/") and topic.endswith("/respuesta"):
        basePath = '/home/rsa/promas/resultados/'
        print(basePath)

    try:
        print("Mensaje recibido")
        # Parsear el mensaje JSON
        data = json.loads(payload_str)

        for sensor_type, sensor_data in data.items():
            # Crear el directorio basado en el tipo de sensor
            sensor_directory = create_sensor_directory(basePath,sensor_type)

            for sensor_name, sensor_info in sensor_data.items():
                # Crear un archivo CSV con el nombre del sensor
                file_name = os.path.join(sensor_directory, sensor_name + '.csv')

                # Extraer los datos necesarios y escribir en el archivo CSV
                timestamp = sensor_info["timeStamp"]
                mediciones = sensor_info["mediciones"]

                with open(file_name, mode='a+', newline='') as archivo_csv:
                    escritor_csv = csv.writer(archivo_csv)
                    escritor_csv.writerow([timestamp] + list(mediciones.values()))

    except json.JSONDecodeError:
        print("Error al decodificar el mensaje JSON:", payload_str)

########################################################################################################

############################################# ~ MAIN ~ #################################################

if __name__ == "__main__":

    # Lee el archivo de configuracion de parametros MQTT
    with open("/home/rsa/configuracion/mqtt-configuracion.json", "r") as config_file:
        config = json.load(config_file)
    server_address = config["server_address"]
    username = config["username"]
    password = config["password"]

    topicChanlud = config["topic_chanlud"]
    topicRegistroContinuo = config["topic_registro-continuo"]
    topicPromas = config["topic_promas"]
   
    # Crea una instancia del cliente MQTTc
    client = mqtt.Client()

    # Asigna los callbacks de conexión y de recepción de mensajes
    client.on_connect = on_connect
    client.on_message = on_message

    # Conexion al broker MQTT
    client.username_pw_set(username, password)
    client.connect(server_address, 1883, 60)

    # Inicia el loop para mantener la conexión y recibir mensajes
    client.loop_forever()

########################################################################################################

