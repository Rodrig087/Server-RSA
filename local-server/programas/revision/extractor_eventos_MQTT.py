import paho.mqtt.client as mqtt
import json
import os
import datetime
import subprocess
import time
from datetime import datetime
import csv
import threading
##################################### ~VARIABLES_GLOBALES~ #############################################
almacenamiento = []                                        # Almacena los valores capturados en cada msg
t_espera = 20                                              # Tiempo de espera para cada mensaje 

bandera = 0
##################################### ~MQTT Configuration~ #############################################
# Definir las variables
def read_fileJSON(nameFile):
    with open(nameFile) as f:
        data = json.load(f)
    return data 

dates_mqtt = read_fileJSON("/home/rsa/configuracion/mqtt-configuracion.json")

server_address = dates_mqtt["server_address"]
username = dates_mqtt["username"]
password = dates_mqtt["password"]
topic = dates_mqtt["topic_registro-continuo"]

#######################################################################################################
# Definir la función para extraer la fecha, hora y duración del payload JSON
def procesar_mensaje(mensaje):
    payload = json.loads(mensaje.payload)
    fecha = payload["fecha"]
    hora = payload["hora"]
    duracion = payload["duracion"]
    print(f"Fecha: {fecha}, Hora: {hora}, Duración: {duracion}")

# Definir los callbacks para los eventos de conexión
def on_connect(client, userdata, flags, rc):
    print("Conectado al broker MQTT con código de resultado: " + str(rc))
    # Suscribirse al topic al conectarse
    client.subscribe(topic)

def on_message(client, userdata, msg):
    global almacenamiento
    global bandera

    if bandera == 0:
        bandera = 1  
        temporizador_hilo = threading.Thread(target=temporizador)
        # Iniciar el hilo del temporizador
        temporizador_hilo.start()
        
    print("Mensaje recibido en el topic " + msg.topic + " con el contenido: " + str(msg.payload))
    #procesar_mensaje(msg)
    payload_str = msg.payload.decode('utf-8')
    payload_json = json.loads(payload_str)
    almacenamiento.append(payload_json)
    

def publicar_mensaje(client, topic, mensaje):
    client.publish(topic, mensaje)

def read_fileJSON(nameFile):
    with open(nameFile) as f:
        data = json.load(f)
    return data 

def determinacion_ubicacion(msg,almacenamiento1):
    if not msg in almacenamiento1:
        almacenamiento1.append(msg) 
    return almacenamiento1

def exportar_csv(direc,nameFile,dates_cath):
    name = direc+'/'+nameFile+'.csv'
    print(name)
    with open(name, mode='a+', newline='') as archivo_csv:
        escritor_csv = csv.writer(archivo_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        escritor_csv.writerow(dates_cath)

def timeStamp_Mayor_Menor(cath,almMenor,almMayor):
    capturado =  datetime.strptime(cath, "%H:%M:%S")
    almMenor_datetime = datetime.strptime(almMenor, "%H:%M:%S")
    almMayor_datetime = datetime.strptime(almMayor, "%H:%M:%S")

    if capturado < almMenor_datetime: 
        almMenor_datetime = capturado
    elif capturado > almMayor_datetime:
        almMayor_datetime = capturado
    diferencia = almMayor_datetime - almMenor_datetime

    return almMenor_datetime.strftime("%H:%M:%S"),almMayor_datetime.strftime("%H:%M:%S"),diferencia.total_seconds()

def temporizador():
    global almacenamiento
    global bandera
    time.sleep(t_espera)  # Esperar 10 segundos
    """
    if len(almacenamiento)>=1:
        procesamiento_datos(almacenamiento)
    else:
        print("No se capturó ningún mensaje en ", t_espera, "s")     
    almacenamiento = []
    """
    procesamiento_datos(almacenamiento)
    bandera = 0
    almacenamiento = []


def procesamiento_datos(alm): 
    almacenamiento1 = []                                       # Almacena las ubicaciones capturadas 
    almacenamiento2 = []                                       # Almacena los identificadores de la estacion
    timeStampMayor = ''                                        # Almacena el TimeStamp Mayor 
    timeStampMenor = ''                                        # Almacena el TimeStamp Menor
    duracionMayor = 0                                          # Almacena la Duracion Menor
    tam = len(alm)
    for conta in range(1,tam+1):
        payload_json = alm[conta-1]

        ubicacion_key = list(payload_json.keys()) 
        ubicacion = ubicacion_key[0]
        
        payload_json_ubi = payload_json[ubicacion] 
        
        idAcelerografo_key = list(payload_json_ubi.keys())
        idAcelerografio = idAcelerografo_key[0]

        inicio = payload_json[ubicacion][idAcelerografio]["inicio"]
        
        duracion = payload_json[ubicacion][idAcelerografio]["duracion"]

        fecha = inicio[0:10]

        if conta == 1 : 
            timeStampMenor = inicio[11:19]
            timeStampMayor = timeStampMenor
            Delta = 0
            duracionMayor = float(duracion)
        else: 
            timeStampMenor, timeStampMayor, Delta = timeStamp_Mayor_Menor(inicio[11:19],timeStampMenor,timeStampMayor)
            if duracionMayor < float(duracion):
                duracionMayor = float(duracion)
        almacenamiento1=determinacion_ubicacion(ubicacion,almacenamiento1)
        almacenamiento2=determinacion_ubicacion(idAcelerografio,almacenamiento2)
        
        result = [fecha,idAcelerografio,timeStampMenor,Delta,duracionMayor]
    
    print("******************************************************************************************************************")
    print(">>Datos Capturados [Fecha, Ultimo Acelerografo, TimeStamp Menor, Delta_TimeStamp, Mayor Duracion]\n",almacenamiento)
    print(">>Ubicaciones del Evento: ",almacenamiento1)
    print(">>Msg Recibidos",conta)

    # Procesamiento de los datos
    paths = read_fileJSON("/home/rsa/configuracion/paths_extractor_mqtt.json")


    if (len(almacenamiento2) == 1) and (len(almacenamiento1) == 1):
        print(">>Evento: AISLADO")
        op = 'aislado'
        nameFile1 = result[1]
    elif len(almacenamiento1)>1:
        print(">>Evento: SISMICO") 
        op = 'sismico'
        nameFile1 = result[0]
    elif (len(almacenamiento2) > 1) and (len(almacenamiento1) == 1):
        print(">>Evento: LOCAL")
        op = 'local'
        nameFile1 = almacenamiento1[0]
    exportar_csv(paths[op],nameFile1,[result[0]+'T'+result[2]+'Z',round(result[3]+result[4],2)])
    print("******************************************************************************************************************")
# Crear una instancia del cliente MQTTc
client = mqtt.Client()

# Asignar los callbacks de conexión y de recepción de mensajes
client.on_connect = on_connect
client.on_message = on_message

# Conectarse al broker MQTT
client.username_pw_set(username, password)
client.connect(server_address, 1883, 60)

"""
temporizador_hilo = threading.Thread(target=temporizador)

# Iniciar el hilo del temporizador
temporizador_hilo.start()
"""

# Iniciar el loop para mantener la conexión y recibir mensajes durante el tiempo de espera
client.loop_forever()