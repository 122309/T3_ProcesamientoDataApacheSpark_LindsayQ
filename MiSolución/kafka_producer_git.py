import time
import os
import sys
import requests
from kafka import KafkaProducer

# --- CONFIGURACIÓN DE FUENTE DE DATOS (REUTILIZADA DE TU SCRIPT) ---
URL_GITHUB_RAW = "https://raw.githubusercontent.com/122309/T3_ProcesamientoDataApacheSpark_LindsayQ/main/Normales_Climatol%C3%B3gicas_de_Colombia_20251020.csv"
LOCAL_FILE_PATH = "Normales_Climatologicas.csv"

# --- CONFIGURACIÓN DE KAFKA ---
KAFKA_BROKER = 'localhost:9092'  # Broker de Kafka
KAFKA_TOPIC = 'clima_raw'        # Tema al que enviaremos los datos

# --- PASO 1: DESCARGA DEL ARCHIVO (REUTILIZACIÓN DE TU CÓDIGO) ---
def download_file(url, local_path):
    """Descarga el archivo si no existe."""
    if os.path.exists(local_path):
        print(f" Archivo ya existe localmente en: {local_path}. Omitiendo descarga.")
        return True
    
    print(f" Descargando archivo desde: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() 

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f" Descarga completada. Archivo guardado en: {local_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f" Error de descarga. Por favor, verifica tu conexión a internet o la URL.")
        print(f"Detalle del error: {e}")
        return False

# --- PASO 2: PRODUCCIÓN DE MENSAJES ---
def run_producer():
    """
    Lee el archivo CSV (descargado de Git), línea por línea, y lo envía 
    como un stream de mensajes al broker de Kafka.
    """
    
    # Intenta descargar el archivo primero
    if not download_file(URL_GITHUB_RAW, LOCAL_FILE_PATH):
        sys.exit(1)

    # 1. Inicializar el Productor Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # Serializar: convertir el mensaje a bytes antes de enviarlo
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        print(f" Productor conectado a Kafka Broker: {KAFKA_BROKER}")
    except Exception as e:
        print(f" Error al conectar con Kafka. Asegúrate de que el broker esté activo en {KAFKA_BROKER}.")
        print(f"Detalle: {e}")
        sys.exit(1)

    # 2. Leer y enviar el archivo
    try:
        with open(LOCAL_FILE_PATH, 'r', encoding='utf-8') as f:
            
            # Recorrido del resto de los datos, incluyendo la cabecera
            for line_number, line in enumerate(f):
                record = line.strip()
                if record:
                    # Enviar el registro al tema (Topic) de Kafka
                    producer.send(KAFKA_TOPIC, record)
                    
                    if line_number == 0:
                         print(f" Enviando Cabecera: {record[:50]}...")
                    elif line_number % 50 == 0:
                         print(f"   Mensajes de datos enviados: {line_number}...")

                    # Simular un flujo de tiempo real con una pequeña pausa
                    time.sleep(0.01) # 10 milisegundos de pausa

            print(f"\n Todos los {line_number} registros de datos enviados a Kafka.")
            producer.flush() # Asegura que todos los mensajes pendientes sean enviados
            
    except Exception as e:
        print(f" Error durante el envío de datos: {e}")

    finally:
        producer.close()
        print(" Productor Kafka cerrado.")

if __name__ == "__main__":
    run_producer()
