from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, count, avg, round
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import sys

# --- CONFIGURACIÓN DE KAFKA ---
KAFKA_BROKER = 'localhost:9092' 
KAFKA_TOPIC = 'clima_raw'        # Debe coincidir con el tema del Productor
APP_NAME = "SparkStreamingConsumerClimatologicas"

# Lista de columnas que necesitan limpieza y tipado
COLUMNAS_NUMERICAS = [
    "ALTITUD (m)", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
    "JUL", "AGO", "SEP", "OCT", "NOV", "DIC", "ANUAL"
]

# Definición del esquema que espera el CSV (MUY IMPORTANTE para el parsing)
# Los índices de las columnas deben coincidir con el orden del CSV.
# Usamos StringType primero porque los datos llegan como texto desde Kafka.
CSV_SCHEMA = StructType([
    StructField("DEPARTAMENTO", StringType()),  # Índice 0
    StructField("MUNICIPIO", StringType()),     # Índice 1
    StructField("ESTACIÓN", StringType()),      # Índice 2
    StructField("CÓDIGO", StringType()),        # Índice 3
    StructField("LATITUD", StringType()),       # Índice 4
    StructField("LONGITUD", StringType()),      # Índice 5
    StructField("ALTITUD (m)", StringType()),   # Índice 6
    StructField("PERIODO", StringType()),       # Índice 7
    StructField("ENE", StringType()),           # Índice 8
    StructField("FEB", StringType()),           # Índice 9
    StructField("MAR", StringType()),           # Índice 10
    StructField("ABR", StringType()),           # Índice 11
    StructField("MAY", StringType()),           # Índice 12
    StructField("JUN", StringType()),           # Índice 13
    StructField("JUL", StringType()),           # Índice 14
    StructField("AGO", StringType()),           # Índice 15
    StructField("SEP", StringType()),           # Índice 16
    StructField("OCT", StringType()),           # Índice 17
    StructField("NOV", StringType()),           # Índice 18
    StructField("DIC", StringType()),           # Índice 19
    StructField("ANUAL", StringType()),          # Índice 20
])

def procesar_stream(df_stream):
    """
    Aplica la limpieza, tipado y análisis al DataFrame de Streaming (Micro-batch).
    """
    
    # 1. Parsing: Convertir el String CSV a columnas
    # Kafka entrega los datos en la columna 'value' (en formato binario/bytes)
    df_parsed = df_stream.select(
        col("value").cast("string").alias("csv_string")
    )
    
    # Separar la cadena CSV por comas (delimitador)
    df_temp = df_parsed.select(
        split(col("csv_string"), ",").alias("data_array")
    )
    
    # Mapear los elementos del array a las columnas del esquema (Schema-on-Read)
    for i, field in enumerate(CSV_SCHEMA.fields):
        df_temp = df_temp.withColumn(
            field.name,
            col("data_array").getItem(i).cast(StringType()) # Aseguramos que son Strings para la limpieza
        )
    
    # Seleccionamos las columnas ya separadas
    df_limpio = df_temp.select(*CSV_SCHEMA.names)

    # 2. Limpieza y Tipado (Similar al Batch)
    for col_name in COLUMNAS_NUMERICAS:
        # Reemplazar comas (,) con vacío y forzar cast a Double
        df_limpio = df_limpio.withColumn(
            col_name,
            regexp_replace(col(col_name), ",", "").cast(DoubleType())
        )
    
    # El campo ALTITUD debe ser Integer
    df_limpio = df_limpio.withColumn(
        "ALTITUD (m)",
        col("ALTITUD (m)").cast(IntegerType())
    )
    
    # 3. EDA en el Stream (Agregación: Precipitación promedio por DEPARTAMENTO)
    # NOTA: Esta agregación se realiza solo sobre los datos que llegaron en este micro-batch
    df_eda = df_limpio.groupBy("DEPARTAMENTO").agg(
        round(avg("ANUAL"), 2).alias("PRECIPITACION_PROMEDIO_MB"),
        count("ESTACIÓN").alias("NUM_ESTACIONES_MB")
    ).orderBy(col("PRECIPITACION_PROMEDIO_MB").desc())

    return df_eda

def run_spark_streaming_consumer():
    """Función principal para inicializar y ejecutar el consumidor Spark Streaming."""
    
    # 1. Inicializar SparkSession
    # MUY IMPORTANTE: Incluir el paquete de Kafka para poder conectarse al broker
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR") # Para reducir la verbosidad de los logs

    print(f"\n--- SPARK STREAMING CONSUMER INICIADO ---")
    print(f"📡 Conectándose a Kafka Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC}...")

    # 2. Definir la fuente de lectura (Source: Kafka)
    df_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Aplicar el procesamiento (limpieza, tipado y EDA)
    # df_processed será un DataFrame de streaming (DataStreamWriter)
    df_processed = procesar_stream(df_raw_stream)

    # 4. Definir el sumidero (Sink: Consola)
    # 'outputMode("complete")' asegura que el resultado de la agregación completa
    # (todos los departamentos vistos hasta ahora) se muestre en cada intervalo.
    query = df_processed.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') # Intervalo de procesamiento: cada 5 segundos
        
    print("\n✅ Stream listo. Esperando datos. Abre la Terminal 3 para iniciar el Productor...")
    
    try:
        # Iniciar la consulta de streaming y bloquear hasta que se detenga (Ctrl+C)
        query.start().awaitTermination()
    except KeyboardInterrupt:
        print("\n👋 Deteniendo el Stream...")
    except Exception as e:
        print(f"\n❌ Error en el proceso de Streaming: {e}")
        
    spark.stop()

if __name__ == "__main__":
    run_spark_streaming_consumer()





