from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
# Importamos funciones para la manipulación de columnas y tipos de datos (agregamos sum, avg, round, count)
from pyspark.sql.functions import col, regexp_replace, sum, avg, round, count
from pyspark.sql.types import DoubleType, IntegerType, StringType
import requests
import os
import sys

# --- CONFIGURACIÓN ---
URL_GITHUB_RAW = "https://raw.githubusercontent.com/122309/T3_ProcesamientoDataApacheSpark_LindsayQ/main/Normales_Climatol%C3%B3gicas_de_Colombia_20251020.csv"
LOCAL_FILE_PATH = "Normales_Climatologicas.csv"
APP_NAME = "ProcesamientoBatchClimatologicas"

# Lista de columnas que contienen valores numéricos mensuales o anuales y necesitan limpieza
COLUMNAS_NUMERICAS = [
    "ALTITUD (m)", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
    "JUL", "AGO", "SEP", "OCT", "NOV", "DIC", "ANUAL"
]

# --- PASO 1: DESCARGA DEL ARCHIVO ---
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

# --- PASO 2: LIMPIEZA Y TIPADO ---
def limpiar_y_tipar_dataframe(df):
    """
    Limpia los caracteres problemáticos (como las comas de miles) 
    y convierte las columnas a sus tipos numéricos.
    """
    print("\n--- PASO 2: Aplicando Limpieza y Tipado (Conversión de String a Double/Integer) ---")
    
    df_limpio = df
    
    # 1. Limpieza General (Remover comas de miles)
    for col_name in COLUMNAS_NUMERICAS:
        # Reemplazar comas (,) con nada
        df_limpio = df_limpio.withColumn(
            col_name,
            regexp_replace(col(col_name), ",", "") 
        )

    # 2. Tipado Específico: Convertir la mayoría a Double
    for col_name in COLUMNAS_NUMERICAS:
        # Usamos .cast() para forzar la conversión. Si falla, el valor será NULL.
        df_limpio = df_limpio.withColumn(
            col_name,
            col(col_name).cast(DoubleType())
        )
    
    # El campo ALTITUD debe ser Integer
    df_limpio = df_limpio.withColumn(
        "ALTITUD (m)",
        col("ALTITUD (m)").cast(IntegerType())
    )

    print(" Limpieza y tipado inicial completados.")
    return df_limpio


# --- FUNCIÓN PRINCIPAL DE EJECUCIÓN ---
def run_spark_batch():
    # Intenta descargar el archivo primero
    if not download_file(URL_GITHUB_RAW, LOCAL_FILE_PATH):
        sys.exit(1)

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    print("\n--- SESIÓN DE SPARK INICIADA ---")

    try:
        # 1. Carga del CSV desde la ruta LOCAL
        df_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .csv(LOCAL_FILE_PATH) 

        print("\n--- DATOS CARGADOS EXITOSAMENTE DESDE LA RUTA LOCAL ---")
        print(f"Número total de registros: {df_raw.count()}")

        # 2. Aplicar Limpieza y Tipado
        df_final = limpiar_y_tipar_dataframe(df_raw)

        # Verificación del Esquema y Filas
        print("\n--- ESQUEMA FINAL VERIFICADO (Tipo de Datos Corregido) ---")
        df_final.printSchema()
        print("\n--- Primeras 5 Filas del DataFrame Limpio ---")
        df_final.show(5, truncate=False)


        # =================================================================
        # --- PASO 3: EDA Y TRANSFORMACIÓN (Análisis Exploratorio de Datos) ---
        # =================================================================

        # 3A. Análisis Descriptivo (Estadísticas Resumen)
        print("\n--- PASO 3A: Análisis Descriptivo (Estadísticas Resumen) ---")
        print("Muestra el conteo, media, desviación estándar, mínimo y máximo de las columnas numéricas.")
        
        # Seleccionamos las columnas numéricas que ya convertimos a DoubleType
        df_final.select(COLUMNAS_NUMERICAS).describe().show()


        # 3B. Verificación de Valores Nulos (Parte de la Limpieza/Calidad de Datos)
        print("\n--- PASO 3B: Conteo de Valores Nulos por Columna ---")
        # Cuenta la suma de nulos por cada columna
        df_null_check = df_final.select([
            sum(col(c).isNull().cast("int")).alias(c) 
            for c in df_final.columns
        ])
        df_null_check.show(truncate=False)

        # 3C. Transformación y Agregación (Precipitación Promedio Anual por Departamento)
        print("\n--- PASO 3C: Transformación y Agregación ---")
        print("Calculando la Precipitación Anual Promedio (mm) por Departamento.")
        
        # 1. Agrupación por DEPARTAMENTO
        df_agregado = df_final.groupBy("DEPARTAMENTO").agg(
            # Calcula el promedio de la columna 'ANUAL' y lo redondea a 2 decimales
            round(avg("ANUAL"), 2).alias("PRECIPITACION_ANUAL_PROMEDIO_mm"),
            # Cuenta cuántas estaciones (filas) contribuyen al promedio por departamento
            count("ESTACIÓN").alias("NUM_ESTACIONES_REGISTRADAS")
        ).orderBy(col("PRECIPITACION_ANUAL_PROMEDIO_mm").desc())

        # 2. Muestra los resultados de la agregación
        print("\n Resultado de la Agregación (Top 10 Departamentos con más precipitación):")
        df_agregado.show(10, truncate=False)
        
        print(f"\nNúmero total de departamentos analizados: {df_agregado.count()}")

    except Exception as e:
        print(f"\n--- OCURRIÓ UN ERROR INESPERADO EN SPARK ---")
        print(f"Detalle del error: {e}")
        
    finally:
        spark.stop()
        print("\n--- SESIÓN DE SPARK FINALIZADA ---")

if __name__ == "__main__":
    run_spark_batch()
