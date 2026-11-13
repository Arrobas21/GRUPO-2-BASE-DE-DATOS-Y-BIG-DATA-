
import os
import time
import csv
import statistics

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# CONFIGURACION DE JAVA Y HADOOP
java_path = (
    r"D:\DESCARGAS CHROME\java-21-openjdk-21.0.4.0.7-1.win.jdk.x86_64"
    r"\java-21-openjdk-21.0.4.0.7-1.win.jdk.x86_64"
)

hadoop_path = r"D:\hadoop"

os.environ["JAVA_HOME"] = java_path
os.environ["HADOOP_HOME"] = hadoop_path
os.environ["hadoop.home.dir"] = hadoop_path
os.environ["PATH"] = (
    os.path.join(java_path, "bin")
    + os.pathsep
    + os.path.join(hadoop_path, "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)

# INICIAR SPARK
spark = (
    SparkSession.builder
    .appName("EXP02_Medicion_Rendimiento")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

script_dir = os.path.dirname(os.path.abspath(__file__))

ruta_entrada = os.path.join(
    script_dir, "data", "experiments",
    "exp02_sintetico.parquet"
)

ruta_salida = os.path.join(
    script_dir, "data", "experiments",
    "exp02_resultado.parquet"
)

ruta_csv = os.path.join(
    script_dir, "EXP02_resultados.csv"
)

# VALIDAR CANTIDAD DE REGISTROS
total = spark.read.parquet(ruta_entrada).count()

print("Registros a procesar:", total)

tiempos = []
resultados = []

# CINCO EJECUCIONES
for i in range(1, 6):
    inicio = time.perf_counter()

    df = spark.read.parquet(ruta_entrada)

    df_limpio = df.filter(
        (col("total_pago") > 0)
        & col("fecha_pago").isNotNull()
    )

    (
        df_limpio
        .repartition(8)
        .write
        .mode("overwrite")
        .partitionBy("ANNIO_TRIBUTARIA")
        .parquet(ruta_salida)
    )

    duracion = time.perf_counter() - inicio
    throughput = total / duracion

    tiempos.append(duracion)
    resultados.append([i, duracion, throughput])

    print(
        f"Ejecucion {i}: "
        f"{duracion:.3f} segundos | "
        f"{throughput:,.2f} registros/s"
    )

# ESTADISTICAS
promedio = statistics.mean(tiempos)
desviacion = statistics.stdev(tiempos)

print("\n===== RESULTADOS EXP-02 =====")
print(f"Registros: {total}")
print(f"Repeticiones: {len(tiempos)}")
print(f"Tiempo promedio: {promedio:.3f} s")
print(f"Desviacion estandar: {desviacion:.3f} s")
print(f"Throughput promedio: {statistics.mean([r[2] for r in resultados]):,.2f} reg/s")

# GUARDAR MEDICIONES
with open(ruta_csv, "w", newline="", encoding="utf-8") as archivo:
    escritor = csv.writer(archivo)
    escritor.writerow(
        ["Ejecucion", "Latencia_segundos", "Throughput_reg_s"]
    )
    escritor.writerows(resultados)

print("\nResultados guardados en:", ruta_csv)

spark.stop()
