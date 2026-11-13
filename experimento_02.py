
import os
from pyspark.sql import SparkSession

# Configuración del entorno
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

# Iniciar Spark
spark = (
    SparkSession.builder
    .appName("EXP02_Procesamiento_BigData")
    .master("local[*]")
    .getOrCreate()
)

# Rutas del proyecto
script_dir = os.path.dirname(os.path.abspath(__file__))

ruta_origen = os.path.join(
    script_dir, "data", "processed", "fact_pago.parquet"
)

ruta_destino = os.path.join(
    script_dir, "data", "experiments", "exp02_sintetico.parquet"
)

# Leer dataset procesado
df_base = spark.read.parquet(ruta_origen)

print("Registros originales:", df_base.count())

# Generar cuatro réplicas identificadas
replicas = spark.range(4).withColumnRenamed("id", "replica")

df_sintetico = df_base.crossJoin(replicas)

total = df_sintetico.count()

print("Registros sintéticos:", total)

# Guardar sin modificar los datos originales
(
    df_sintetico
    .repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("ANNIO_TRIBUTARIA")
    .parquet(ruta_destino)
)

print("Dataset sintético generado correctamente")
print("Destino:", ruta_destino)

spark.stop()
