import os
from pyspark.sql import SparkSession

java_path = (
    r"D:\DESCARGAS CHROME\java-21-openjdk-21.0.4.0.7-1.win.jdk.x86_64"
    r"\java-21-openjdk-21.0.4.0.7-1.win.jdk.x86_64"
)

hadoop_path = r"D:\hadoop"

java_executable = os.path.join(java_path, "bin", "java.exe")

if not os.path.isfile(java_executable):
    raise FileNotFoundError(
        f"No se encontro Java en la ruta configurada: "
        f"{java_executable}"
    )

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

print(f"Java configurado: {java_path}")
print(f"Hadoop configurado: {hadoop_path}")



spark = SparkSession.builder \
    .appName("ValidacionCalidad") \
    .master("local[*]") \
    .getOrCreate()

# Leer los datos procesados
script_dir = os.path.dirname(os.path.abspath(__file__))

ruta_parquet = os.path.join(
    script_dir,
    "data",
    "processed",
    "fact_pago.parquet"
)

df = spark.read.parquet(ruta_parquet)

# Verificar filas completamente iguales
total = df.count()
sin_duplicados = df.dropDuplicates().count()

print("Registros procesados:", total)
print("Filas exactamente duplicadas:", total - sin_duplicados)

spark.stop()
