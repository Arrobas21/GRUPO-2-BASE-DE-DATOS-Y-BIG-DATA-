
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


import os

java_path = r"D:\DESCARGAS CHROME\java-21-openjdk-21.0.4.0.7-1.win.jdk.x86_64\java-21-openjdk-21.0.4.0.7-1.win.jdk.x86_64"
hadoop_path = r"D:\hadoop"

os.environ["JAVA_HOME"] = java_path
os.environ["HADOOP_HOME"] = hadoop_path
os.environ["PATH"] = (
    os.path.join(java_path, "bin")
    + os.pathsep
    + os.path.join(hadoop_path, "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)


spark = (
    SparkSession.builder
    .appName("Validar_Recaudacion_Moyobamba")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet("data/processed/fact_pago.parquet")

print("REGISTROS:", df.count())

total = df.agg(
    spark_sum("total_pago")
).first()[0]

print(f"RECAUDACION TOTAL: S/ {total:,.2f}")

spark.stop()
