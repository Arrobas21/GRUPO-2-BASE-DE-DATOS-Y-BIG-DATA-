
from pathlib import Path
from pyspark.sql import SparkSession

# Ubicación de este script
BASE = Path(__file__).resolve().parent

# Iniciar Spark
spark = (
    SparkSession.builder
    .appName("Exportacion_PowerBI_Moyobamba")
    .master("local[*]")
    .getOrCreate()
)

# Leer los datos procesados anteriormente
origen = BASE / "data" / "processed" / "fact_pago.parquet"

df = spark.read.parquet(str(origen))

# Validar registros
print("Total de registros:", df.count())
df.printSchema()
df.show(5, truncate=False)

# Crear una salida consolidada para Power BI
destino = BASE / "data" / "bi_export"

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .parquet(str(destino))
)

print("Exportación completada:", destino)

spark.stop()
