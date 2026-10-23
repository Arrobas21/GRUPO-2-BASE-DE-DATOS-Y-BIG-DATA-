from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PruebaSpark")
    .master("local[*]")
    .getOrCreate()
)

print("Version de Spark:", spark.version)

spark.range(5).show()

spark.stop()
