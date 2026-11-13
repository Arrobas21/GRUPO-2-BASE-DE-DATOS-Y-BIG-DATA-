
import os
import re
import gc
import csv
import time
import statistics
import pandas as pd

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

print("Java configurado:", java_path)
print("Hadoop configurado:", hadoop_path)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CARPETA = os.path.dirname(os.path.abspath(__file__))

RUTA_CSV = os.path.join(
    CARPETA, "data", "raw",
    "DATASET_RECAUDACION ARBITRARIA - 2022 - MPM.csv"
)

RUTA_RESULTADOS = os.path.join(
    CARPETA, "EXP03_resultados.csv"
)

TOTAL_ORIGINAL = 278646

spark = (
    SparkSession.builder
    .appName("EXP03_Pandas_vs_PySpark")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def pandas_proceso():
    df = pd.read_csv(
        RUTA_CSV,
        sep=";",
        encoding="utf-8-sig",
        dtype="string",
        keep_default_na=False
    )

    df.columns = [
        re.sub(r"[^A-Za-z0-9_]+", "_", c.strip())
        .strip("_").upper()
        for c in df.columns
    ]

    importe = df["TOTAL_PAGO"].str.strip()

    importe = importe.where(
        ~importe.str.contains(",", regex=False),
        importe.str.replace(".", "", regex=False)
               .str.replace(",", ".", regex=False)
    )

    importe = pd.to_numeric(importe, errors="coerce")

    fecha = df["FECHA_PAGO"].str.strip()
    fecha = fecha.where(fecha.str.len() != 9, fecha.str[1:])
    fecha = fecha.str.replace("-", "", regex=False)

    fecha = pd.to_datetime(
        fecha, format="%Y%m%d", errors="coerce"
    )

    validos = (importe > 0) & fecha.notna()

    return int(validos.sum()), float(importe[validos].sum())


def pyspark_proceso():
    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
        .csv(RUTA_CSV)
    )

    nuevos_nombres = [
        re.sub(r"[^A-Za-z0-9_]+", "_", c.strip())
        .strip("_").upper()
        for c in df.columns
    ]

    df = df.toDF(*nuevos_nombres)

    importe = F.trim(F.col("TOTAL_PAGO"))

    importe = F.when(
        F.instr(importe, ",") > 0,
        F.regexp_replace(
            F.regexp_replace(importe, r"\.", ""),
            ",", "."
        )
    ).otherwise(importe)

    fecha = F.trim(F.col("FECHA_PAGO"))

    fecha = F.when(
        F.length(fecha) == 9,
        F.substring(fecha, 2, 8)
    ).otherwise(fecha)

    fecha_convertida = (
        F.when(
            F.length(fecha) == 8,
            F.try_to_date(fecha, "yyyyMMdd")
        )
        .when(
            fecha.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.try_to_date(fecha, "yyyy-MM-dd")
        )
    )

    df = df.select(
        importe.cast("double").alias("monto"),
        fecha_convertida.alias("fecha")
    ).filter(
        (F.col("monto") > 0)
        & F.col("fecha").isNotNull()
    )

    resultado = df.agg(
        F.count("*").alias("registros"),
        F.sum("monto").alias("recaudacion")
    ).collect()[0]

    return (
        int(resultado["registros"]),
        float(resultado["recaudacion"])
    )


resultados = []

try:
    for repeticion in range(1, 6):
        # Alternar el orden para reducir sesgos
        orden = (
            ["Pandas", "PySpark"]
            if repeticion % 2
            else ["PySpark", "Pandas"]
        )

        for motor in orden:
            gc.collect()

            inicio = time.perf_counter()

            if motor == "Pandas":
                registros, recaudacion = pandas_proceso()
            else:
                registros, recaudacion = pyspark_proceso()

            segundos = time.perf_counter() - inicio

            # Validar resultados antes de compararlos
            if registros != 278499:
                raise ValueError(
                    f"{motor}: registros inesperados: {registros}"
                )

            if abs(recaudacion - 756022.67) > 0.02:
                raise ValueError(
                    f"{motor}: monto inesperado: {recaudacion}"
                )

            throughput = TOTAL_ORIGINAL / segundos

            resultados.append([
                repeticion, motor,
                round(segundos, 4),
                round(throughput, 2),
                registros,
                round(recaudacion, 2)
            ])

            print(
                f"Repeticion {repeticion} | {motor}: "
                f"{segundos:.3f} s | "
                f"{throughput:,.2f} reg/s"
            )

    with open(
        RUTA_RESULTADOS, "w",
        newline="", encoding="utf-8"
    ) as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow([
            "Repeticion", "Motor", "Latencia_seg",
            "Throughput_reg_s", "Registros_validos",
            "Recaudacion"
        ])
        escritor.writerows(resultados)

    print("\n===== RESULTADOS EXP-03 =====")

    for motor in ["Pandas", "PySpark"]:
        tiempos = [
            r[2] for r in resultados if r[1] == motor
        ]

        print(f"\n{motor}")
        print(f"Repeticiones: {len(tiempos)}")
        print(f"Promedio: {statistics.mean(tiempos):.3f} s")
        print(
            f"Desviacion estandar: "
            f"{statistics.stdev(tiempos):.3f} s"
        )
        print(
            f"Throughput promedio: "
            f"{statistics.mean([
                r[3] for r in resultados
                if r[1] == motor
            ]):,.2f} reg/s"
        )

    print("\nArchivo generado:", RUTA_RESULTADOS)

finally:
    spark.stop()
