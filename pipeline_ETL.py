
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    sha2,
    regexp_replace,
    when,
    instr,
    length,
    try_to_date,
    desc,
    substring
)


# ============================================================
# 1. CONFIGURACION DEL ENTORNO
# ============================================================

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


# ============================================================
# 2. FUNCION PRINCIPAL DEL PIPELINE ETL
# ============================================================

def ejecutar_pipeline():

    spark = None

    try:
        print("\nIniciando motor de Apache Spark...")

        spark = (
            SparkSession.builder
            .appName("ETL_Municipalidad_Moyobamba")
            .config("spark.ui.port", "4040")
            .getOrCreate()
        )

        print(
            f"Spark iniciado correctamente: "
            f"version {spark.version}"
        )

        # ----------------------------------------------------
        # 3. DEFINIR LAS RUTAS
        # ----------------------------------------------------

        script_dir = os.path.dirname(
            os.path.abspath(__file__)
        )

        path_source = os.path.join(
            script_dir,
            "data",
            "raw",
            "DATASET_RECAUDACION ARBITRARIA - 2022 - MPM.csv"
        )

        path_target = os.path.join(
            script_dir,
            "data",
            "processed",
            "fact_pago.parquet"
        )

        if not os.path.isfile(path_source):
            raise FileNotFoundError(
                f"No se encuentra el archivo CSV:\n{path_source}"
            )

        os.makedirs(
            os.path.dirname(path_target),
            exist_ok=True
        )

        # ----------------------------------------------------
        # 4. EXTRAER LOS DATOS DEL CSV
        # ----------------------------------------------------

        print("\n1. Cargando archivo CSV original...")

        df_raw = (
            spark.read
            .option("header", "true")
            .option("delimiter", ";")
            .option("encoding", "UTF-8")
            .option("inferSchema", "false")
            .csv(path_source)
        )

        total_original = df_raw.count()

        print(f"Registros leidos: {total_original}")

        # ----------------------------------------------------
        # 5. NORMALIZAR LOS NOMBRES DE LAS COLUMNAS
        # ----------------------------------------------------

        print("\n2. Normalizando nombres de columnas...")

        nombres_usados = set()

        for nombre_original in df_raw.columns:

            nombre_limpio = nombre_original.strip()
            nombre_limpio = nombre_limpio.replace(".", "")

            nombre_limpio = re.sub(
                r"[^A-Za-z0-9_]+",
                "_",
                nombre_limpio
            )

            nombre_limpio = nombre_limpio.strip("_")

            if not nombre_limpio:
                nombre_limpio = "COLUMNA"

            nombre_base = nombre_limpio
            contador = 2

            while nombre_limpio.lower() in nombres_usados:
                nombre_limpio = f"{nombre_base}_{contador}"
                contador += 1

            nombres_usados.add(nombre_limpio.lower())

            if nombre_original != nombre_limpio:
                df_raw = df_raw.withColumnRenamed(
                    nombre_original,
                    nombre_limpio
                )

        print("Columnas normalizadas:")
        print(df_raw.columns)

        # ----------------------------------------------------
        # 6. VALIDAR LAS COLUMNAS OBLIGATORIAS
        # ----------------------------------------------------

        columnas_requeridas = [
            "TOTAL_PAGO",
            "FECHA_PAGO",
            "DESCR_TRIBUTARIA",
            "COD_CONTRIBUYENTE",
            "ANNIO_TRIBUTARIA"
        ]

        faltantes = [
            nombre
            for nombre in columnas_requeridas
            if nombre not in df_raw.columns
        ]

        if faltantes:
            raise ValueError(
                f"Faltan columnas requeridas: {faltantes}\n"
                f"Columnas disponibles: {df_raw.columns}"
            )

        # ----------------------------------------------------
        # 6.1 DIAGNOSTICO DE LOS FORMATOS DE FECHA
        # ----------------------------------------------------

        print("\n2.1 Diagnosticando los valores de FECHA_PAGO...")

        print("\nFechas originales mas frecuentes:")

        df_raw.select(
            trim(col("FECHA_PAGO")).alias("fecha_original")
        ).groupBy(
            "fecha_original"
        ).count().orderBy(
            desc("count")
        ).show(30, truncate=False)

        print("\nCantidad de registros por longitud de fecha:")

        df_raw.select(
            length(trim(col("FECHA_PAGO"))).alias(
                "longitud_fecha"
            )
        ).groupBy(
            "longitud_fecha"
        ).count().orderBy(
            "longitud_fecha"
        ).show(truncate=False)

        print(
            "\nEjemplos de fechas con longitud distinta "
            "de 8 o 10:"
        )

        df_raw.filter(
            ~length(trim(col("FECHA_PAGO"))).isin(8, 10)
        ).select(
            "FECHA_PAGO"
        ).show(30, truncate=False)

        # ----------------------------------------------------
        # 6.2 DIAGNOSTICO DE FECHAS DE NUEVE DIGITOS
        # ----------------------------------------------------

        print("\n2.2 Analizando fechas de nueve digitos...")

        df_nueve = (
            df_raw
            .withColumn(
                "fecha_original",
                trim(col("FECHA_PAGO").cast("string"))
            )
            .filter(
                length(col("fecha_original")) == 9
            )
            .withColumn(
                "prefijo_fecha",
                substring(col("fecha_original"), 1, 1)
            )
            .withColumn(
                "fecha_sin_prefijo",
                substring(col("fecha_original"), 2, 8)
            )
            .withColumn(
                "fecha_convertida",
                try_to_date(
                    col("fecha_sin_prefijo"),
                    "yyyyMMdd"
                )
            )
        )

        print("\nDistribucion de prefijos:")

        df_nueve.groupBy(
            "prefijo_fecha"
        ).count().orderBy(
            desc("count")
        ).show(truncate=False)

        print("\nEjemplos de conversion:")

        df_nueve.select(
            "fecha_original",
            "prefijo_fecha",
            "fecha_sin_prefijo",
            "fecha_convertida"
        ).show(30, truncate=False)

        print("\nResumen de fechas de nueve digitos:")

        df_nueve.select(
            when(
                col("fecha_convertida").isNotNull(),
                "Fecha valida al quitar el primer digito"
            ).otherwise(
                "Fecha invalida incluso sin quitar el primer digito"
            ).alias("resultado")
        ).groupBy(
            "resultado"
        ).count().show(truncate=False)

        # ----------------------------------------------------
        # 7. LIMPIAR Y TRANSFORMAR LOS DATOS
        # ----------------------------------------------------

        print("\n3. Aplicando limpieza y transformaciones...")

        # ----------------------------------------------------
        # 7.1 LIMPIAR EL IMPORTE
        # ----------------------------------------------------
        # Ejemplo: 1.234,56 -> 1234.56
        # Ejemplo: 1234.56 -> 1234.56

        importe_texto = trim(
            col("TOTAL_PAGO").cast("string")
        )

        importe_limpio = when(
            instr(importe_texto, ",") > 0,
            regexp_replace(
                regexp_replace(
                    importe_texto,
                    r"\.",
                    ""
                ),
                ",",
                "."
            )
        ).otherwise(
            importe_texto
        )

        df_importes = df_raw.withColumn(
            "total_pago",
            importe_limpio.cast("double")
        )

        # Conservar solamente importes numericos positivos.

        df_importes_validos = df_importes.filter(
            col("total_pago").isNotNull()
            & (col("total_pago") > 0)
        )

        total_importes_validos = (
            df_importes_validos.count()
        )

        descartados_importe = (
            total_original - total_importes_validos
        )

        print(
            f"Registros con importe valido: "
            f"{total_importes_validos}"
        )

        print(
            f"Registros descartados por importe: "
            f"{descartados_importe}"
        )

        # ----------------------------------------------------
        # 7.2 NORMALIZAR Y CONVERTIR LAS FECHAS
        # ----------------------------------------------------
        # Formato 1: 20220801 -> 2022-08-01
        # Formato 2: 2022-08-01 -> 2022-08-01
        # Formato 3: 120220905 -> 2022-09-05
        #
        # En el formato de nueve digitos se intenta interpretar
        # los ultimos ocho como yyyyMMdd.
        # Esta regla debe validarse con el origen del dataset.

        fecha_texto = trim(
            col("FECHA_PAGO").cast("string")
        )

        fecha_sin_prefijo = substring(
            fecha_texto,
            2,
            8
        )

        df_fechas = (
            df_importes_validos
            .withColumn(
                "fecha_pago",
                when(
                    length(fecha_texto) == 8,
                    try_to_date(
                        fecha_texto,
                        "yyyyMMdd"
                    )
                ).when(
                    length(fecha_texto) == 9,
                    try_to_date(
                        fecha_sin_prefijo,
                        "yyyyMMdd"
                    )
                ).when(
                    fecha_texto.rlike(
                        r"^\d{4}-\d{2}-\d{2}$"
                    ),
                    try_to_date(
                        fecha_texto,
                        "yyyy-MM-dd"
                    )
                ).otherwise(None)
            )
        )

        total_fechas_validas = (
            df_fechas.filter(
                col("fecha_pago").isNotNull()
            ).count()
        )

        descartados_fecha = (
            total_importes_validos - total_fechas_validas
        )

        print(
            f"Registros con fecha valida: "
            f"{total_fechas_validas}"
        )

        print(
            f"Registros descartados por fecha: "
            f"{descartados_fecha}"
        )

        # Mantener unicamente registros con fechas validas.

        df_cleaned = df_fechas.filter(
            col("fecha_pago").isNotNull()
        )

        # ----------------------------------------------------
        # 7.3 ESTANDARIZAR LA DESCRIPCION TRIBUTARIA
        # ----------------------------------------------------

        df_cleaned = df_cleaned.withColumn(
            "descr_tributaria",
            upper(
                trim(
                    regexp_replace(
                        col("DESCR_TRIBUTARIA"),
                        "(?i)publica",
                        "PÚBLICA"
                    )
                )
            )
        )

        # ----------------------------------------------------
        # 7.4 SEUDONIMIZAR EL CODIGO DEL CONTRIBUYENTE
        # ----------------------------------------------------

        df_cleaned = df_cleaned.withColumn(
            "cod_contribuyente_hash",
            sha2(
                trim(
                    col("COD_CONTRIBUYENTE").cast("string")
                ),
                256
            )
        )

        # ----------------------------------------------------
        # 8. PREPARAR EL DATASET FINAL
        # ----------------------------------------------------

        print("\n4. Validando los datos transformados...")

        # Excluir el codigo original del contribuyente.

        df_final = df_cleaned.select(
            col("ANNIO_TRIBUTARIA"),
            col("total_pago"),
            col("fecha_pago"),
            col("descr_tributaria"),
            col("cod_contribuyente_hash"),
            col("DEPARTAMENTO").alias("departamento"),
            col("PROVINCIAL").alias("provincia"),
            col("DISTRITO").alias("distrito"),
            col("UBIGEO").alias("ubigeo"),
            col("MES").alias("mes_tributario")
        )

        total_final = df_final.count()

        print(f"Registros originales: {total_original}")
        print(
            f"Descartados por importe: "
            f"{descartados_importe}"
        )
        print(
            f"Descartados por fecha: "
            f"{descartados_fecha}"
        )
        print(
            f"Registros finales validos: {total_final}"
        )
        from pyspark.sql.functions import sum as spark_sum
        total_recaudacion = df_final.agg(
             spark_sum("total_pago")).first()[0]
        print(f"RECAUDACION TOTAL: S/ {total_recaudacion:,.2f}")

        # ----------------------------------------------------
        # 9. CARGAR LOS DATOS EN FORMATO PARQUET
        # ----------------------------------------------------

        print("\n5. Guardando los datos en formato Parquet...")
        print(f"Destino: {path_target}")

        (
            df_final.write
            .mode("overwrite")
            .partitionBy("ANNIO_TRIBUTARIA")
            .parquet(path_target)
        )

        print("\n==============================================")
        print("ETL FINALIZADO CORRECTAMENTE")
        print(f"Archivo generado: {path_target}")
        print(f"Registros guardados: {total_final}")
        print("Formato: Parquet")
        print("Particion: ANNIO_TRIBUTARIA")
        print("==============================================")

        # ----------------------------------------------------
        # 10. MOSTRAR ESQUEMA Y MUESTRA DE RESULTADOS
        # ----------------------------------------------------

        print("\nEsquema de los datos procesados:")
        df_final.printSchema()

        print("\nMuestra de los datos procesados:")
        df_final.show(10, truncate=False)

        print(
            "\nSpark UI: http://localhost:4040"
            "\nPuedes revisar la interfaz mientras Spark siga activo."
        )

        input(
            "\nPresiona ENTER cuando termines de revisar "
            "los resultados y tomar tus capturas..."
        )

    except Exception as error:
        print("\nERROR DURANTE LA EJECUCION DEL ETL")
        print(f"Detalle: {error}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            print("\nMotor de Apache Spark detenido.")


# ============================================================
# 11. PUNTO DE ENTRADA DEL PROGRAMA
# ============================================================

if __name__ == "__main__":
    ejecutar_pipeline()
