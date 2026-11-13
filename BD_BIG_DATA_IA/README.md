# ETL de Datos de Recaudación Municipal

## Descripción del proyecto

Este proyecto implementa un proceso ETL (Extract, Transform, Load) utilizando Python y Apache Spark mediante PySpark, con el objetivo de procesar y transformar datos de recaudación tributaria municipal correspondientes al año 2022.

El proceso permite mejorar la calidad de los datos, validar registros, estandarizar formatos y generar un conjunto de datos estructurado en formato Parquet, preparado para su posterior análisis.

## Objetivos

- Extraer datos desde un archivo CSV.
- Limpiar y estandarizar los nombres y valores de las columnas.
- Validar los importes y las fechas de pago.
- Normalizar las descripciones tributarias.
- Aplicar SHA-256 al código del contribuyente para reducir la exposición del identificador original.
- Generar un archivo Parquet particionado por año tributario.

## Tecnologías utilizadas

- Python
- Apache Spark
- PySpark
- Pandas
- Formato CSV
- Formato Parquet
- Git y GitHub

## Estructura del proyecto

```text
BD_BIG_DATA_IA/
├── data/
│   ├── raw/
│   │   └── [archivo CSV de entrada]
│   └── processed/
│       └── [resultados en formato Parquet]
├── pipeline_ETL.py
├── requirements.txt
├── README.md
├── .gitignore
└── docs/
    └── [documentación y evidencias]
```

Los archivos de datos pueden mantenerse fuera del repositorio cuando no exista autorización para compartirlos públicamente.

## Requisitos previos

Antes de ejecutar el proyecto, se necesita:

- Python compatible con la versión de PySpark utilizada.
- Java compatible con la instalación de Spark.
- PySpark 4.2.0.
- Un entorno de ejecución configurado para Spark.
- En Windows, puede ser necesario configurar las herramientas nativas de Hadoop, incluido `winutils.exe`, para las operaciones de escritura en el sistema de archivos local.

## Instalación

### 1. Clonar el repositorio

```bash
git clone URL_DE_TU_REPOSITORIO
cd BD_BIG_DATA_IA
```

Reemplaza `URL_DE_TU_REPOSITORIO` por la dirección real de tu repositorio de GitHub.

### 2. Crear un entorno virtual

En Windows:

```powershell
py -m venv .venv
```

Activarlo:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Instalar las dependencias

```powershell
py -m pip install -r requirements.txt
```

### 4. Preparar los datos

Coloca el archivo CSV autorizado para el procesamiento dentro de `data/raw/`, respetando el nombre