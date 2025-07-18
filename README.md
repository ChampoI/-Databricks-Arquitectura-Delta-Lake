# 🧊 Arquitectura Delta Lake con Databricks

Este repositorio contiene ejemplos prácticos de procesamiento de datos con Apache Spark y PySpark sobre **Databricks**, implementando una arquitectura en capas: **Bronze**, **Silver** y **Gold**, y mostrando lectura, transformación, uso de funciones definidas por el usuario (UDFs), y consultas SQL sobre datos estructurados y semi-estructurados.

---

## 📁 Estructura del Proyecto

```
📦 -Databricks-Arquitectura-Delta-Lake/
 ┣ 📜 Ejemplo practico de UDF.ipynb
 ┣ 📜 Implementacion de un DeltaLake sobre Databricks.ipynb
 ┣ 📜 Instalar una librería en Databricks.ipynb
 ┣ 📜 Laboratorio - PySpark SQL - Parte 1.ipynb
 ┣ 📜 Laboratorio - PySpark SQL - Parte 2.ipynb
 ┣ 📜 Lectura de Datos.ipynb
```

---

## 📘 Descripción de los Notebooks

### 1. `Lectura de Datos.ipynb`

Se realiza lectura y análisis de:

- **Archivos CSV** (`2015_summary.csv`)
- **Archivos delimitados** (`persona.data`)
- **Archivos JSON** (`transacciones.json`)

#### Ejemplo de lectura CSV:

```python
df = spark.read.csv("dbfs:/FileStore/curso_databricks/2015_summary.csv", header=True, inferSchema=True)
df.show(5)
```

| DEST_COUNTRY_NAME | ORIGIN_COUNTRY_NAME | count |
|-------------------|---------------------|-------|
| United States     | Romania             | 15    |
| United States     | Croatia             | 1     |

---

### 2. `Instalar una librería en Databricks.ipynb`

Se ejemplifica cómo instalar librerías adicionales (como `spark-xml`) y se realiza lectura de datos XML:

```python
transacciones_xml = spark.read.format("xml") \
  .option("rootTag", "root") \
  .option("rowTag", "element") \
  .load("dbfs:/FileStore/curso_databricks/transacciones.xml")
transacciones_xml.show()
```

| EMPRESA     | PERSONA                          | TRANSACCION     |
|-------------|----------------------------------|-----------------|
| {5, Amazon} | {[59, 9811935], ...}             | {2021-01-23, 2628.0} |

---

### 3. `Implementación de un DeltaLake sobre Databricks.ipynb`

Simula la creación de una arquitectura **Delta Lake** por capas:

```python
%fs mkdirs dbfs:///FileStore/curso_databricks/deltalake/bronze/raw_data
%fs mkdirs dbfs:///FileStore/curso_databricks/deltalake/silver/cleaning_data
%fs mkdirs dbfs:///FileStore/curso_databricks/deltalake/gold/final_data
```

📌 Se trabaja sobre carpetas `bronze`, `silver` y `gold` para reflejar una arquitectura robusta para ETL de datos.

---

### 4. `Laboratorio - PySpark SQL - Parte 1.ipynb`

Demuestra:

- Creación de DataFrames
- Consultas SQL sobre vistas temporales
- Funciones SQL como `GROUP BY`, `ORDER BY`, `WHERE`

```python
df.createOrReplaceTempView("ventas")
spark.sql("SELECT producto, SUM(monto) FROM ventas GROUP BY producto").show()
```

---

### 5. `Laboratorio - PySpark SQL - Parte 2.ipynb`

Complementa el laboratorio anterior con:

- Funciones agregadas personalizadas
- Operaciones con fechas
- Manipulación de columnas complejas

---

### 6. `Ejemplo práctico de UDF.ipynb`

Crea y aplica funciones definidas por el usuario (`User Defined Functions`) en Spark para extender el procesamiento de datos:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clasificar_salario(salario):
    return "Alto" if salario > 20000 else "Bajo"

df = df.withColumn("rango_salario", clasificar_salario(df["salario"]))
df.select("nombre", "salario", "rango_salario").show()
```

---

## 💡 Aprendizajes clave

- Lectura de formatos complejos como JSON y XML en Spark.
- Creación y uso de estructuras Delta Lake (bronze → silver → gold).
- Uso de funciones SQL y UDFs para procesamiento avanzado.
- Modularización del proceso de ETL para entornos productivos.

---

## ✅ Requisitos

- Apache Spark 3.0+
- Databricks Runtime (Community o Pro)
- Archivos en `dbfs:/FileStore/curso_databricks/`

---

## 🧠 Recursos adicionales

- [Documentación de Delta Lake](https://docs.delta.io/)
- [API de PySpark](https://spark.apache.org/docs/latest/api/python/)

---
