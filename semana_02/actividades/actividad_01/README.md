# Actividad 01 — Semana 02: Fundamentos de PySpark con datos financieros

**Semana:** 02  
**Tema:** PySpark — DataFrames, transformaciones básicas y perfilamiento  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

En semana 1 leíste archivos, hiciste SQL básico y construiste un pipeline medallón. Esta semana vamos a profundizar en **PySpark** — la herramienta que usarás en el día a día para procesar datos a escala.

El dataset de esta semana es real: transacciones financieras de una institución bancaria de la década del 2010, creado por **Caixabank Tech** para un hackathon de detección de fraude. Tiene 5 archivos relacionados entre sí.

---

## Dataset

Descarga los archivos desde Google Drive:

**[data_engineering_files/semana_02_actividad_01](https://drive.google.com/drive/folders/1NPcvkwEyU5t9euXay3Uzxo02LqY_Ptb9)**

| Archivo | Formato | Descripción |
|---------|---------|-------------|
| `transactions_data.csv` | CSV | Transacciones bancarias — tabla principal |
| `users_data.csv` | CSV | Información demográfica de clientes |
| `cards_data.csv` | CSV | Datos de tarjetas de crédito/débito |
| `mcc_codes.json` | JSON | Códigos de categoría de comercio (MCC) |
| `train_fraud_labels.json` | JSON | Etiquetas de fraude por transacción (0/1) |

> Esta actividad trabaja principalmente con `transactions_data.csv`. Las demás tablas las usarás a partir de la Actividad 02.

---

## Material de estudio previo

Antes de comenzar, estudia los siguientes temas. Busca en Databricks Academy o en los recursos del bootcamp:

- ¿Qué es Apache Spark y por qué existe? (vs pandas)
- ¿Qué es un DataFrame de PySpark?
- ¿Cómo funciona la evaluación lazy en Spark?
- ¿Qué es un cluster y qué rol tiene el driver vs los workers?

Preguntas que debes poder responder antes de abrir el notebook:
- ¿Por qué pandas falla con archivos de varios GB?
- ¿Qué significa que Spark sea distribuido?
- ¿Qué es una transformación vs una acción en Spark?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana02-pyspark-fundamentos-<tu-nombre>
```

Crea tu carpeta de trabajo:
```
semana_02/actividades/actividad_01/<tu-nombre>/
```

---

## Estructura del notebook

Crea el notebook `pyspark_fundamentos_<tu-nombre>.ipynb` con las siguientes secciones:

---

### Celda 1 — Contexto y objetivo

```markdown
# PySpark Fundamentos — <Tu Nombre>
**Dataset:** Financial Transactions (Caixabank Tech)
**Fecha:** <fecha>
**Objetivo:** Cargar, explorar y transformar el dataset de transacciones usando PySpark puro.
```

---

### Celda 2 — Lectura del archivo y primeras inspecciones

```python
# En Databricks, spark ya está disponible
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/transactions_data.csv")

# ¿Cuántos registros?
print(f"Total de registros: {df.count():,}")

# ¿Cuántas columnas?
print(f"Columnas: {len(df.columns)}")

# Ver schema inferido
df.printSchema()

# Ver primeras filas
df.show(5, truncate=False)
```

> **Pista:** `inferSchema=True` hace que Spark lea una muestra del archivo para inferir los tipos. ¿Qué pasa si lo pones en `False`? Pruébalo y documenta la diferencia.

---

### Celda 3 — Exploración del schema y tipos de datos

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Ver tipos de cada columna
for field in df.schema.fields:
    print(f"{field.name}: {field.dataType}")
```

Responde en una celda markdown:
- ¿Qué columnas deberían ser numéricas pero Spark las leyó como string?
- ¿Hay columnas de fecha? ¿En qué formato llegaron?
- ¿Qué columnas parecen categóricas?

---

### Celda 4 — Selección y renombrado de columnas

```python
# Seleccionar solo columnas relevantes
df_sel = df.select("id", "date", "client_id", "card_id", "amount", "use_chip", "merchant_city", "merchant_state", "mcc")

# Renombrar columnas a snake_case consistente
df_sel = df_sel \
    .withColumnRenamed("id", "transaction_id") \
    .withColumnRenamed("date", "transaction_date") \
    .withColumnRenamed("use_chip", "transaction_type")

df_sel.show(5)
```

> **Investigar:** ¿Cuál es la diferencia entre `.select()` y `.drop()`? ¿Cuándo usarías cada uno?

---

### Celda 5 — Transformaciones de tipos

```python
# Convertir amount a numérico (puede venir como string con signos)
df_typed = df_sel \
    .withColumn("amount", F.regexp_replace(F.col("amount"), "[$,]", "").cast("double")) \
    .withColumn("transaction_date", F.to_timestamp(F.col("transaction_date"), "yyyy-MM-dd HH:mm:ss"))

df_typed.printSchema()
df_typed.select("amount", "transaction_date").show(5)
```

> **Pista:** El campo `amount` puede tener símbolo `$` o valores negativos (retiros). Documenta qué encontraste.

---

### Celda 6 — Filtros y condiciones

Practica el equivalente PySpark de `WHERE` en SQL:

```python
# Transacciones con monto mayor a $1,000
df_grandes = df_typed.filter(F.col("amount") > 1000)
print(f"Transacciones > $1,000: {df_grandes.count():,}")

# Transacciones con chip (no online)
df_chip = df_typed.filter(F.col("transaction_type") == "Swipe Transaction")
print(f"Transacciones con chip: {df_chip.count():,}")

# Transacciones negativas (posibles retiros o devoluciones)
df_negativos = df_typed.filter(F.col("amount") < 0)
print(f"Transacciones negativas: {df_negativos.count():,}")
```

Documenta en markdown: ¿qué encontraste en cada filtro? ¿Tiene sentido para el negocio?

---

### Celda 7 — Nuevas columnas calculadas

```python
# Extraer año y mes de la fecha
df_enriched = df_typed \
    .withColumn("year", F.year(F.col("transaction_date"))) \
    .withColumn("month", F.month(F.col("transaction_date"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("transaction_date"))) \
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)) \
    .withColumn("amount_abs", F.abs(F.col("amount")))

df_enriched.select("transaction_id", "transaction_date", "year", "month", "day_of_week", "is_weekend", "amount", "amount_abs").show(10)
```

> **Investigar:** ¿Qué valor retorna `dayofweek()` para lunes? ¿Y para domingo? (pista: depende del estándar — documenta lo que encontraste)

---

### Celda 8 — Agrupaciones y estadísticas

```python
# Transacciones por año
df_enriched.groupBy("year") \
    .agg(
        F.count("transaction_id").alias("total_transacciones"),
        F.sum("amount_abs").alias("volumen_total"),
        F.avg("amount_abs").alias("ticket_promedio"),
        F.max("amount_abs").alias("monto_maximo")
    ) \
    .orderBy("year") \
    .show()

# Top 10 ciudades con más transacciones
df_enriched.groupBy("merchant_city") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .limit(10) \
    .show()

# Distribución por tipo de transacción
df_enriched.groupBy("transaction_type") \
    .agg(F.count("*").alias("total"), F.avg("amount_abs").alias("ticket_promedio")) \
    .orderBy(F.col("total").desc()) \
    .show()
```

---

### Celda 9 — Valores nulos

```python
# Contar nulos por columna
from pyspark.sql.functions import col, sum as spark_sum, isnan, when, count

nulos = df_enriched.select([
    spark_sum(when(col(c).isNull() | isnan(c), 1).otherwise(0)).alias(c)
    for c in df_enriched.columns
])
nulos.show(vertical=True)
```

Documenta: ¿qué columnas tienen nulos? ¿Qué impacto tiene eso en el análisis?

---

### Celda 10 — Reflexión y comparación con pandas

```markdown
## Reflexión final

### PySpark vs pandas
- ¿Qué fue más difícil de entender al pasar de pandas a PySpark?
- ¿Qué ventajas notaste trabajando con Spark en Databricks?
- ¿Cuánto tardó en ejecutarse cada celda? ¿Fue más rápido o más lento de lo esperado?

### Sobre el dataset
- ¿Qué hallazgo del dataset te pareció más interesante?
- ¿Qué preguntas de negocio se te ocurren con solo esta tabla?

### Evaluación lazy
- ¿Notaste la diferencia entre `.show()` y solo encadenar transformaciones sin acción?
- ¿Cuándo se ejecuta realmente el código en Spark?
```

---

## Entrega en Git

```bash
git add semana_02/actividades/actividad_01/<tu-nombre>/
git commit -m "feat: add pyspark fundamentals notebook - <tu-nombre>"
git push origin feature/semana02-pyspark-fundamentos-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 02] PySpark Fundamentos — <Tu Nombre>
```

En la descripción incluye:
- ¿Cuántos registros tiene el dataset?
- ¿Qué columna tuvo más nulos?
- Un hallazgo que te sorprendió

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Notebook ejecutable de inicio a fin | Sin errores en ninguna celda | 20% |
| Transformaciones de tipos correctas | `amount` numérico, fecha como timestamp | 20% |
| Columnas calculadas presentes | year, month, day_of_week, is_weekend, amount_abs | 20% |
| Análisis de nulos documentado | Con interpretación de negocio | 15% |
| Reflexión PySpark vs pandas | Respuestas concretas con observaciones reales | 15% |
| Git: commit descriptivo y PR completo | | 10% |

---

## Lo que NO se debe hacer

- No usar pandas para leer el archivo — todo en PySpark
- No omitir las celdas de reflexión
- No hacer un solo commit con todo
- No hardcodear rutas absolutas que solo funcionen en tu entorno — usa variables

---

## Referencias

- [GITFLOW.md](../../GITFLOW.md)
- [PySpark SQL Functions — documentación oficial](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Databricks Academy](https://customer-academy.databricks.com)
