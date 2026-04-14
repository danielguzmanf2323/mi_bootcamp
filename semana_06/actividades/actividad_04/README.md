# Actividad 04 — Semana 06: Performance Tuning en Spark

**Semana:** 06  
**Tema:** Optimización de rendimiento — Spark UI, joins, skew y small files  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise

---

## Objetivo

Aprender a diagnosticar y resolver los problemas de rendimiento más comunes
en pipelines Spark: entender el Spark UI, identificar joins lentos,
detectar skew en los datos y gestionar el problema de los small files.
El objetivo no es memorizar parámetros — es saber dónde mirar cuando algo va lento.

---

## Dataset

Las tablas Delta de semanas anteriores. Para reproducir problemas de rendimiento
reales usarás `silver.transactions` (la tabla más grande que tienes disponible).

---

## Material de estudio previo

1. ¿Qué es un **stage** en Spark y qué lo delimita? ¿Por qué el shuffle crea una barrera entre stages?
2. ¿Cuándo Spark usa un **broadcast join** automáticamente? ¿Cuál es el parámetro que controla ese umbral?
3. ¿Qué es el **data skew** en Spark? ¿Por qué una sola partición lenta puede ralentizar todo el job?
4. ¿Qué es el **small files problem** en Delta Lake y por qué ocurre naturalmente en pipelines de streaming?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana06-performance-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_06/actividades/actividad_04/<tu-nombre>/
```

El entregable incluye un notebook con código y capturas del Spark UI comentadas.

---

## Parte 1 — Leer el Spark UI: anatomía de un job lento

Ejecuta esta query deliberadamente costosa y analiza el resultado en el Spark UI:

```python
# Query con shuffle costoso — muchos groupBy encadenados
df_result = (
    spark.table("silver.transactions")
    .groupBy("category", "customer_id")
    .agg({"amount": "sum"})
    .groupBy("category")
    .agg({"sum(amount)": "avg"})
    .orderBy("avg(sum(amount))", ascending=False)
)

df_result.show()
```

En el Spark UI (pestaña **Jobs → Stages**), localiza y documenta:
- ¿Cuántos stages generó esta query? ¿Por qué?
- ¿Cuánto **shuffle read/write** hubo en cada stage?
- ¿Cuál fue el stage más lento?
- ¿Hay tareas con tiempo muy diferente al resto dentro de un mismo stage?

Incluye un screenshot del DAG visualizer con anotaciones en tu documento.

Commit esperado:
```bash
git commit -m "docs: analyze Spark UI for expensive query - DAG and stages"
```

---

## Parte 2 — Broadcast joins

Un join entre una tabla grande y una tabla pequeña puede ser muy lento si Spark
hace un sort-merge join en lugar de un broadcast join.

```python
# Join sin broadcast (Spark decide según el umbral automático)
df_no_broadcast = (
    spark.table("silver.transactions")   # tabla grande
    .join(
        spark.table("silver.customers"), # tabla más pequeña
        on="customer_id",
        how="inner"
    )
)
df_no_broadcast.explain()  # ver el plan: busca "SortMergeJoin"
df_no_broadcast.count()    # ejecutar y medir tiempo

# Join con broadcast explícito
from pyspark.sql import functions as F

df_broadcast = (
    spark.table("silver.transactions")
    .join(
        F.broadcast(spark.table("silver.customers")),
        on="customer_id",
        how="inner"
    )
)
df_broadcast.explain()  # ver el plan: busca "BroadcastHashJoin"
df_broadcast.count()    # ejecutar y medir tiempo
```

Documenta:
- ¿Cuánto tiempo tardó cada join?
- ¿Cuántos stages generó cada uno? ¿Por qué el broadcast tiene menos?
- ¿Cuándo NO deberías usar broadcast aunque la tabla sea pequeña?

Commit esperado:
```bash
git commit -m "feat: benchmark sort-merge vs broadcast join with Spark UI evidence"
```

---

## Parte 3 — repartition vs coalesce

```python
# Ver particiones actuales
df = spark.table("silver.transactions")
print(f"Particiones actuales: {df.rdd.getNumPartitions()}")

# repartition: redistribuye datos con shuffle completo (puede aumentar o reducir)
df_repartitioned = df.repartition(8)
print(f"Tras repartition: {df_repartitioned.rdd.getNumPartitions()}")

# coalesce: solo reduce particiones, sin shuffle (más eficiente para reducir)
df_coalesced = df.coalesce(4)
print(f"Tras coalesce: {df_coalesced.rdd.getNumPartitions()}")

# Medir diferencia de tiempo al escribir
import time

t0 = time.time()
df_repartitioned.write.format("delta").mode("overwrite").save("/tmp/test_repartition")
print(f"repartition write: {time.time()-t0:.2f}s")

t0 = time.time()
df_coalesced.write.format("delta").mode("overwrite").save("/tmp/test_coalesce")
print(f"coalesce write: {time.time()-t0:.2f}s")
```

Documenta cuándo elegirías cada uno. Regla general a validar:
> *"Usa `coalesce` para reducir particiones antes de escribir. Usa `repartition` para redistribuir datos de forma equilibrada antes de un join costoso."*

¿Esta regla es siempre correcta? ¿Cuándo falla?

Commit esperado:
```bash
git commit -m "docs: repartition vs coalesce benchmark and decision criteria"
```

---

## Parte 4 — Data Skew: identificar y mitigar

El skew ocurre cuando una o pocas particiones tienen muchos más datos que las demás.
En el dataset financiero, algunas categorías tienen muchas más transacciones que otras.

```python
# Identificar distribución de datos por categoría
spark.table("silver.transactions") \
    .groupBy("category") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(20)

# Simular skew: filtrar solo la categoría más frecuente y hacer un join
categoria_skewed = "shopping"  # reemplaza por la más frecuente en tus datos

df_skewed = spark.table("silver.transactions").filter(
    F.col("category") == categoria_skewed
)

# Join con skew visible en Spark UI
df_join_skewed = df_skewed.join(
    spark.table("silver.customers"),
    on="customer_id"
)
df_join_skewed.count()
```

En el Spark UI, busca tareas que tarden 5-10x más que las demás en el mismo stage.
Eso es skew.

Mitigation con salting:
```python
import random

# Añadir una columna "salt" para distribuir los datos de la partición skewed
num_salts = 4

df_salted = df_skewed.withColumn(
    "salt", (F.rand() * num_salts).cast("int")
)

df_customers_salted = spark.table("silver.customers").crossJoin(
    spark.range(num_salts).toDF("salt")
)

df_join_salted = df_salted.join(
    df_customers_salted,
    on=["customer_id", "salt"]
).drop("salt")

df_join_salted.count()
```

Documenta: ¿el tiempo mejoró? ¿Las tareas están más equilibradas en el Spark UI?

Commit esperado:
```bash
git commit -m "docs: skew identification and salting mitigation with Spark UI evidence"
```

---

## Entrega en Git

```bash
git push origin feature/semana06-performance-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 06] Performance Tuning — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Spark UI analizado con screenshot anotado | DAG, stages, shuffle documentados | 25% |
| [Requerido] Broadcast vs sort-merge comparado | Tiempos medidos, plan de ejecución leído | 25% |
| [Requerido] repartition vs coalesce con criterio documentado | Benchmark + regla de decisión propia | 20% |
| [Recomendado] Skew identificado en Spark UI | Screenshot con tarea lenta señalada | 20% |
| [Opcional] Salting implementado y comparado | Mejora de tiempo documentada con evidencia | 10% |
| **Total** | | **100%** |

---

## Referencias

- [Spark UI explained (Databricks blog)](https://www.databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html)
- [Optimizing Apache Spark (Databricks docs)](https://docs.databricks.com/optimizations/index.html)
- [Handling Data Skew in Apache Spark](https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
