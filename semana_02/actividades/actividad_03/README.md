# Actividad 03 — Semana 02: Funciones Avanzadas en PySpark

**Semana:** 02  
**Tema:** Funciones de fecha, window functions y agregaciones avanzadas  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

Ya sabes leer datos y conectar tablas. Ahora el reto es sacar más valor de ellos.

En análisis financiero, preguntas como:
- "¿El cliente lleva 3 transacciones seguidas en el mismo día?" → window function
- "¿Cuánto gastó este usuario en los últimos 7 días respecto al día anterior?" → LAG
- "¿A qué hora del día ocurre más fraude?" → función de fecha
- "¿Cuál es el puesto de este comercio por volumen de ventas dentro de su categoría?" → RANK con PARTITION BY

...no se responden con un simple `groupBy`. Necesitas funciones más precisas.

---

## Dataset

Los mismos archivos de semana 02:

**[data_engineering_files/semana_02_actividad_01](https://drive.google.com/drive/folders/1NPcvkwEyU5t9euXay3Uzxo02LqY_Ptb9)**

Punto de partida recomendado: el `df_final` construido en la Actividad 02 (5 tablas unidas).

---

## Material de estudio previo

- ¿Qué es una window function y en qué se diferencia de `groupBy`?
- ¿Qué hace `PARTITION BY` en el contexto de una ventana?
- ¿Qué diferencia hay entre `ROW_NUMBER`, `RANK` y `DENSE_RANK`?
- ¿Qué hace `LAG` y `LEAD`?
- ¿Qué es `date_trunc` y cuándo es útil?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana02-funciones-avanzadas-<tu-nombre>
```

Crea tu carpeta:
```
semana_02/actividades/actividad_03/<tu-nombre>/
```

---

## Punto de partida — cargar df_final desde Delta

Esta actividad continúa desde el `df_final` que guardaste en formato Delta al finalizar la Actividad 02. Ejecuta esta celda antes de cualquier otra:

```python
from pyspark.sql import functions as F

# Leer df_final desde la tabla Delta registrada en Unity Catalog (guardada en la Actividad 02)
df_final = spark.table("workspace.default.financial_final")

print(f"Filas: {df_final.count():,} | Columnas: {len(df_final.columns)}")
df_final.printSchema()
```

> **Si no tienes la tabla** (no ejecutaste el último bloque de la Actividad 02), completa esa actividad primero. No repitas los JOINs aquí — `saveAsTable` existe precisamente para evitar recomputar en cada sesión.

---

## Parte 1 — Funciones de fecha y tiempo

### 1.1 Extraer componentes de timestamp

```python
from pyspark.sql import functions as F

# df_final ya tiene transaction_date como timestamp (viene de la Actividad 02)
# Solo extraemos los componentes temporales encima del DataFrame unido
df = df_final \
    .withColumn("hora", F.hour("transaction_date")) \
    .withColumn("dia_semana", F.dayofweek("transaction_date")) \
    .withColumn("mes", F.month("transaction_date")) \
    .withColumn("anio", F.year("transaction_date")) \
    .withColumn("semana_anio", F.weekofyear("transaction_date")) \
    .withColumn("dia_mes", F.dayofmonth("transaction_date"))

df.select("transaction_date", "hora", "dia_semana", "mes", "semana_anio").show(5)
```

> `dayofweek` en Spark: 1 = domingo, 7 = sábado.

---

### 1.2 Truncar fechas con `date_trunc`

```python
# Agrupar transacciones por semana y mes
df = df \
    .withColumn("semana_inicio", F.date_trunc("week", "transaction_date")) \
    .withColumn("mes_inicio", F.date_trunc("month", "transaction_date"))

# Volumen semanal
df.groupBy("semana_inicio") \
    .agg(
        F.count("*").alias("num_transacciones"),
        F.sum("amount").alias("monto_total")
    ) \
    .orderBy("semana_inicio") \
    .show(10)
```

---

### 1.3 Calcular diferencia entre fechas

```python
# ¿Cuántos días han pasado desde la primera transacción del dataset?
fecha_min = df.agg(F.min("transaction_date")).collect()[0][0]

df = df.withColumn(
    "dias_desde_inicio",
    F.datediff(F.col("transaction_date"), F.lit(fecha_min))
)

# ¿Cuántos días entre la transacción y el final del mes?
df = df.withColumn(
    "dias_para_fin_de_mes",
    F.datediff(
        F.last_day(F.col("transaction_date")),
        F.col("transaction_date")
    )
)
```

Commit esperado:
```bash
git commit -m "feat: add date extraction and date_trunc transformations"
```

---

## Parte 2 — Agregaciones avanzadas con `groupBy`

```python
# Gasto por usuario y mes — múltiples métricas
df_mensual_usuario = df.groupBy("user_id", "mes_inicio") \
    .agg(
        F.count("*").alias("num_transacciones"),
        F.sum("amount").alias("gasto_mensual"),
        F.avg("amount").alias("ticket_promedio"),
        F.max("amount").alias("compra_maxima"),
        F.countDistinct("description").alias("categorias_distintas"),
        F.percentile_approx("amount", 0.5).alias("mediana_gasto")
    ) \
    .orderBy("mes_inicio", F.col("gasto_mensual").desc())

df_mensual_usuario.show(10)
```

### Aggregaciones con `pivot`

```python
# Gasto total por tipo de tarjeta por mes (si tienes la columna card_type)
df_pivot = df.groupBy("mes_inicio") \
    .pivot("card_type") \
    .agg(F.round(F.sum("amount"), 2))

df_pivot.show()
```

Responde: ¿Cuándo tiene sentido usar pivot y cuándo es un antipatrón?

---

## Parte 3 — Window Functions

Primero, el concepto clave:

```python
from pyspark.sql import Window

# Una ventana = un grupo de filas sobre las cuales calcular
# sin reducir el número de filas (a diferencia de groupBy)
```

---

### 3.1 Ranking por categoría

```python
# ¿Qué comercio tiene más volumen de ventas dentro de su categoría MCC?
# "description" es la columna de categoría que viene del JOIN con mcc_codes (Actividad 02)
windowSpec = Window.partitionBy("description").orderBy(F.col("gasto_total").desc())

df_por_comercio = df.groupBy("description") \
    .agg(F.sum("amount").alias("gasto_total"))

df_ranking = df_por_comercio.withColumn("rank_en_categoria", F.rank().over(windowSpec))

# Top 3 comercios por categoría
df_ranking.filter(F.col("rank_en_categoria") <= 3) \
    .orderBy("description", "rank_en_categoria") \
    .show(20, truncate=False)
```

Documenta la diferencia entre `rank()`, `dense_rank()` y `row_number()` con un ejemplo propio.

---

### 3.2 LAG y LEAD — comparar con la fila anterior

```python
# Dentro de cada usuario, ¿cuánto gastó en la transacción anterior?
windowSpec_usuario = Window.partitionBy("user_id").orderBy("transaction_date")

df = df.withColumn("gasto_anterior", F.lag("amount", 1).over(windowSpec_usuario)) \
       .withColumn("gasto_siguiente", F.lead("amount", 1).over(windowSpec_usuario)) \
       .withColumn(
           "variacion_vs_anterior",
           F.round(F.col("amount") - F.col("gasto_anterior"), 2)
       )

# Transacciones donde el monto casi duplica el anterior (posible anomalía)
df.filter(F.col("amount") > F.col("gasto_anterior") * 2) \
  .select("user_id", "transaction_date", "amount", "gasto_anterior", "variacion_vs_anterior") \
  .orderBy(F.col("variacion_vs_anterior").desc()) \
  .show(10)
```

---

### 3.3 Acumulados (running totals)

```python
# Gasto acumulado por usuario ordenado por fecha
windowSpec_acum = Window.partitionBy("user_id") \
    .orderBy("transaction_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn("gasto_acumulado", F.sum("amount").over(windowSpec_acum))

df.select("user_id", "transaction_date", "amount", "gasto_acumulado") \
  .filter(F.col("user_id") == df.limit(1).select("user_id").collect()[0][0]) \
  .show(10)
```

---

### 3.4 Media móvil (rolling average)

```python
# Media móvil de los últimos 3 transacciones por usuario
windowSpec_rolling = Window.partitionBy("user_id") \
    .orderBy("transaction_date") \
    .rowsBetween(-2, 0)  # fila actual y las 2 anteriores

df = df.withColumn("media_movil_3", F.round(F.avg("amount").over(windowSpec_rolling), 2))
```

Commit esperado:
```bash
git commit -m "feat: add window functions - rank, lag, running totals, rolling avg"
```

---

## Parte 4 — Análisis final con todo combinado

Con las transformaciones anteriores, responde estas preguntas. Cada respuesta debe ser un bloque de código + una celda markdown con tu conclusión:

1. **Hora pico del fraude:** ¿En qué hora del día ocurren más transacciones fraudulentas? ¿Y menos?

2. **Efecto fin de semana:** ¿El monto promedio de transacción es mayor en fines de semana o días hábiles?

3. **Usuarios que "explotan" en gasto:** Identifica usuarios cuya última transacción del mes duplica su mediana histórica de gasto.

4. **Comercio top por mes:** Para cada mes, ¿cuál fue el comercio con mayor volumen de transacciones? Usa window functions para responderlo sin perder detalle de filas.

5. **Patrón temporal de fraude:** ¿El número de transacciones fraudulentas aumenta o disminuye a lo largo del año?

---

## Entrega en Git

```bash
git add semana_02/actividades/actividad_03/<tu-nombre>/
git commit -m "feat: advanced functions analysis - date, window, aggregations - <tu-nombre>"
git push origin feature/semana02-funciones-avanzadas-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 02] Funciones Avanzadas — <Tu Nombre>
```

Describe en el PR:
- ¿Cuál fue la window function más difícil de entender? ¿Por qué?
- ¿Qué hallazgo del análisis final te sorprendió más?
- ¿En qué se diferencia una window function de un `groupBy` + `join`?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Funciones de fecha implementadas | hour, dayofweek, date_trunc, datediff | 20% |
| Agregaciones avanzadas correctas | percentile, countDistinct, pivot | 15% |
| Window functions: rank / lag / acumulado | Las 4 variantes implementadas | 35% |
| Análisis final: 5 preguntas respondidas | Código + conclusión en markdown | 25% |
| Commits descriptivos | Mínimo 3 commits | 5% |

---

## Laboratorio

Completa el notebook de laboratorio con un dataset temporal de tu elección:

**[`semana_02/laboratorios/lab_03_funciones_avanzadas.ipynb`](../../laboratorios/lab_03_funciones_avanzadas.ipynb)**

El notebook te guía por 9 partes: descripción del dataset, perfil técnico, preparación de la columna temporal, agregaciones por período, ranking con window functions, LAG y detección de variaciones, acumulado o media móvil, análisis de negocio temporal y reflexión final.

---

## Referencias(https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [PySpark Date Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/datetime.html)
- [Actividad 02 semana 02](../actividad_02/README.md) — df_final como punto de partida
