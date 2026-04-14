# Actividad 03 — Semana 06: Structured Streaming Real

**Semana:** 06  
**Tema:** Ingesta continua con Structured Streaming y Auto Loader  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise (Volumes requeridos)

---

## Objetivo

Construir un pipeline de streaming real con Spark Structured Streaming:
leer datos en modo continuo desde un Volumen de ADLS con Auto Loader,
manejar late data con watermarking, garantizar resiliencia con checkpointing,
y escribir a Delta en modo streaming. La diferencia con DLT (semana 05) es que
aquí controlas cada parámetro del streaming manualmente — sin la abstracción declarativa.

---

## Dataset

Los archivos CSV del dataset Financial Transactions, disponibles en tu Volumen:
`/Volumes/main/landing/raw/`

La simulación de streaming consiste en copiar archivos al Volumen de uno en uno,
como si llegaran de un sistema externo en tiempo real.

---

## Material de estudio previo

1. ¿Qué diferencia hay entre **micro-batch** y **continuous processing** en Spark Structured Streaming?
2. ¿Qué es un **trigger** en Structured Streaming y qué opciones existen? ¿Cuándo usarías `availableNow` vs `processingTime`?
3. ¿Qué es el **watermark** y qué problema resuelve? Pon un ejemplo con datos de transacciones.
4. ¿Qué información guarda el **checkpoint** y por qué es necesario para recuperarse de un fallo?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana06-streaming-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_06/actividades/actividad_03/<tu-nombre>/
```

---

## Parte 1 — Auto Loader en modo streaming

Auto Loader en semana 05 lo usaste dentro de DLT. Aquí lo usas directamente
con la API de Structured Streaming.

```python
# Leer continuamente nuevos archivos del Volumen
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/main/checkpoints/schema/transactions")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(transactions_schema)  # usa el schema explícito de semana 02
    .load("/Volumes/main/landing/raw/transactions/")
)

# Verificar que es un stream
print(df_stream.isStreaming)  # debe ser True
```

Explora el schema del stream con `df_stream.printSchema()` y documenta
las columnas extra que añade Auto Loader (`_metadata`).

Commit esperado:
```bash
git commit -m "feat: configure Auto Loader in streaming mode"
```

---

## Parte 2 — Trigger modes: cuándo procesar

```python
# Trigger availableNow: procesa todo lo disponible y para (útil para testing)
query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/main/checkpoints/bronze_streaming")
    .trigger(availableNow=True)
    .toTable("bronze.transactions_streaming")
)
query.awaitTermination()

# Trigger processingTime: procesa cada N segundos (streaming continuo)
query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/main/checkpoints/bronze_streaming")
    .trigger(processingTime="30 seconds")
    .toTable("bronze.transactions_streaming")
)
```

Ejecuta ambas variantes y documenta:
- ¿Cuándo termina cada una?
- ¿Cuál usarías en producción para un pipeline que recibe datos cada hora?
- ¿Cuál usarías para backfill?

Commit esperado:
```bash
git commit -m "feat: test availableNow and processingTime triggers"
```

---

## Parte 3 — Watermarking para late data

El watermark define cuánto tiempo esperamos datos que lleguen tarde.
Sin watermark, el stream acumula estado indefinidamente.

```python
from pyspark.sql import functions as F

# Añadir watermark de 2 horas: ignorar eventos con más de 2h de retraso
df_con_watermark = (
    df_stream
    .withColumn("event_time", F.to_timestamp("transaction_date"))
    .withWatermark("event_time", "2 hours")
)

# Agregación sobre ventana de tiempo (tumbling window de 1 hora)
df_agregado = (
    df_con_watermark
    .groupBy(
        F.window("event_time", "1 hour"),
        "category"
    )
    .agg(
        F.count("transaction_id").alias("num_transactions"),
        F.sum("amount").alias("total_amount")
    )
)

# Escribir resultado en modo update (no append — hay agregaciones)
query = (
    df_agregado.writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", "/Volumes/main/checkpoints/gold_streaming")
    .trigger(processingTime="60 seconds")
    .toTable("gold.transactions_por_hora")
)
```

Documenta:
- ¿Qué pasa si un registro llega con 3 horas de retraso? ¿Aparece en el resultado?
- ¿Qué outputMode usarías si quisieras ver solo los registros nuevos? ¿Y si quieres la tabla completa actualizada?

Commit esperado:
```bash
git commit -m "feat: add watermarking and tumbling window aggregation"
```

---

## Parte 4 — Checkpointing y tolerancia a fallos

El checkpoint es lo que hace el streaming resiliente: guarda el estado del stream
para que pueda reanudarse exactamente donde lo dejó si falla.

```python
# Arrancar el stream
query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/main/checkpoints/bronze_streaming")
    .trigger(processingTime="30 seconds")
    .toTable("bronze.transactions_streaming")
)

# Simular un fallo: parar el stream a mano
query.stop()

# Contar cuántos registros hay antes de reiniciar
spark.table("bronze.transactions_streaming").count()

# Reiniciar el stream con el MISMO checkpointLocation
query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/main/checkpoints/bronze_streaming")  # mismo path
    .trigger(availableNow=True)
    .toTable("bronze.transactions_streaming")
)
query.awaitTermination()

# Verificar: no hay duplicados — el stream sabe qué ya procesó
spark.table("bronze.transactions_streaming").count()
```

Documenta qué pasaría si cambiaras el `checkpointLocation` en el reinicio.

Commit esperado:
```bash
git commit -m "docs: document checkpoint behavior and fault tolerance test"
```

---

## Entrega en Git

```bash
git push origin feature/semana06-streaming-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 06] Structured Streaming — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Auto Loader en modo streaming configurado | Schema explícito, `_metadata` documentado | 20% |
| [Requerido] Ambos trigger modes probados y comparados | `availableNow` y `processingTime` con conclusión | 20% |
| [Requerido] Checkpoint: parada y reinicio sin duplicados | Conteo antes/después documentado | 25% |
| [Recomendado] Watermark + tumbling window funcionando | Gold table con agregaciones por hora | 25% |
| [Opcional] Análisis de outputModes | Comparativa append/update/complete con casos de uso | 10% |
| **Total** | | **100%** |

---

## Referencias

- [Structured Streaming Programming Guide (Spark docs)](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Auto Loader (Databricks docs)](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Watermarking in Structured Streaming](https://www.databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html)
