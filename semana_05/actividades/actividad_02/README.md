# Actividad 02 — Semana 05: Auto Loader y Quality Expectations

**Semana:** 05  
**Tema:** Ingesta incremental automática + calidad de datos declarativa en Lakeflow Spark Declarative Pipelines  
**Nivel:** Intermedio–Avanzado  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
**Prerequisito:** Actividad 01 completada — primer pipeline declarativo funcionando

---

## Contexto

En la Actividad 01 leíste el archivo completo con `spark.read` dentro de un DLT pipeline. Eso funciona, pero en producción los archivos llegan de a poco — cada hora, cada día. No quieres releer el archivo completo cada vez.

**Auto Loader** (`cloudFiles`) resuelve eso: detecta automáticamente los archivos nuevos que llegan a un directorio y los procesa de forma incremental. No necesitas saber qué archivos son nuevos — Databricks los rastrea.

El segundo problema: ¿qué pasa cuando el archivo tiene una fila con `amount = null`, o un `user_id` que no existe? En semana 02–03 usabas filtros `WHERE is_null(amount)` mezclados con la lógica de negocio. **Quality Expectations** en los pipelines declarativos separan la calidad del código: declaras las reglas, el pipeline las aplica y registra cuántas filas fallaron — sin mezclar nada.

---

## Material de estudio previo

- ¿Qué es Auto Loader en Databricks? ¿Qué archivo de checkpointing usa para saber qué procesó?
- ¿Cuáles son los tres comportamientos de `@dp.expect`? (`warn`, `drop`, `fail`)
- ¿Qué es una tabla de cuarentena y cuándo se usa?
- ¿Qué columna interna agrega el pipeline para marcar filas que fallaron expectativas? (`_rescued_data`)

---

## Instrucciones Git

```bash
git checkout feature/semana05-dlt-<tu-nombre>
```

Crea tus notebooks en:
```
semana_05/actividades/actividad_02/<tu-nombre>/
```

---

## Parte 1 — Auto Loader: ingesta incremental con cloudFiles

Auto Loader usa el format `cloudFiles` y **Spark Structured Streaming**. Dentro de DLT, lo usas con `spark.readStream` (no `spark.read`).

Crea un notebook declarativo `bronze_autoloader_<tu-nombre>.py`:

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit, input_file_name

volumes_base = spark.conf.get(
    "pipelines.parameter.volumes_path",
    "/Volumes/main/landing/raw"
)
```

```python
@dp.table(
    name="bronze_transactions_stream",
    comment="Transacciones — ingesta incremental con Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_transactions_stream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")       # formato del archivo fuente
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        # Auto Loader rastrea el progreso en este path (nunca reprocesa lo ya visto)
        .option("cloudFiles.schemaLocation",
                "/pipelines/checkpoints/bronze_transactions_stream/schema")
        .load(f"{volumes_base}/transactions/")    # carpeta (no archivo único)
        .withColumn("_ingested_at",   current_timestamp())
        .withColumn("_source_file",   input_file_name())
    )
```

> **Diferencia clave:** antes leías `transactions_data.csv` (un archivo exacto). Ahora leés de la carpeta `transactions/` — cualquier archivo CSV que aparezca ahí será procesado automáticamente la próxima vez que el pipeline corra. Tú subes el archivo. DLT lo detecta.

Simular la llegada de un nuevo batch de archivos:

```python
# Este código va en un notebook SEPARADO (no DLT) para simular la llegada de datos
# Copiar un subconjunto del CSV a la carpeta "landing" de transactions

df_batch1 = spark.table("bronze.transactions").limit(100000)
(
    df_batch1
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/Volumes/main/landing/raw/transactions/batch_2024_01.csv")
)
print("batch_2024_01.csv subido")

# Simular un segundo archivo que llega al día siguiente
df_batch2 = spark.table("bronze.transactions").orderBy("date").limit(50000)
(
    df_batch2
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/Volumes/main/landing/raw/transactions/batch_2024_02.csv")
)
print("batch_2024_02.csv subido")
```

Ejecuta el pipeline después de subir `batch_2024_01.csv`. Luego ejecuta el pipeline de nuevo después de `batch_2024_02.csv`. Documenta cuántas filas tiene la tabla después de cada ejecución.

---

## Parte 2 — Los tres comportamientos de Quality Expectations

DLT tiene tres formas de reaccionar cuando una fila viola una regla de calidad:

| Decorador | Comportamiento | Cuándo usarlo |
|-----------|---------------|---------------|
| `@dp.expect` | Registra la violación como métrica pero **no elimina** la fila | Calidad informativa (quieres saber pero no rechazar) |
| `@dp.expect_or_drop` | **Elimina las filas** que violan la regla | Filas inválidas que contaminarían Silver |
| `@dp.expect_or_fail` | **Detiene el pipeline** si alguna fila viola la regla | Violaciones críticas (ej: columna clave nunca puede ser null) |

```python
@dp.expect("amount_no_nulo", "amount IS NOT NULL")
@dp.expect_or_drop("user_id_valido", "client_id IS NOT NULL AND client_id > 0")
@dp.expect_or_fail("fecha_requerida", "date IS NOT NULL")
@dp.table(
    name="bronze_transactions_validada",
    comment="Transacciones con reglas de calidad aplicadas",
)
def bronze_transactions_validada():
    return spark.readStream.table("bronze_transactions_stream")
```

> El orden de los decoradores importa: primero las expectations, luego `@dp.table`. El pipeline los aplica de arriba hacia abajo.

Agrega las expectations y ejecuta el pipeline. En la interfaz del pipeline, busca la pestaña **Data Quality** — verás las métricas de cada regla:
- filas evaluadas
- filas que pasaron
- filas que fallaron (y qué porcentaje)

Documenta las métricas en tu notebook.

---

## Parte 3 — Múltiples expectations con nombres descriptivos

Las expectations deben leerse como documentación de negocio:

```python
@dp.expect_all_or_drop({
    "amount_es_numerico":      "amount IS NOT NULL",
    "user_id_positivo":        "client_id > 0",
    "mcc_code_presente":       "mcc_code IS NOT NULL",
    "merchant_city_no_vacia":  "merchant_city IS NOT NULL AND merchant_city != ''",
})
@dp.table(
    name="silver_transactions_clean",
    comment="Transacciones Silver — solo filas que pasan todas las reglas de calidad",
)
def silver_transactions_clean():
    from pyspark.sql.functions import regexp_replace, col, to_timestamp, hour, month, year, abs as spark_abs
    return (
        spark.readStream.table("bronze_transactions_validada")
        .withColumnRenamed("id", "transaction_id")
        .withColumnRenamed("client_id", "user_id")
        .withColumn("amount",
            regexp_replace(col("amount"), r"[$,]", "").cast("double"))
        .withColumn("transaction_date",
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hora",        hour(col("transaction_date")))
        .withColumn("mes",         month(col("transaction_date")))
        .withColumn("anio",        year(col("transaction_date")))
        .withColumn("amount_abs",  spark_abs(col("amount")))
        .drop("date")
        .withColumn("_processed_at", current_timestamp())
    )
```

---

## Parte 4 — Tabla de cuarentena (quarantine pattern)

No siempre quieres descartar las filas inválidas — a veces quieres guardarlas en una tabla separada para investigarlas. Ese es el **quarantine pattern**.

```python
# Función auxiliar: leer el stream y marcar qué filas son válidas
def get_bronze_con_flag():
    from pyspark.sql.functions import (
        col, when, lit
    )
    return (
        spark.readStream.table("bronze_transactions_stream")
        .withColumn("es_valida",
            (col("amount").isNotNull()) &
            (col("client_id").isNotNull()) &
            (col("client_id").cast("long") > 0)
        )
    )

@dp.table(name="silver_transactions_validas")
def silver_transactions_validas():
    return get_bronze_con_flag().filter("es_valida = true")

@dp.table(
    name="cuarentena_transactions",
    comment="Filas rechazadas por reglas de calidad — para investigación",
    table_properties={"quality": "quarantine"},
)
def cuarentena_transactions():
    return (
        get_bronze_con_flag()
        .filter("es_valida = false")
        .withColumn("_quarantine_reason", lit("amount IS NULL OR client_id inválido"))
        .withColumn("_quarantine_at", current_timestamp())
    )
```

> El quarantine pattern no usa `@dp.expect_or_drop` porque quieres las filas — solo en otro lado. En producción, el equipo de calidad revisa la tabla de cuarentena periódicamente.

Verifica cuántas filas quedaron en cuarentena:
```python
# En un notebook separado (no DLT) — para inspección post-pipeline
spark.table("<schema_del_pipeline>.cuarentena_transactions").show(10)
spark.sql("SELECT _quarantine_reason, COUNT(*) FROM <schema_del_pipeline>.cuarentena_transactions GROUP BY 1").show()
```

---

## Parte 5 — Reflexión

1. Auto Loader usa un checkpoint para rastrear qué archivos ya procesó. ¿Qué pasa si eliminas ese checkpoint y corres el pipeline de nuevo? ¿Es seguro hacerlo en producción?

2. `@dp.expect` no elimina filas pero registra la violación. ¿Tiene sentido usar `@dp.expect` en una tabla Silver (supuestamente limpia)? ¿Cuándo sí y cuándo no?

3. La tabla de cuarentena y la tabla válida leen el mismo stream. ¿Eso significa que los datos se procesan dos veces? ¿Cómo optimiza el pipeline internamente este patrón?

4. En el Spark UI de semana 02, viste `SortMergeJoin` y `Exchange` como señales de shuffle. En el DLT Dashboard, ¿qué métricas equivalentes observas?

---

## Entrega en Git

```bash
git add semana_05/actividades/actividad_02/<tu-nombre>/
git commit -m "feat: auto loader + quality expectations + quarantine pattern - <tu-nombre>"
git push origin feature/semana05-dlt-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 05] Auto Loader + Quality Expectations — <Tu Nombre>
```

Incluye:
- Screenshot del Data Quality tab mostrando métricas de cada expectation
- Screenshot del DAG incluyendo la tabla de cuarentena
- Respuestas a las preguntas de reflexión

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Auto Loader con `cloudFiles` | Dos batches procesados incrementalmente | 25% |
| Los 3 comportamientos (`expect`, `expect_or_drop`, `expect_or_fail`) | Cada uno demostrado | 25% |
| `@dp.expect_all_or_drop` con 4 reglas | Nombres descriptivos de negocio | 15% |
| Quarantine pattern implementado | Tabla de cuarentena con reason y timestamp | 25% |
| Reflexión respondida | Respuestas técnicas concretas | 10% |

---

## Referencias

- [Auto Loader — cloudFiles](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [Lakeflow Pipelines — Data Quality Expectations](https://learn.microsoft.com/en-us/azure/databricks/dlt/expectations)
- [Quarantine pattern en pipelines declarativos](https://learn.microsoft.com/en-us/azure/databricks/dlt/tutorial-python)
- [What happened to @dlt?](https://learn.microsoft.com/en-us/azure/databricks/dlt/what-happened-to-dlt)
