# Proyecto Semana 05 — Pipeline DLT de producción: e-commerce de retail bancario

**Semana:** 05  
**Tema:** Pipeline DLT completo con nuevo dataset — CDC, SCD2, calidad, Gold, notificaciones  
**Nivel:** Avanzado  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise

---

## Contexto

Las 4 actividades de esta semana te dieron todas las piezas:
- `@dlt.table` con pipeline parameters
- Auto Loader incremental con `cloudFiles`  
- Quality Expectations (warn / drop / fail) y quarantine pattern
- `dlt.apply_changes()` para SCD1 y SCD2
- Pipeline de 3 notebooks con notificaciones configuradas

Este proyecto integra todo eso sobre un **escenario diferente** al de las actividades: datos de un sistema de *e-commerce bancario* donde los clientes compran en comercios usando sus tarjetas. El dataset es el mismo que conoces (Financial Transactions de Caixabank Tech), pero el escenario simula una ingesta real con:

1. **Batches diarios** de transacciones nuevas (Auto Loader)
2. **Feed CDC de clientes** donde los atributos cambian (SCD2 en users)
3. **Reglas de calidad** que el equipo de riesgo definió como contractuales
4. **Capa Gold** que alimenta dos dashboards: fraude por día y perfil de tarjeta

---

## El caso de negocio

El equipo de riesgo te pasa esta especificación:

> "Necesitamos una pipeline que:
> - Ingeste transacciones en cuanto lleguen (no esperar al batch semanal)  
> - Mantenga el historial completo de cambios de dirección de los clientes (regulatorio)  
> - Rechace automáticamente transacciones con monto nulo o usuario desconocido  
> - Produzca para las 6am un reporte diario de fraude por categoría de comercio  
> - Avise al equipo de ingeniería si algo falla — no queremos enterarnos por el equipo de analítica"

Tu pipeline tiene que cumplir esa especificación.

---

## Estructura del proyecto

```
semana_05/proyecto/<tu-nombre>/
├── notebooks/
│   ├── 01_bronze_ingesta.py         # Auto Loader: transactions + users CDC + mcc_codes
│   ├── 02_silver_quality.py         # Expectations + SCD2 en users + cuarentena
│   └── 03_gold_reportes.py          # 2 tablas Gold para dashboards
├── simulacion/
│   └── generar_batches_cdc.py       # Notebook para simular batches y cambios
├── pipeline_spec.json               # Exportado desde la UI de Databricks
└── analisis_calidad.md              # Documento con resultados de calidad observados
```

---

## Requisitos del pipeline

### Bronze

| Tabla | Fuente | Método | Notas |
|-------|--------|--------|-------|
| `bronze_transactions` | `/Volumes/main/landing/raw/transactions/` | Auto Loader (cloudFiles CSV) | Incremental, columna `_source_file` |
| `bronze_users_feed` | `/Volumes/main/landing/raw/users_cdc/` | Auto Loader (cloudFiles Delta) | Feed CDC con columna `operacion` |
| `bronze_mcc_codes` | `/Volumes/main/landing/raw/mcc_codes.json` | `spark.read` (catálogo estático) | Carga completa OK para tablas pequeñas |
| `bronze_fraud_labels` | `/Volumes/main/landing/raw/train_fraud_labels.json` | `spark.read` | Etiquetas de fraude por transacción |

### Silver

| Tabla | Origen | Tipo | Expectativas |
|-------|--------|------|-------------|
| `silver_transactions` | `bronze_transactions` + `bronze_fraud_labels` | Streaming + expect_all_or_drop | `amount != null`, `user_id > 0`, `fecha != null`, `mcc_code != null` |
| `silver_users` | `bronze_users_feed` | SCD2 via `apply_changes` | Historial completo de cambios de atributos |
| `cuarentena_transactions` | `bronze_transactions` | Quarantine pattern | Filas que no pasan validaciones de Silver |

### Gold

| Tabla | Descripción | Granularidad |
|-------|-------------|-------------|
| `gold_fraude_por_categoria` | Fraude por categoría MCC + día | fecha + categoría_mcc |
| `gold_perfil_tarjeta` | Métricas por tipo de tarjeta | tipo_tarjeta |

---

## Parte 1 — Simular los batches de datos

Crea `simulacion/generar_batches_cdc.py` (notebook normal, no DLT):

```python
from pyspark.sql.functions import lit, current_timestamp, col, rand
import random

# Batch 1 de transacciones — primer día
df_all_tx = spark.table("bronze.transactions")
df_batch1  = df_all_tx.orderBy("date").limit(300000)

(
    df_batch1.write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/Volumes/main/landing/raw/transactions/batch_dia_01.csv")
)
print(f"Batch 1: {df_batch1.count():,} transacciones")

# Batch 2 de transacciones — segundo día (registros más recientes)
df_batch2 = df_all_tx.orderBy("date", ascending=False).limit(200000)

(
    df_batch2.write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("/Volumes/main/landing/raw/transactions/batch_dia_02.csv")
)
print(f"Batch 2: {df_batch2.count():,} transacciones")
```

```python
# Feed CDC de usuarios
df_users = spark.table("bronze.users")

# Día 1: 500 usuarios cambian de ciudad (UPDATE)
df_cambios_ciudad = (
    df_users.limit(500)
    .withColumn("city",       lit("Madrid"))
    .withColumn("operacion",  lit("UPDATE"))
    .withColumn("updated_at", current_timestamp())
)

# Día 1: 200 usuarios nuevos (INSERT)
df_nuevos = (
    df_users.orderBy("user_id", ascending=False).limit(200)
    .withColumn("user_id",    (col("user_id") + 9000000).cast("long"))
    .withColumn("operacion",  lit("INSERT"))
    .withColumn("updated_at", current_timestamp())
)

# Día 1: 100 usuarios dados de baja (DELETE)
df_bajas = (
    df_users.limit(100)
    .orderBy("user_id")
    .withColumn("operacion",  lit("DELETE"))
    .withColumn("updated_at", current_timestamp())
)

df_cdc_dia1 = df_cambios_ciudad.union(df_nuevos).union(df_bajas)

(
    df_cdc_dia1.write.format("delta")
    .mode("overwrite")
    .save("/Volumes/main/landing/raw/users_cdc/dia_01/")
)
print(f"Feed CDC día 1: {df_cdc_dia1.count()} registros")
```

---

## Parte 2 — Notebook Bronze

El `01_bronze_ingesta.py` ya lo tienes de referencia en la Actividad 04. **Adapta el siguiente elemento** para este proyecto: el join con `bronze_fraud_labels` debe hacerse en Bronze o en Silver. Decide cuál y justifica en `analisis_calidad.md`.

---

## Parte 3 — Notebook Silver con join de fraud_labels

La tabla Silver de transacciones debe incluir el campo `is_fraud`. El join con `bronze_fraud_labels` puede hacerse aquí:

```python
@dlt.expect_all_or_drop({
    "amount_presente":   "amount IS NOT NULL",
    "user_id_valido":    "client_id IS NOT NULL AND client_id > 0",
    "fecha_presente":    "date IS NOT NULL",
    "mcc_code_presente": "mcc_code IS NOT NULL",
})
@dlt.expect("is_fraud_presente", "is_fraud IS NOT NULL")  # warn, no drop — puede llegar tarde
@dlt.table(
    name="silver_transactions",
    comment="Transacciones limpias con etiqueta de fraude y features temporales",
    table_properties={"quality": "silver"},
)
def silver_transactions():
    from pyspark.sql.functions import (
        regexp_replace, col, to_timestamp, hour, month, year,
        dayofweek, abs as spark_abs, current_timestamp, coalesce, lit
    )

    df_tx = (
        dlt.read_stream("bronze_transactions")
        .withColumnRenamed("id", "transaction_id")
        .withColumnRenamed("client_id", "user_id")
        .withColumn("amount",
            regexp_replace(col("amount"), r"[$,]", "").cast("double"))
        .withColumn("transaction_date",
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hora",        hour(col("transaction_date")))
        .withColumn("mes",         month(col("transaction_date")))
        .withColumn("anio",        year(col("transaction_date")))
        .withColumn("dia_semana",  dayofweek(col("transaction_date")))
        .withColumn("amount_abs",  spark_abs(col("amount")))
        .drop("date")
    )

    df_fraud = dlt.read("bronze_fraud_labels").select("id", "is_fraud")

    return (
        df_tx
        .join(df_fraud, df_tx["transaction_id"] == df_fraud["id"], how="left")
        .withColumn("is_fraud", coalesce(col("is_fraud"), lit(0)))
        .drop("id")
        .withColumn("_processed_at", current_timestamp())
    )
```

---

## Parte 4 — Notebook Gold

```python
# 03_gold_reportes.py
import dlt
from pyspark.sql.functions import (
    col, to_date, count, sum as spark_sum, avg,
    round as spark_round, current_timestamp
)

@dlt.table(
    name="gold_fraude_por_categoria",
    comment="Fraude diario por categoría MCC — para dashboard de riesgo",
    table_properties={"quality": "gold"},
)
def gold_fraude_por_categoria():
    df_tx  = dlt.read("silver_transactions")
    df_mcc = dlt.read("bronze_mcc_codes")

    return (
        df_tx
        .join(df_mcc, on="mcc_code", how="left")
        .withColumn("fecha", to_date(col("transaction_date")))
        .groupBy("fecha", "mcc_code", "edited_description")
        .agg(
            count("*").alias("total_tx"),
            spark_sum(col("is_fraud")).alias("total_fraude"),
            spark_round(
                (spark_sum(col("is_fraud")) / count("*")) * 100, 2
            ).alias("tasa_fraude_pct"),
            spark_round(avg(col("amount_abs")), 2).alias("monto_promedio"),
        )
        .orderBy("fecha", col("total_fraude").desc())
        .withColumn("_generated_at", current_timestamp())
    )

@dlt.table(
    name="gold_perfil_tarjeta",
    comment="Métricas agregadas por tipo de tarjeta — para análisis de producto",
    table_properties={"quality": "gold"},
)
def gold_perfil_tarjeta():
    from pyspark.sql.functions import countDistinct

    df_tx    = dlt.read("silver_transactions")
    # Usar solo usuarios vigentes de SCD2
    df_users = dlt.read("silver_users").filter(col("__CURRENT") == True)
    df_cards = spark.table("bronze.cards")  # catálogo de tarjetas — no está en DLT, leer directo

    return (
        df_tx
        .join(df_cards.select("id", "card_type"), df_tx["card_id"] == df_cards["id"], how="left")
        .groupBy("card_type")
        .agg(
            count("*").alias("total_transacciones"),
            spark_sum(col("is_fraud")).alias("total_fraude"),
            spark_round(avg(col("amount_abs")), 2).alias("ticket_promedio"),
            countDistinct("user_id").alias("usuarios_distintos"),
        )
    )
```

---

## Parte 5 — Configurar notificaciones y exportar el pipeline spec

1. Configura notificaciones `on_update_failure` hacia tu email y (si tienes acceso) un canal de Teams
2. Ejecuta el pipeline completo
3. Verifica que Gold tiene datos con:

```sql
-- Ejecutar en un notebook normal post-pipeline
SELECT fecha, COUNT(*) categorias, SUM(total_fraude) fraude_dia
FROM <schema_pipeline>.gold_fraude_por_categoria
GROUP BY fecha
ORDER BY fecha
LIMIT 10;

SELECT card_type, total_transacciones, tasa_fraude_pct
FROM <schema_pipeline>.gold_perfil_tarjeta
ORDER BY total_transacciones DESC;
```

4. Exporta el pipeline: en la UI del pipeline → `...` → **Export pipeline settings** → guarda como `pipeline_spec.json`

---

## Análisis de calidad requerido

Escribe `analisis_calidad.md` respondiendo:

1. **Batch 1 vs Batch 2:** ¿cuántas filas ingresó Auto Loader en cada run? ¿Reprocesó datos del batch 1 en el segundo run?

2. **Expectations:** ¿cuántas filas fueron eliminadas por `expect_or_drop`? ¿Qué porcentaje del total? ¿Cuál fue la expectativa que más fallas registró?

3. **Cuarentena:** ¿cuántas filas quedaron en `cuarentena_transactions`? ¿Qué tienen en común?

4. **SCD2 usuarios:** después de procesar el feed CDC del Día 1, ¿cuántas filas tiene `silver_users`? ¿Cuántas más que el total de usuarios únicos? ¿Por qué?

5. **Decisión de diseño:** ¿Pusiste el join con `fraud_labels` en Bronze o en Silver? ¿Por qué? ¿Qué implica para el linaje?

---

## Entrega en Git

```bash
git add semana_05/proyecto/<tu-nombre>/
git commit -m "feat: full DLT production pipeline - CDC, SCD2, quality, gold - semana 05 - <tu-nombre>"
git push origin feature/semana05-dlt-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 05 Proyecto] Pipeline DLT producción completa — <Tu Nombre>
```

El PR debe incluir:
- Screenshot del DAG completo del pipeline
- `analisis_calidad.md` completo con respuestas basadas en datos reales
- `pipeline_spec.json` exportado
- Screenshot del resultado de las 2 queries de Gold
- Screenshot de la notificación de email (del fallo deliberado o del primer run con algún fallo)

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Bronze con Auto Loader para las 2 fuentes incrementales | Batches procesados sin duplicados | 15% |
| Silver transactions con 4+ expectations y quarantine | Métricas de calidad documentadas | 20% |
| SCD2 en silver_users funcionando | Historial con `__START_AT`, `__END_AT`, `__CURRENT` | 20% |
| Gold con 2 tablas analíticas | Datos correctos verificados con queries | 20% |
| Notificaciones configuradas | Screenshot de email de fallo | 10% |
| `analisis_calidad.md` con respuestas basadas en datos reales | 5 preguntas respondidas | 10% |
| `pipeline_spec.json` versionado | Archivo exportado y commiteado | 5% |

---

## Reflexión de cierre de módulo Databricks

En el PR, incluye un párrafo respondiendo:

> "Llevas 5 semanas en Databricks — de leer un CSV con pandas a un pipeline DLT con CDC, SCD2 y notificaciones. ¿Qué concepto de la semana 05 te costó más entender? ¿Cuándo tiene sentido usar un Job (semana 04) vs un DLT Pipeline (semana 05)? ¿Qué harías diferente si empezaras otra vez desde semana 01?"

---

## Referencias

- [Delta Live Tables best practices](https://docs.databricks.com/en/delta-live-tables/best-practices.html)
- [DLT — Pipeline settings export](https://docs.databricks.com/en/delta-live-tables/settings.html)
- [Auto Loader schema evolution](https://docs.databricks.com/en/ingestion/auto-loader/schema.html)
