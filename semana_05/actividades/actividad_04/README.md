# Actividad 04 — Semana 05: Pipeline DLT completa + Notificaciones de fallo

**Semana:** 05  
**Tema:** Unificar Bronze→Silver→Gold en un pipeline DLT + alertas cuando algo falla  
**Nivel:** Avanzado  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
**Prerequisito:** Actividades 01, 02 y 03 completadas

---

## Contexto

En las 3 actividades anteriores construiste piezas separadas:
- **Act 01:** `@dlt.table` básico, pipeline parameters, Silver con `dlt.read()`
- **Act 02:** Auto Loader con `cloudFiles`, Quality Expectations, quarantine pattern
- **Act 03:** `dlt.apply_changes()` para SCD1 y SCD2

Esta actividad las une en **un único pipeline DLT** que va de extremo a extremo: Bronze → Silver (con CDC) → Gold, con calidad en cada capa.

El segundo objetivo es igual de importante: **cuando el pipeline falla en producción a las 3am, alguien tiene que enterarse.** Configuras las notificaciones para que lleguen por email (o webhook) automáticamente.

---

## Material de estudio previo

- ¿Cómo se estruturan múltiples notebooks en un solo DLT Pipeline?
- ¿Cuáles son los eventos de pipeline que pueden disparar una notificación? (`on_update_success`, `on_update_failure`, etc.)
- ¿Qué es un webhook en el contexto de notificaciones de Databricks?
- ¿Qué es el modo `development` vs `production` de un DLT Pipeline?

---

## Instrucciones Git

```bash
git checkout feature/semana05-dlt-<tu-nombre>
```

Crea tus archivos en:
```
semana_05/actividades/actividad_04/<tu-nombre>/
```

---

## Parte 1 — Estructura del pipeline completo

Un DLT Pipeline puede incluir múltiples notebooks. Databricks los ejecuta como si fueran uno solo — las tablas definidas en un notebook pueden ser leídas por otro con `dlt.read()`.

Estructura que construirás:

```
semana_05/actividades/actividad_04/<tu-nombre>/
├── 01_bronze.py      # Auto Loader: transactions + users + mcc_codes
├── 02_silver.py      # Transformaciones + Quality Expectations + SCD2 en users
└── 03_gold.py        # Agregados gold: reporte de fraude diario
```

El pipeline en Databricks apuntará a los 3 notebooks. DLT resuelve las dependencias entre ellos automáticamente.

---

## Parte 2 — Bronze (01_bronze.py)

```python
# 01_bronze.py
import dlt
from pyspark.sql.functions import current_timestamp, lit, input_file_name

volumes_base = spark.conf.get("pipelines.parameter.volumes_path", "/Volumes/main/landing/raw")

@dlt.table(
    name="bronze_transactions",
    comment="Transacciones financieras — Auto Loader incremental",
    table_properties={"quality": "bronze"},
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation",
                f"/pipelines/checkpoints/{dlt.current_table_name()}/schema")
        .load(f"{volumes_base}/transactions/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

@dlt.table(
    name="bronze_users_cdc",
    comment="Feed CDC de usuarios — cambios de atributos de clientes",
    table_properties={"quality": "bronze"},
)
def bronze_users_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "delta")
        .option("cloudFiles.schemaLocation",
                f"/pipelines/checkpoints/{dlt.current_table_name()}/schema")
        .load("/Volumes/main/landing/raw/users_cdc/")
        .withColumn("_ingested_at", current_timestamp())
    )

@dlt.table(
    name="bronze_mcc_codes",
    comment="Catálogo MCC — carga completa (tabla pequeña, no incremental)",
    table_properties={"quality": "bronze"},
)
def bronze_mcc_codes():
    # Las tablas de catálogo pequeñas se pueden cargar completas sin Auto Loader
    return (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load(f"{volumes_base}/mcc_codes.json")
    )
```

---

## Parte 3 — Silver (02_silver.py)

```python
# 02_silver.py
import dlt
from pyspark.sql.functions import (
    col, regexp_replace, to_timestamp, hour, month, year,
    dayofweek, abs as spark_abs, current_timestamp, lit
)

# ─── Silver transactions (con Expectations) ───────────────────────────────────

@dlt.expect_all_or_drop({
    "amount_presente":    "amount IS NOT NULL",
    "user_id_valido":     "client_id IS NOT NULL AND client_id > 0",
    "fecha_presente":     "date IS NOT NULL",
    "mcc_code_presente":  "mcc_code IS NOT NULL",
})
@dlt.table(
    name="silver_transactions",
    comment="Transacciones limpias con features calculadas",
    table_properties={"quality": "silver"},
)
def silver_transactions():
    return (
        dlt.read_stream("bronze_transactions")
        .withColumnRenamed("id", "transaction_id")
        .withColumnRenamed("client_id", "user_id")
        .withColumn("amount",
            regexp_replace(col("amount"), r"[$,]", "").cast("double"))
        .withColumn("transaction_date",
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hora",       hour(col("transaction_date")))
        .withColumn("mes",        month(col("transaction_date")))
        .withColumn("anio",       year(col("transaction_date")))
        .withColumn("dia_semana", dayofweek(col("transaction_date")))
        .withColumn("amount_abs", spark_abs(col("amount")))
        .drop("date")
        .withColumn("_processed_at", current_timestamp())
    )

# ─── Silver users SCD2 ────────────────────────────────────────────────────────

dlt.create_streaming_table(
    name="silver_users",
    comment="Dimensión de clientes — SCD2 (historial completo de cambios)",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_users",
    source="bronze_users_cdc",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=col("operacion") == "DELETE",
    except_column_list=["operacion", "updated_at"],
    stored_as_scd_type="2",
)

# ─── Tabla de cuarentena ──────────────────────────────────────────────────────

def get_tx_con_flag():
    return (
        dlt.read_stream("bronze_transactions")
        .withColumn("es_valida",
            col("amount").isNotNull() &
            col("client_id").isNotNull()
        )
    )

@dlt.table(
    name="cuarentena_transactions",
    comment="Filas rechazadas — investigación de calidad",
    table_properties={"quality": "quarantine"},
)
def cuarentena_transactions():
    return (
        get_tx_con_flag()
        .filter("es_valida = false")
        .withColumn("_quarantine_reason", lit("amount IS NULL OR client_id IS NULL"))
        .withColumn("_quarantine_at", current_timestamp())
    )
```

---

## Parte 4 — Gold (03_gold.py)

```python
# 03_gold.py
import dlt
from pyspark.sql.functions import (
    col, to_date, count, sum as spark_sum, avg, round as spark_round, lit
)

@dlt.table(
    name="gold_reporte_fraude_diario",
    comment="Reporte Gold: tasa de fraude por día — para dashboards de riesgo",
    table_properties={"quality": "gold"},
)
def gold_reporte_fraude_diario():
    # dlt.read (no readStream) porque la tabla Silver ya hace la agregación completa
    df = dlt.read("silver_transactions")

    if "is_fraud" not in df.columns:
        df = df.withColumn("is_fraud", lit(0))

    return (
        df
        .withColumn("fecha", to_date(col("transaction_date")))
        .groupBy("fecha")
        .agg(
            count("*").alias("total_transacciones"),
            spark_sum(col("is_fraud")).alias("total_fraude"),
            spark_round((spark_sum(col("is_fraud")) / count("*")) * 100, 2).alias("tasa_fraude_pct"),
            spark_round(avg(col("amount_abs")), 2).alias("monto_promedio"),
        )
        .orderBy("fecha")
        .withColumn("_generated_at", col("fecha"))  # para particionado futuro
    )

@dlt.table(
    name="gold_usuarios_activos",
    comment="Usuarios con actividad reciente — join Silver transactions + Silver users",
    table_properties={"quality": "gold"},
)
def gold_usuarios_activos():
    from pyspark.sql.functions import max as spark_max, count as spark_count

    df_tx    = dlt.read("silver_transactions")
    # Para users SCD2, filtrar solo el estado actual
    df_users = dlt.read("silver_users").filter(col("__CURRENT") == True)

    return (
        df_tx
        .groupBy("user_id")
        .agg(
            spark_count("*").alias("total_transacciones"),
            spark_max("transaction_date").alias("ultima_transaccion"),
            avg("amount_abs").alias("monto_promedio"),
        )
        .join(df_users.select("user_id", "city", "state"), on="user_id", how="left")
    )
```

---

## Parte 5 — Crear el pipeline con los 3 notebooks

En la interfaz de Databricks:

1. **Workflows → Delta Live Tables → Create Pipeline**

2. Configura:

| Campo | Valor |
|-------|-------|
| Pipeline name | `financial_pipeline_completo_<tu-nombre>` |
| Product edition | Advanced (necesario para SCD2 y apply_changes) |
| Notebook libraries | Agregar los 3 notebooks en orden: 01, 02, 03 |
| Target schema | `pipeline_<tu-nombre>` |
| Compute | Serverless |
| Pipeline mode | Triggered |

3. Pipeline Parameters:

| Key | Value |
|-----|-------|
| `volumes_path` | `/Volumes/main/landing/raw` |
| `environment` | `dev` |

4. Haz clic en **Start**. Observa que:
   - Los 3 notebooks se ejecutan como un solo pipeline
   - DLT infiere las dependencias entre tablas automáticamente
   - El DAG muestra Bronze → Silver → Gold en cascada

---

## Parte 6 — Configurar notificaciones de fallo

Cuando el pipeline falla en producción, el equipo tiene que saber. Databricks soporta dos mecanismos: email y webhooks.

### 6.1 Notificaciones por email (recomendado para comenzar)

En la configuración del pipeline, sección **Notifications**:

1. Haz clic en **Add notification**
2. En **Email addresses**, agrega tu email corporativo
3. Selecciona los eventos:
   - `on_update_failure` — el pipeline falló
   - `on_update_success` — el pipeline terminó bien (opcional, puede ser ruidoso)
   - `on_flow_failure` — una tabla específica falló

4. Guarda los cambios

### 6.2 Simular un fallo para ver la notificación

Modifica temporalmente la tabla `silver_transactions` para agregar un `@dlt.expect_or_fail` que fallará deliberadamente:

```python
# TEMPORAL — solo para probar la notificación
@dlt.expect_or_fail("test_fallo_deliberado", "1 = 0")  # siempre falla
@dlt.table(name="tabla_que_fallara_a_proposito")
def tabla_que_fallara_a_proposito():
    return dlt.read("bronze_transactions").limit(1)
```

Ejecuta el pipeline. Debería fallar y mandarte un email.

Luego **elimina esta tabla** del notebook y vuelve a ejecutar el pipeline en estado correcto.

### 6.3 Webhook (Teams, Slack, PagerDuty)

Para notificaciones a Teams o Slack, usa la API de Databricks para configurar un webhook:

```python
# Notebook de configuración (no DLT) — ejecutar una vez por pipeline
import requests

DATABRICKS_HOST  = "https://<tu-workspace>.azuredatabricks.net"
DATABRICKS_TOKEN = dbutils.secrets.get(scope="secrets", key="databricks_token")
PIPELINE_ID      = "<id-del-pipeline>"

# Ejemplo: agregar webhook de Teams
payload = {
    "webhook": {
        "url": "https://outlook.office.com/webhook/<tu-teams-webhook-url>",
        "events": ["on_update_failure", "on_flow_failure"],
    }
}

response = requests.post(
    f"{DATABRICKS_HOST}/api/2.0/pipelines/{PIPELINE_ID}/notifications",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json=payload,
)
print(f"Status: {response.status_code}")
print(response.json())
```

> Si no tienes un webhook de Teams disponible, el email es suficiente para esta actividad. La configuración de webhook es opcional.

---

## Parte 7 — Modo Development vs Production

DLT tiene dos modos de ejecución:

| | Development mode | Production mode |
|---|---|---|
| **Comportamiento de errores** | Reintentos limitados, falla rápido | Reintentos automáticos (configurable) |
| **Cluster** | Se reutiliza entre ejecuciones (arranque rápido para iterar) | Se recrea en cada run (siempre estado limpio) |
| **Uso** | Mientras desarrollas y pruebas | Jobs automáticos, scheduled pipelines |

Cambia el pipeline a **Production mode** y vuélvelo a ejecutar. Documenta qué cambió en el comportamiento desde la interfaz.

---

## Entrega en Git

```bash
git add semana_05/actividades/actividad_04/<tu-nombre>/
git commit -m "feat: full DLT pipeline bronze->silver(SCD2)->gold + notifications - <tu-nombre>"
git push origin feature/semana05-dlt-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 05] Pipeline DLT completo + Notificaciones — <Tu Nombre>
```

Incluye en el PR:
- Screenshot del DAG completo con Bronze, Silver (incluye SCD2 y cuarentena), Gold
- Screenshot del email de notificación de fallo (del fallo deliberado)
- Diferencias observadas entre Development y Production mode

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Pipeline con 3 notebooks registrados | `01_bronze`, `02_silver`, `03_gold` | 15% |
| Silver con Expectations y SCD2 en mismo pipeline | Ambas funcionalidades activas | 30% |
| Gold con 2 tablas analíticas | Fraude diario + usuarios activos con join | 20% |
| Notificación por email funcionando | Screenshot del email de fallo deliberado | 20% |
| Development vs Production mode documentado | Diferencias concretas observadas | 10% |
| DAG visual completo | Screenshot con todas las tablas | 5% |

---

## Referencias

- [DLT Pipeline notifications](https://docs.databricks.com/en/delta-live-tables/settings.html#configure-pipeline-notifications)
- [DLT development and production modes](https://docs.databricks.com/en/delta-live-tables/updates.html)
- [Databricks Secrets — dbutils.secrets](https://docs.databricks.com/en/security/secrets/index.html)
