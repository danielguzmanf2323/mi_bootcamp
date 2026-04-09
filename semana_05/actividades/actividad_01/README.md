# Actividad 01 — Semana 05: Introducción a Declarative Pipelines (DLT)

**Semana:** 05  
**Tema:** Delta Live Tables — del notebook orquestado al pipeline declarativo  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise (DLT requiere licencia Standard o Advanced)  
**Prerequisito:** Semana 04 completa — Jobs funcionando, YAML-driven pipeline

---

## Contexto

En semana 04 construiste una pipeline completa:
- notebooks genéricos parametrizados con `dbutils.widgets`
- YAML como fuente de configuración
- Job de Databricks encadenando Bronze → Silver → Gold

Eso es producción real. Pero tiene problemas que se hacen visibles cuando la pipeline crece:

1. **Dependencias implícitas:** Silver asume que Bronze terminó porque `depends_on` lo garantiza, pero si Bronze falla a mitad, Silver puede correr igual sobre datos parciales
2. **Linaje manual:** para saber de dónde viene `gold.reporte_fraude_diario`, tienes que leer el código
3. **Data quality como código:** si quieres rechazar filas con `amount = null`, escribes filtros en PySpark — mezclado con la lógica de negocio
4. **Restart parcial:** si Silver falla en el registro 800k de 1M, al relanzar reprocesas los 800k

**Declarative Pipelines (DLT)** resuelve estos problemas: defines **qué** quieres (`@dlt.table`), Databricks se encarga del **cómo** — dependencias, retries, lineage, calidad, y progress tracking.

---

## Material de estudio previo

- ¿Qué es un DLT Pipeline? ¿Cómo difiere de un Databricks Job?
- ¿Qué son los Pipeline Parameters? ¿Por qué `dbutils.widgets` no funciona dentro de DLT?
- ¿Qué modos de ejecución tiene DLT? (Triggered vs Continuous)
- ¿Qué es el Data Lineage y cómo lo muestra DLT automáticamente?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana05-dlt-<tu-nombre>
```

Crea tu carpeta:
```
semana_05/actividades/actividad_01/<tu-nombre>/
```

---

## Parte 1 — Jobs vs DLT: la diferencia fundamental

Crea un notebook `comparacion_jobs_vs_dlt_<tu-nombre>.py` con estas celdas de análisis:

```markdown
## Jobs (semana 04)

- Unidad: notebook
- Dependencias: configuradas en la UI del Job (depends_on)
- Data quality: filtros escritos en PySpark dentro del notebook
- Linaje: deduces quién lee a quién leyendo el código
- Restart: si una task falla, todo el Job puede reiniciarse desde esa task
- Parametrización: dbutils.widgets + arguments del Job

## Declarative Pipelines (DLT)

- Unidad: función Python decorada con @dlt.table
- Dependencias: inferidas automáticamente (dlt.read("tabla_fuente"))
- Data quality: @dlt.expect, @dlt.expect_or_drop, @dlt.expect_or_fail
- Linaje: Databricks lo construye y visualiza automáticamente
- Restart: DLT detecta qué tablas cambiaron y solo reprocesa lo necesario
- Parametrización: Pipeline Parameters (spark.conf.get("pipelines.parameter.X"))
```

---

## Parte 2 — Tu primer @dlt.table

Crea un notebook DLT `bronze_dlt_<tu-nombre>.py`. Este notebook NO se ejecuta directamente — es la definición del pipeline.

> **Importante:** un notebook DLT no tiene el botón "Run All". Se ejecuta solo a través de un DLT Pipeline en Workflows → Delta Live Tables.

```python
import dlt
from pyspark.sql.functions import current_timestamp, lit

# Pipeline parameter — reemplaza dbutils.widgets.get() en DLT
environment = spark.conf.get("pipelines.parameter.environment", "dev")
print(f"Entorno: {environment}")  # Esto correrá en el log del pipeline

# Ruta base según entorno
volumes_base = spark.conf.get(
    "pipelines.parameter.volumes_path",
    "/Volumes/main/landing/raw"
)
```

```python
@dlt.table(
    name="bronze_transactions",
    comment="Transacciones financieras crudas — ingesta directa desde Volumes",
    table_properties={"quality": "bronze"},
)
def bronze_transactions():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{volumes_base}/transactions_data.csv")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(f"{volumes_base}/transactions_data.csv"))
    )
```

```python
@dlt.table(
    name="bronze_users",
    comment="Datos demográficos de clientes — ingesta desde Volumes",
    table_properties={"quality": "bronze"},
)
def bronze_users():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{volumes_base}/users_data.csv")
        .withColumn("_ingested_at", current_timestamp())
    )
```

```python
@dlt.table(
    name="bronze_mcc_codes",
    comment="Códigos MCC — catálogo de categorías de comercio",
    table_properties={"quality": "bronze"},
)
def bronze_mcc_codes():
    return (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load(f"{volumes_base}/mcc_codes.json")
    )
```

---

## Parte 3 — Crear el DLT Pipeline en la interfaz

1. Ve a **Workflows → Delta Live Tables → Create Pipeline**

2. Configura:

| Campo | Valor |
|-------|-------|
| Pipeline name | `financial_bronze_<tu-nombre>` |
| Product edition | Core (suficiente para esta actividad) |
| Notebook libraries | Path a tu notebook `bronze_dlt_<tu-nombre>` |
| Storage location | `/pipelines/<tu-nombre>/bronze` |
| Target schema | `bronze_dlt_<tu-nombre>` (Databricks crea el schema) |
| Compute | Serverless (recomendado) o un cluster existente |
| Pipeline mode | Triggered |

3. En **Configuration** (Advanced), agrega los Pipeline Parameters:

| Key | Value |
|-----|-------|
| `environment` | `dev` |
| `volumes_path` | `/Volumes/main/landing/raw` |

4. Haz clic en **Start** y observa el DAG que Databricks construye automáticamente.

---

## Parte 4 — Leer el lineage automático

Cuando el pipeline termine, observa el **DAG visual** en la interfaz del pipeline. Documenta:

```markdown
## Lineage automático observado

Pipeline: financial_bronze_<nombre>

Tablas creadas:
- bronze_transactions: √ | N filas | tiempo
- bronze_users:        √ | N filas | tiempo
- bronze_mcc_codes:    √ | N filas | tiempo

Sin ninguna línea de código de orquestación escrita, 
Databricks construyó el DAG y sabe que estas 3 tablas
son independientes entre sí → las ejecuta en paralelo.

Diferencia con Jobs: en Jobs, teníamos que definir `depends_on` 
manualmente. Aquí lo infiere de `dlt.read()`.
```

---

## Parte 5 — Agregar Silver en el mismo notebook

Agrega estas funciones al mismo notebook DLT:

```python
from pyspark.sql.functions import (
    regexp_replace, col, to_timestamp, hour, dayofweek, month, year, abs as spark_abs
)

@dlt.table(
    name="silver_transactions",
    comment="Transacciones limpias con features calculadas",
    table_properties={"quality": "silver"},
)
def silver_transactions():
    return (
        # dlt.read() crea la dependencia automática — DLT sabe que necesita bronze_transactions
        dlt.read("bronze_transactions")
        .withColumnRenamed("id", "transaction_id")
        .withColumnRenamed("client_id", "user_id")
        .withColumn("amount",
            regexp_replace(col("amount"), r"[$,]", "").cast("double")
        )
        .withColumn("transaction_date",
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("hora",         hour(col("transaction_date")))
        .withColumn("mes",          month(col("transaction_date")))
        .withColumn("anio",         year(col("transaction_date")))
        .withColumn("dia_semana",   dayofweek(col("transaction_date")))
        .withColumn("amount_abs",   spark_abs(col("amount")))
        .drop("date")
        .withColumn("_processed_at", current_timestamp())
    )
```

Vuelve a correr el pipeline (botón **Start**). El DAG ahora muestra:

```
bronze_transactions ──▶ silver_transactions
bronze_users
bronze_mcc_codes
```

Databricks entiende que `silver_transactions` depende de `bronze_transactions` porque la función llama a `dlt.read("bronze_transactions")`. Sin configurar nada más.

---

## Parte 6 — Reflexión: qué cambió respecto a semana 04

En una celda markdown separada, responde:

1. En semana 04 el orquestador llamaba a `dbutils.notebook.run()` con parámetros. ¿Cuál es el equivalente en DLT?

2. ¿Puedes usar `dbutils.widgets.get()` dentro de un notebook DLT? ¿Por qué sí o no?

3. El DAG del DLT Pipeline es visual y automático. ¿Qué tenías que hacer manualmente en semana 04 para que Bronze corriera antes que Silver?

4. Si `bronze_transactions` falla, ¿DLT intenta correr `silver_transactions`? ¿Qué pasa en Jobs con `depends_on`?

---

## Entrega en Git

```bash
git add semana_05/actividades/actividad_01/<tu-nombre>/
git commit -m "feat: first DLT pipeline - bronze + silver declarative - <tu-nombre>"
git push origin feature/semana05-dlt-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 05] DLT Fundamentos — <Tu Nombre>
```

Incluye en el PR:
- Screenshot del DAG del pipeline con las tablas en verde
- Screenshot de una de las tablas en el Data Explorer (Catalog)
- Respuestas a las 4 preguntas de reflexión

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Comparación Jobs vs DLT documentada | Diferencias técnicas concretas | 15% |
| 3 tablas Bronze definidas con `@dlt.table` | Con `comment`, `_ingested_at`, `_source` | 30% |
| Pipeline Parameter `environment` usado | Leído con `spark.conf.get()` | 15% |
| Silver definida con `dlt.read()` | Dependencia automática en el DAG | 25% |
| Screenshot del DAG y reflexión | Evidencia de ejecución exitosa | 15% |

---

## Referencias

- [Databricks — What is Delta Live Tables?](https://docs.databricks.com/en/delta-live-tables/index.html)
- [DLT Python syntax](https://docs.databricks.com/en/delta-live-tables/python-ref.html)
- [DLT Pipeline Parameters](https://docs.databricks.com/en/delta-live-tables/settings.html#pipeline-parameters)
