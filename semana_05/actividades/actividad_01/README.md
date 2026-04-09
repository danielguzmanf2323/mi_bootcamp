# Actividad 01 — Semana 05: Introducción a Lakeflow Spark Declarative Pipelines

**Semana:** 05  
**Tema:** Lakeflow Spark Declarative Pipelines — del notebook orquestado al pipeline declarativo  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
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

**Lakeflow Spark Declarative Pipelines (SDP)** resuelve estos problemas: defines **qué** quieres (`@dp.materialized_view()` para cargas batch, `@dp.table()` para streams), Databricks se encarga del **cómo** — dependencias, retries, lineage, calidad, y progress tracking.

> El módulo Python que antes se llamaba `dlt` ahora se llama `pyspark.pipelines`. Se importa como `from pyspark import pipelines as dp`.

---

## Material de estudio previo

- ¿Qué es un Lakeflow Spark Declarative Pipeline? ¿Cómo difiere de un Databricks Job?
- ¿Qué son los Pipeline Parameters? ¿Por qué `dbutils.widgets` no funciona dentro de un pipeline declarativo?
- ¿Qué modos de ejecución tiene un pipeline declarativo? (Triggered vs Continuous)
- ¿Qué es el Data Lineage y cómo lo muestra Databricks automáticamente?
- ¿Cuándo usar `@dp.materialized_view()` vs `@dp.table()`?

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

## Lakeflow Spark Declarative Pipelines (SDP)

- Unidad: función Python decorada con @dp.materialized_view() o @dp.table()
- Dependencias: inferidas automáticamente (spark.read.table("tabla_fuente"))
- Data quality: @dp.expect, @dp.expect_or_drop, @dp.expect_or_fail
- Linaje: Databricks lo construye y visualiza automáticamente
- Restart: el pipeline detecta qué tablas cambiaron y solo reprocesa lo necesario
- Parametrización: Pipeline Parameters (spark.conf.get("pipelines.parameter.X"))
```

---

## Parte 2 — Tu primer @dp.materialized_view()

Crea un notebook declarativo `bronze_pipeline_<tu-nombre>.py`. Este notebook NO se ejecuta directamente — es la definición del pipeline.

> **Importante:** un notebook de pipeline declarativo no tiene el botón "Run All". Se ejecuta solo a través de un Pipeline en Workflows → Lakeflow Pipelines.

> **`@dp.materialized_view()` vs `@dp.table()`:** Las tablas que leen datos estáticos con `spark.read` son **materialized views**. Las que leen un stream con `spark.readStream` son **streaming tables** (`@dp.table()`). Esta actividad usa `spark.read` — cargas batch desde Volumes, sin Auto Loader.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

# Pipeline parameter — reemplaza dbutils.widgets.get() en el pipeline declarativo
environment = spark.conf.get("pipelines.parameter.environment", "dev")
print(f"Entorno: {environment}")  # Esto correrá en el log del pipeline

# Ruta base según entorno
volumes_base = spark.conf.get(
    "pipelines.parameter.volumes_path",
    "/Volumes/main/landing/raw"
)
```

```python
@dp.materialized_view(
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
@dp.materialized_view(
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
@dp.materialized_view(
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

1. Ve a **Workflows → Lakeflow Pipelines → Create Pipeline**

2. Configura:

| Campo | Valor |
|-------|-------|
| Pipeline name | `financial_bronze_<tu-nombre>` |
| Product edition | Core (suficiente para esta actividad) |
| Notebook libraries | Path a tu notebook `bronze_pipeline_<tu-nombre>` |
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
manualmente. Aquí lo infiere de spark.read.table("nombre_tabla").
```

---

## Parte 5 — Agregar Silver en el mismo notebook

Agrega estas funciones al mismo notebook:

```python
from pyspark.sql.functions import (
    regexp_replace, col, to_timestamp, hour, dayofweek, month, year, abs as spark_abs
)

@dp.materialized_view(
    name="silver_transactions",
    comment="Transacciones limpias con features calculadas",
    table_properties={"quality": "silver"},
)
def silver_transactions():
    return (
        # spark.read.table() crea la dependencia automática — el pipeline sabe que necesita bronze_transactions
        spark.read.table("bronze_transactions")
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

Databricks entiende que `silver_transactions` depende de `bronze_transactions` porque la función llama a `spark.read.table("bronze_transactions")`. Sin configurar nada más.

---

## Parte 6 — Reflexión: qué cambió respecto a semana 04

En una celda markdown separada, responde:

1. En semana 04 el orquestador llamaba a `dbutils.notebook.run()` con parámetros. ¿Cuál es el equivalente en DLT?

2. ¿Puedes usar `dbutils.widgets.get()` dentro de un notebook DLT? ¿Por qué sí o no?

3. El DAG del DLT Pipeline es visual y automático. ¿Qué tenías que hacer manualmente en semana 04 para que Bronze corriera antes que Silver?

4. Si `bronze_transactions` falla, ¿el pipeline intenta correr `silver_transactions`? ¿Qué pasa en Jobs con `depends_on`?

5. ¿Cuándo usarías `@dp.table()` en lugar de `@dp.materialized_view()`? Da un ejemplo concreto del pipeline de esta actividad.

---

## Entrega en Git

```bash
git add semana_05/actividades/actividad_01/<tu-nombre>/
git commit -m "feat: first declarative pipeline - bronze + silver - <tu-nombre>"
git push origin feature/semana05-dlt-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 05] Declarative Pipeline Fundamentos — <Tu Nombre>
```

Incluye en el PR:
- Screenshot del DAG del pipeline con las tablas en verde
- Screenshot de una de las tablas en el Data Explorer (Catalog)
- Respuestas a las 5 preguntas de reflexión

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Comparación Jobs vs Declarative Pipeline documentada | Diferencias técnicas concretas | 15% |
| 3 tablas Bronze definidas con `@dp.materialized_view()` | Con `comment`, `_ingested_at`, `_source` | 30% |
| Pipeline Parameter `environment` usado | Leído con `spark.conf.get()` | 15% |
| Silver definida con `spark.read.table()` | Dependencia automática en el DAG | 25% |
| Screenshot del DAG y reflexión | Evidencia de ejecución exitosa | 15% |

---

## Referencias

- [Azure Databricks — Lakeflow Spark Declarative Pipelines](https://learn.microsoft.com/en-us/azure/databricks/dlt/)
- [Lakeflow Pipelines — Python reference](https://learn.microsoft.com/en-us/azure/databricks/dlt/python-ref)
- [Pipeline Parameters](https://learn.microsoft.com/en-us/azure/databricks/dlt/settings#pipeline-parameters)
- [What happened to @dlt?](https://learn.microsoft.com/en-us/azure/databricks/dlt/what-happened-to-dlt)
