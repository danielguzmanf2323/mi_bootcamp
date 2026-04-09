# Actividad 04 — Semana 04: Silver metadata-driven y Databricks Jobs

**Semana:** 04  
**Tema:** Transformaciones declarativas desde YAML + orquestar con un Job de Databricks  
**Nivel:** Intermedio–Avanzado  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
**Prerequisito:** Actividades 01, 02 y 03 completadas

---

## Contexto

Tienes la capa Bronze funcionando: un YAML define las fuentes, un notebook genérico las ingesta, un orquestador las ejecuta en loop. Cero código quemado.

El siguiente problema es Silver. En semana 03, las transformaciones de Silver estaban quemadas en el notebook:

```python
# Semana 03 — todo quemado
df_silver = (
    df_bronze
    .withColumnRenamed("id", "transaction_id")
    .withColumnRenamed("client_id", "user_id")
    .withColumn("amount", regexp_replace(col("amount"), "[$,]", "").cast("double"))
    ...
)
df_silver.write.saveAsTable("silver.transactions")
```

Si cambia el nombre de una columna fuente, abres el notebook y editas. Si hay que agregar una columna derivada, abres el notebook y editas. En producción con decenas de tablas Silver, eso es insostenible.

La solución: el `silver_config.yml` describe **qué** transformaciones aplicar. El notebook Silver genérico las lee y las aplica sin saber de antemano qué columnas existen.

---

## Objetivo

1. Implementar un notebook Silver genérico que lee `silver_config.yml` y aplica transformaciones de forma declarativa
2. Crear un Databricks Job con dos tasks encadenadas: Bronze → Silver
3. Ejecutar el Job completo y verificar ambas capas

---

## Material de estudio previo

- ¿Qué es `eval()` en Python? ¿Por qué es peligroso en código de producción? ¿Qué alternativas existen?
- ¿Qué es `spark.sql()` con una expresión como string? ¿Cuándo conviene esto sobre llamadas a PySpark?
- ¿Qué es un Databricks Job? ¿Qué es una Task dentro de un Job?
- ¿Qué es la dependencia de tasks en un Job? (`depends_on`)

---

## Instrucciones Git

```bash
git checkout feature/semana04-widgets-<tu-nombre>
```

Crea tus archivos en:
```
semana_04/actividades/actividad_04/<tu-nombre>/
```

---

## El archivo de configuración Silver

Revisa `semana_04/configs/silver_config.yml`. La estructura que encontrarás:

```yaml
pipeline:
  name: financial_transactions_silver
  source_schema: bronze
  destination_schema: silver

tables:
  - name: transactions
    source: bronze.transactions
    destination: silver.transactions
    transformations:
      renames:
        - from: id
          to: transaction_id
        - from: client_id
          to: user_id
      casts:
        - column: amount
          expression: "regexp_replace(amount, '[$,]', '')"
          type: double
          alias: amount
        - column: date
          expression: "to_timestamp(date, 'yyyy-MM-dd HH:mm:ss')"
          type: timestamp
          alias: transaction_date
      derived_columns:
        - name: hora
          expression: "hour(transaction_date)"
        - name: dia_semana
          expression: "dayofweek(transaction_date)"
        - name: es_fin_de_semana
          expression: "case when dayofweek(transaction_date) in (1, 7) then 1 else 0 end"
        - name: mes
          expression: "month(transaction_date)"
        - name: anio
          expression: "year(transaction_date)"
        - name: amount_abs
          expression: "abs(amount)"
      drops:
        - date
      add_metadata:
        - column: _processed_at
          expression: "current_timestamp()"
        - column: _source_table
          value: "bronze.transactions"
    write_mode: overwrite
```

---

## Parte 1 — Leer el YAML Silver

```python
import yaml, os

# Resolver el path al YAML (mismo patrón que Actividad 03)
notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
repo_root = os.path.dirname(
             os.path.dirname(
             os.path.dirname(
             os.path.dirname(
             notebook_path))))

config_path = f"/Workspace{repo_root}/semana_04/configs/silver_config.yml"

with open(config_path, "r", encoding="utf-8") as f:
    silver_config = yaml.safe_load(f)

tablas = silver_config["tables"]
print(f"Tablas Silver a procesar: {[t['name'] for t in tablas]}")
```

---

## Parte 2 — Función: aplicar renames

```python
def aplicar_renames(df, renames: list):
    """
    Aplica renombrados de columnas definidos como lista de dicts {from, to}.
    """
    for r in renames:
        df = df.withColumnRenamed(r["from"], r["to"])
    return df
```

Prueba con un DataFrame de ejemplo:
```python
df_test = spark.table("bronze.transactions")
print("Antes:", df_test.columns[:5])
df_renamed = aplicar_renames(df_test, silver_config["tables"][0]["transformations"]["renames"])
print("Después:", df_renamed.columns[:5])
```

---

## Parte 3 — Función: aplicar casts con expresiones Spark SQL

```python
from pyspark.sql.functions import expr

def aplicar_casts(df, casts: list):
    """
    Aplica casts definidos en el YAML.
    Cada cast tiene: expression (Spark SQL), type, alias.
    
    Estrategia: evaluar la expresión como SQL con expr(), luego castear al tipo.
    """
    for c in casts:
        columna_calculada = expr(c["expression"]).cast(c["type"])
        df = df.withColumn(c["alias"], columna_calculada)
    return df
```

> `expr()` evalúa una string de Spark SQL como si fuera código PySpark. Es seguro dentro del contexto de Spark porque Spark valida la expresión antes de ejecutarla — a diferencia de `eval()` de Python, que ejecuta código arbitrario sin ninguna validación.

Prueba:
```python
df_cast = aplicar_casts(df_renamed, silver_config["tables"][0]["transformations"]["casts"])
df_cast.select("amount", "transaction_date").show(5)
df_cast.printSchema()
```

---

## Parte 4 — Función: columnas derivadas

```python
def aplicar_columnas_derivadas(df, derived_columns: list):
    """
    Agrega columnas calculadas a partir de expresiones Spark SQL.
    """
    for col_def in derived_columns:
        df = df.withColumn(col_def["name"], expr(col_def["expression"]))
    return df
```

```python
df_derived = aplicar_columnas_derivadas(
    df_cast,
    silver_config["tables"][0]["transformations"]["derived_columns"]
)
df_derived.select("transaction_date", "hora", "dia_semana", "es_fin_de_semana", "mes", "anio", "amount_abs").show(5)
```

---

## Parte 5 — Función: drops y metadatos

```python
from pyspark.sql.functions import current_timestamp, lit

def aplicar_drops(df, drops: list):
    return df.drop(*drops)

def agregar_metadata(df, metadata_config: list):
    """
    Agrega columnas de auditoría Silver.
    Cada item tiene 'column' y 'expression' o 'value'.
    """
    for m in metadata_config:
        if "expression" in m:
            df = df.withColumn(m["column"], expr(m["expression"]))
        elif "value" in m:
            df = df.withColumn(m["column"], lit(m["value"]))
    return df
```

---

## Parte 6 — Pipeline completa de una tabla

Ensambla todas las funciones en una sola función de transformación:

```python
def transformar_tabla_silver(tabla_config: dict) -> int:
    """
    Ejecuta la pipeline completa de transformación Silver para una tabla.
    Lee de Bronze, aplica transformaciones del YAML, escribe en Silver.
    Retorna el número de filas escritas.
    """
    nombre        = tabla_config["name"]
    fuente        = tabla_config["source"]
    destino       = tabla_config["destination"]
    transformaciones = tabla_config["transformations"]
    write_mode    = tabla_config.get("write_mode", "overwrite")

    print(f"\nTransformando: {fuente} → {destino}")

    # 1. Leer fuente Bronze
    df = spark.table(fuente)
    print(f"  Filas leídas de {fuente}: {df.count():,}")

    # 2. Aplicar transformaciones en orden
    df = aplicar_renames(df, transformaciones.get("renames", []))
    df = aplicar_casts(df, transformaciones.get("casts", []))
    df = aplicar_columnas_derivadas(df, transformaciones.get("derived_columns", []))
    df = aplicar_drops(df, transformaciones.get("drops", []))
    df = agregar_metadata(df, transformaciones.get("add_metadata", []))

    # 3. Crear schema destino y escribir
    schema_destino = destino.split(".")[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_destino}")

    df.write.format("delta").mode(write_mode).saveAsTable(destino)

    n_filas = df.count()
    print(f"  Escrito en {destino}: {n_filas:,} filas")
    return n_filas
```

Ejecuta para la tabla `transactions`:
```python
tabla_transactions = silver_config["tables"][0]
n = transformar_tabla_silver(tabla_transactions)
print(f"\nTotal filas Silver transactions: {n:,}")
```

Verifica:
```python
spark.table("silver.transactions").select(
    "transaction_id", "user_id", "amount", "transaction_date",
    "hora", "dia_semana", "es_fin_de_semana", "amount_abs",
    "_processed_at", "_source_table"
).show(10)
```

---

## Parte 7 — Loop sobre todas las tablas Silver

```python
resultados_silver = []

for tabla_config in silver_config["tables"]:
    try:
        n_filas = transformar_tabla_silver(tabla_config)
        resultados_silver.append({"tabla": tabla_config["name"], "filas": n_filas, "estado": "OK"})
    except Exception as e:
        resultados_silver.append({"tabla": tabla_config["name"], "filas": 0, "estado": f"ERROR: {e}"})

print("\nRESUMEN SILVER:")
for r in resultados_silver:
    print(f"  {r['tabla']:20s} → {r['filas']:>10,} filas | {r['estado']}")
```

---

## Parte 8 — Crear el Databricks Job (Bronze → Silver)

Ahora que tienes los notebooks de Bronze (Actividad 03) y Silver funcionando, vas a crear un Job de Databricks que los encadena. Un Job es la forma en que Databricks ejecuta pipelines automatizadas — con schedule, dependencias, reintentos y logs.

### 8.1 Crear el Job desde la interfaz

1. En la barra lateral de Databricks, ve a **Workflows → Jobs**
2. Haz clic en **Create Job**
3. Ponle nombre: `financial_transactions_pipeline_<tu-nombre>`

### 8.2 Configurar Task 1 — Bronze

| Campo | Valor |
|-------|-------|
| Task name | `bronze_ingestion` |
| Type | Notebook |
| Source | Workspace |
| Path | Path a tu orquestador de Actividad 03 |
| Cluster | Single node (small) o tu cluster existente |
| Parameters | `environment = dev` |

### 8.3 Configurar Task 2 — Silver

| Campo | Valor |
|-------|-------|
| Task name | `silver_transform` |
| Type | Notebook |
| Source | Workspace |
| Path | Path a tu notebook Silver de esta actividad |
| Depends on | `bronze_ingestion` |
| Cluster | Mismo cluster que Bronze |
| Parameters | `environment = dev` |

> **Depende on** es la clave. Si Bronze falla, Silver no se ejecuta. Databricks garantiza el orden sin que tengas que escribir lógica de espera.

### 8.4 Ejecutar el Job

Haz clic en **Run now** y observa el DAG de ejecución. Mientras corre, haz clic en cada task para ver sus logs.

Cuando termine, captura una screenshot del DAG con ambas tasks en verde (éxito) e inclúyela en tu PR.

### 8.5 Exportar la definición del Job como JSON

Una práctica importante es versionar la definición del Job en Git (Infrastructure as Code).

En la interfaz del Job, ve a `...` → **Export Job JSON** y guarda el JSON en:
```
semana_04/actividades/actividad_04/<tu-nombre>/job_definition.json
```

Agrega ese archivo al commit. Así el Job puede recrearse desde el repo sin configuración manual.

---

## Parte 9 — Reflexión

En tu notebook de reflexión o como comentario en el PR:

1. ¿Qué ocurre si la tarea `silver_transform` falla y la vuelves a correr manualmente? ¿Bronze se vuelve a ejecutar?

2. En el YAML Silver, las expresiones están escritas en Spark SQL (`hour(transaction_date)`, `regexp_replace(...)`). ¿Qué pasaría si una expresión tiene un error de sintaxis? ¿Cuándo lo detectarías?

3. El diseño actual aplica las transformaciones en un orden fijo: `renames → casts → derived → drops → metadata`. ¿Importa ese orden? ¿Qué pasa si aplicas `drops` antes que `derived_columns` cuando una columna derivada depende de una columna que vas a eliminar?

4. ¿Qué ventajas tiene exportar el Job JSON al repositorio?

---

## Parte 10 — Carga incremental con MERGE INTO

Hasta ahora todas tus escrituras son `overwrite`. En producción eso significa reprocesar 50 millones de filas cada noche aunque solo cambiaron 50 mil. `MERGE INTO` de Delta Lake resuelve eso: actualiza lo que cambió, inserta lo nuevo, deja intacto el resto.

### ¿Qué hace MERGE INTO?

```
FUENTE (filas nuevas/modificadas)     DESTINO (tabla existente)
┌──────────────────┐              ┌───────────────────────┐
│ tx_id=1 amount=50│──match─▶│ tx_id=1 amount=40     │ → UPDATE
│ tx_id=99 amount=30│─nuevo─▶│ tx_id=2 amount=20     │ → sin cambio
└──────────────────┘              │ tx_id=99 [nuevo]      │ → INSERT
                                 └───────────────────────┘
```

### SCD1 con MERGE (sobrescribir el valor actual)

SCD1 = "Solo me importa el valor más reciente." Si el usuario cambia de ciudad: actualizar. Si es usuario nuevo: insertar.

```python
from pyspark.sql.functions import current_timestamp

# Simular un batch incremental de usuarios nuevos o actualizados
df_usuarios_nuevos = spark.table("bronze.users").limit(500)
df_usuarios_nuevos.createOrReplaceTempView("usuarios_source")

# Crear tabla destino incremental si no existe
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.users_scd1
    USING DELTA
    AS SELECT * FROM silver.users WHERE 1=0
""")

# MERGE SCD1: UPDATE * si existe, INSERT * si es nuevo
spark.sql("""
    MERGE INTO silver.users_scd1 AS destino
    USING usuarios_source AS fuente
    ON destino.user_id = fuente.user_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"Registros en silver.users_scd1: {spark.table('silver.users_scd1').count():,}")
spark.sql("DESCRIBE HISTORY silver.users_scd1").show(5, False)
```

### MERGE de transacciones: UPDATE condicional

```python
# Simular batch de transacciones con posibles correcciones de monto
df_nuevas = spark.table("bronze.transactions").limit(1000)
df_nuevas.createOrReplaceTempView("tx_source")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.transactions_incremental
    USING DELTA
    AS SELECT * FROM silver.transactions WHERE 1=0
""")

spark.sql("""
    MERGE INTO silver.transactions_incremental AS destino
    USING tx_source AS fuente
    ON destino.transaction_id = fuente.id
    WHEN MATCHED AND destino.amount != cast(regexp_replace(fuente.amount, '[$,]', '') AS DOUBLE) THEN
        UPDATE SET destino.amount = cast(regexp_replace(fuente.amount, '[$,]', '') AS DOUBLE),
                   destino._processed_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, user_id, amount, transaction_date, _processed_at)
        VALUES (fuente.id, fuente.client_id,
                cast(regexp_replace(fuente.amount, '[$,]', '') AS DOUBLE),
                to_timestamp(fuente.date, 'yyyy-MM-dd HH:mm:ss'),
                current_timestamp())
""")
```

Documenta: ¿cuánto tarda el MERGE vs un `write.mode("overwrite")` con el mismo volumen? ¿Cuántos registros fueron UPDATE vs INSERT? (lo ves en el output del MERGE).

> **Enlace con semana 05:** `APPLY CHANGES INTO` en Declarative Pipelines hace SCD1 y SCD2 automáticamente con solo declarar las keys y el campo de secuencia — sin escribir el SQL del MERGE. Es el motivo por el que DLT existe.

---

## Entrega en Git

```bash
git add semana_04/actividades/actividad_04/<tu-nombre>/
git commit -m "feat: metadata-driven silver + databricks job bronze→silver - <tu-nombre>"
git push origin feature/semana04-widgets-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 04] Silver YAML-driven + Databricks Job — <Tu Nombre>
```

Incluye en el PR:
- Screenshot del Job corriendo exitosamente (ambas tasks en verde)
- El JSON de definición del Job
- Respuestas a las preguntas de reflexión

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Funciones de transformación implementadas | `renames`, `casts`, `derived`, `drops`, `metadata` | 30% |
| Pipeline `transformar_tabla_silver()` funciona | Transactions Silver creada correctamente | 25% |
| Loop sobre todas las tablas del YAML | Con manejo de errores por tabla | 10% |
| Job creado con 2 tasks encadenadas | Bronze → Silver con `depends_on` | 20% |
| Job ejecutado exitosamente | Screenshot en el PR | 10% |
| job_definition.json versionado en el repo | Archivo exportado y commiteado | 5% |

---

## Referencias

- [Databricks Jobs — Create and manage](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html)
- [Task dependencies in Jobs](https://docs.databricks.com/en/workflows/jobs/job-task-types.html)
- [pyspark.sql.functions.expr()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html)
- [Export Job JSON via API](https://docs.databricks.com/en/workflows/jobs/jobs-api-updates.html)
