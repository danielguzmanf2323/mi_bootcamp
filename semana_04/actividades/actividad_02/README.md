# Actividad 02 — Semana 04: El notebook genérico de Bronze

**Semana:** 04  
**Tema:** Diseñar un solo notebook que ingesta cualquier fuente de datos  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
**Prerequisito:** Actividad 01 completada (dbutils.widgets)

---

## Contexto

En la Actividad 01 creaste un notebook que parametriza la ingesta de `transactions_data.csv`. Bien, pero todavía tienes el problema inverso: si necesitas ingestar `users_data.csv`, `cards_data.csv` y `mcc_codes.json`, necesitas tres notebooks casi idénticos.

La pregunta correcta no es *¿cómo parametrizo este notebook?* sino *¿puedo tener un solo notebook que ingeste cualquier archivo?*

La respuesta es sí, y en esta actividad lo construyes.

---

## Objetivo

Crear un notebook reutilizable (`bronze_generico`) que:

1. Recibe **todos** sus parámetros via `dbutils.widgets`
2. Soporta múltiples formatos: `csv`, `json`, `parquet`, `delta`
3. Crea el schema de destino si no existe
4. Escribe en Delta Lake con el modo especificado
5. Retorna un resultado legible via `dbutils.notebook.exit()`
6. Funciona correctamente cuando se invoca desde otro notebook O directamente desde la interfaz

---

## Material de estudio previo

- ¿Cuál es la diferencia entre `inferSchema` y un schema explícito? ¿Cuándo conviene cada uno?
- ¿Qué es el modo `merge` (MERGE INTO) en Delta Lake? ¿Por qué no lo implementaremos hoy?
- ¿Qué información registra Delta Lake en el Transaction Log?
- ¿Qué hace `DESCRIBE HISTORY <tabla>`?

---

## Instrucciones Git

```bash
git checkout feature/semana04-widgets-<tu-nombre>  # o tu rama actual de semana 04
```

Crea tu notebook en:
```
semana_04/actividades/actividad_02/<tu-nombre>/bronze_generico.py
```

---

## Parte 1 — Diseño de la interfaz del notebook

Un notebook genérico bien diseñado expone exactamente los parámetros que necesita, con docstrings claros. Empieza definiendo la interfaz como comentario:

```python
# =============================================================================
# NOTEBOOK: bronze_generico
# PROPÓSITO: Ingestar cualquier archivo de datos en la capa Bronze (Delta Lake)
#
# PARÁMETROS (via dbutils.widgets):
#   source_path    : str  — Ruta al archivo fuente (Volumes, FileStore, DBFS)
#   format         : str  — Formato: csv | json | parquet | delta
#   schema_name    : str  — Schema de destino (ej: bronze, bronze_dev)
#   table_name     : str  — Nombre de la tabla Delta de destino
#   write_mode     : str  — Modo: overwrite | append
#
# RETORNA (via dbutils.notebook.exit):
#   str — "OK: <schema>.<tabla> | <n> filas"
#   str — "ERROR: <mensaje>" si falla
#
# VERSIÓN: 1.0.0
# =============================================================================
```

Esto convierte el notebook en una función autodocumentada. Cualquier persona puede leer el encabezado y saber cómo usarlo.

---

## Parte 2 — Definir y leer los widgets

```python
dbutils.widgets.removeAll()

dbutils.widgets.text(
    name="source_path",
    defaultValue="/Volumes/main/landing/raw/transactions_data.csv",
    label="Ruta del archivo fuente"
)
dbutils.widgets.dropdown(
    name="format",
    defaultValue="csv",
    choices=["csv", "json", "parquet", "delta"],
    label="Formato"
)
dbutils.widgets.combobox(
    name="schema_name",
    defaultValue="bronze",
    choices=["bronze", "bronze_dev", "silver", "gold"],
    label="Schema de destino"
)
dbutils.widgets.text(
    name="table_name",
    defaultValue="transactions",
    label="Tabla de destino"
)
dbutils.widgets.dropdown(
    name="write_mode",
    defaultValue="overwrite",
    choices=["overwrite", "append"],
    label="Modo de escritura"
)
```

```python
# Leer todos los parámetros
source_path = dbutils.widgets.get("source_path")
file_format = dbutils.widgets.get("format")
schema_name = dbutils.widgets.get("schema_name")
table_name  = dbutils.widgets.get("table_name")
write_mode  = dbutils.widgets.get("write_mode")

full_table  = f"{schema_name}.{table_name}"

print("=" * 60)
print("BRONZE GENERICO — Parámetros recibidos")
print(f"  source_path : {source_path}")
print(f"  format      : {file_format}")
print(f"  destino     : {full_table}")
print(f"  write_mode  : {write_mode}")
print("=" * 60)
```

---

## Parte 3 — Crear el schema si no existe

```python
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
print(f"Schema '{schema_name}' listo.")
```

> Este paso es crítico y suele olvidarse. Si el schema no existe, el `saveAsTable` falla con un error críptico. Crear el schema con `IF NOT EXISTS` es idempotente — ejecutarlo múltiples veces no causa problemas.

---

## Parte 4 — Leer el archivo según el formato

Aquí está la lógica central del notebook genérico: una función que sabe cómo leer cada formato.

```python
def leer_fuente(source_path: str, file_format: str):
    """
    Lee un archivo desde el path dado en el formato especificado.
    Retorna un DataFrame de Spark.
    """
    if file_format == "csv":
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .load(source_path)
        )

    elif file_format == "json":
        df = (
            spark.read
            .format("json")
            .option("multiLine", "true")
            .load(source_path)
        )

    elif file_format == "parquet":
        df = spark.read.format("parquet").load(source_path)

    elif file_format == "delta":
        df = spark.read.format("delta").load(source_path)

    else:
        raise ValueError(f"Formato no soportado: '{file_format}'. Use: csv, json, parquet, delta")

    return df
```

```python
df = leer_fuente(source_path, file_format)
print(f"Filas leídas    : {df.count():,}")
print(f"Columnas ({len(df.columns)}): {df.columns}")
df.printSchema()
```

---

## Parte 5 — Agregar metadatos de auditoría

Una buena capa Bronze preserva los datos originales y agrega columnas de auditoría que permiten rastrear de dónde vino cada fila y cuándo fue procesada.

```python
from pyspark.sql.functions import current_timestamp, lit, input_file_name

df_con_metadata = (
    df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_path",  lit(source_path))
    .withColumn("_source_format", lit(file_format))
)

print("Columnas de metadatos agregadas: _ingested_at, _source_path, _source_format")
```

> `_ingested_at` responde "¿cuándo corrió esta pipeline?". Si algo falla en producción, puedes ver exactamente qué batch procesó cada fila.

---

## Parte 6 — Escribir en Delta Lake

```python
(
    df_con_metadata
    .write
    .format("delta")
    .mode(write_mode)
    .saveAsTable(full_table)
)

print(f"\nTabla escrita: {full_table}")
print(f"Modo usado:    {write_mode}")
```

Verificar que la tabla quedó bien:

```python
df_verificado = spark.table(full_table)
print(f"Filas en {full_table}: {df_verificado.count():,}")

# En Databricks: display() es preferible a show() — tabla interactiva, paginado, exportable
display(df_verificado)
# Alternativa portable: df_verificado.show(5, truncate=True)
```

Ver el historial de cambios Delta:

```python
spark.sql(f"DESCRIBE HISTORY {full_table}").show(5, truncate=False)
```

Documentar qué campos aparecen en `DESCRIBE HISTORY`. ¿Cuántas versiones hay después de correr el notebook dos veces con `overwrite`?

---

## Parte 7 — Retornar un resultado

```python
# Contar filas finales para el resultado
n_filas = df_con_metadata.count()

# Mensaje de éxito — será visible en el orquestador o en el Job
resultado = f"OK: {full_table} | {n_filas:,} filas | modo={write_mode}"
print(resultado)

dbutils.notebook.exit(resultado)
```

> En la Actividad 01 viste que `dbutils.notebook.exit()` devuelve el valor al notebook que hizo `dbutils.notebook.run()`. En un Job de Databricks, este valor queda registrado en los logs de la tarea. Siempre retorna algo informativo.

---

## Parte 8 — Probar el notebook con todas las fuentes

Ejecuta el notebook **manualmente** desde la interfaz de Databricks, cambiando los widgets para procesar cada una de las 5 fuentes del dataset. Verifica que las tablas aparecen en el catálogo.

| source_path | format | schema_name | table_name | write_mode |
|-------------|--------|-------------|------------|------------|
| `/Volumes/main/landing/raw/transactions_data.csv` | csv | bronze | transactions | overwrite |
| `/Volumes/main/landing/raw/users_data.csv` | csv | bronze | users | overwrite |
| `/Volumes/main/landing/raw/cards_data.csv` | csv | bronze | cards | overwrite |
| `/Volumes/main/landing/raw/mcc_codes.json` | json | bronze | mcc_codes_raw | overwrite |
| `/Volumes/main/landing/raw/train_fraud_labels.json` | json | bronze | fraud_labels | overwrite |

> **Tarea adicional obligatoria — Pivotear `mcc_codes`:**
>
> El archivo `mcc_codes.json` tiene una estructura de objeto plano (clave = código MCC, valor = descripción de la categoría). Cuando Spark lo lee, el resultado **no es una tabla usable** directamente — las columnas son los propios códigos MCC, lo que hace imposible hacer JOINs con la tabla de transacciones.
>
> Tu tarea es transformar esa tabla cruda en una tabla con esta estructura:
>
> | mcc_code | description |
> |----------|-------------|
> | 0742     | Veterinary Services |
> | 0763     | Agricultural Supplies |
> | ...      | ... |
>
> **No se te da el código** — investiga cómo transformar un DataFrame con columnas dinámicas en filas. Pistas: busca funciones de Spark que trabajen con schemas dinámicos, `stack()`, o cómo transponer/despivotar un DataFrame en PySpark.
>
> Guarda el resultado como tabla `bronze.mcc_codes` (sin el sufijo `_raw`). Este será el formato que se usará en actividades posteriores para hacer JOIN con las transacciones.

Para cada ejecución, guarda en tu notebook de pruebas:

```python
# Verificación post-ingesta
# En Databricks usa display() para ver resultados en tabla interactiva
display(spark.sql("SHOW TABLES IN bronze"))

# Alternativa portable (cualquier entorno Spark):
# spark.sql("SHOW TABLES IN bronze").show(truncate=False)
```

> **Nota:** `toPandas()` convierte el resultado a un DataFrame de pandas — esto trae todos los datos al driver y rompe el modelo distribuido. Desde semana 01 pandas quedó fuera del stack. En Databricks usa `display()` para una vista interactiva; si el notebook puede ejecutarse fuera de Databricks usa `show()`. Nunca uses `toPandas()` en pipelines de producción.

---

## Parte 9 — Reflexión sobre el diseño

En un notebook aparte (`reflexion_<tu-nombre>.py`), responde:

1. ¿Qué pasaría si el archivo CSV tiene un encoding distinto a UTF-8? ¿Cómo lo manejarías sin cambiar el código del notebook genérico?

2. El notebook actual usa `inferSchema=true` para CSV. ¿Cuál es el riesgo de eso en producción? Propón una solución.

3. Si quisieras agregar soporte para `write_mode="merge"` (MERGE INTO de Delta), ¿qué parámetros adicionales necesitarías? Diseña la interfaz de widgets que necesitarías agregar.

4. ¿Por qué es importante que el notebook retorne algo via `dbutils.notebook.exit()` cuando lo llamarás desde un Job?

---

## Parte 10 — Tipos de cluster en Databricks Enterprise

Antes de enviar este notebook a producción, necesitas entender qué cluster está corriendo tu código — porque afecta el costo, el tiempo de arranque y las capacidades disponibles.

### Los tipos principales

| Tipo | Cuándo usarlo | Costo | Arranque |
|------|--------------|-------|----------|
| **Single Node** | Desarrollo, datasets pequeños, scripts simples | Bajo | ~1 min |
| **Standard (Multi-node)** | Pipelines interactivos, exploración con datos medianos | Medio | ~2-3 min |
| **Shared** | Múltiples usuarios en paralelo, acceso centralizado | Medio | Compartido |
| **Job Cluster** | Tasks automatizadas en Jobs — nace con el Job, muere al terminar | El más eficiente | ~2 min |
| **Serverless** | Pipelines DLT y Jobs sin configuración de workers | Variable | ~5-10 seg |

```python
# Ver el tipo de cluster donde corre este notebook
# ⚠️  ADVERTENCIA: spark.databricks.clusterUsageTags.clusterNodeType NO está disponible
#     en Serverless Compute — lanzará un error si lo intentas en ese tipo de cluster.
#     Usa la alternativa de abajo si no estás seguro del tipo de compute:
try:
    node_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")
    print(f"Tipo de nodo: {node_type}")
except Exception:
    print("Tipo de nodo: no disponible (probablemente Serverless Compute)")

print(f"Versión Spark:    {spark.version}")
print(f"Particiones SQL:  {spark.conf.get('spark.sql.shuffle.partitions')}")

# spark.sparkContext.defaultParallelism también falla en Serverless:
# En Serverless, el JVM del driver no es accesible directamente.
```

> **Serverless vs cluster dedicado:** si estás en Serverless Compute, `spark.databricks.clusterUsageTags.clusterNodeType` y `spark.sparkContext.*` lanzarán errores porque el driver JVM no es accesible. Usa `spark.version` y `spark.conf.get(...)` como alternativas seguras en cualquier tipo de compute.

### All-Purpose vs Job Cluster

```
All-Purpose Cluster (lo que usas ahora):
  Siempre activo → siempre pagando
  Ideal para: desarrollo, exploración interactiva, notebooks manuales
  Problema: si olvidas apagarlo, el costo corre

Job Cluster (para el Job que crearás en Act 04):
  Se crea al iniciar la task → se destruye al terminar
  No hay costo en idle
  Ideal para: pipelines automatizados nocturnos, batch processing
  Ahorro típico: 40–70% del costo de compute vs All-Purpose
```

### Serverless

Serverless Compute elimina la gestión de workers. No configuras cores ni memory — Databricks asigna recursos dinámicamente. El arranque es en segundos, no minutos.

Cuándo usar Serverless:
- DLT Pipelines (semana 05 — es la opción recomendada por Databricks)
- Jobs cortos y frecuentes donde el tiempo de arranque importa
- Cuando quieres olvidarte del cluster sizing

Cuándo NO usar Serverless:
- Necesitas configuración explícita de Spark (`shuffle.partitions`, broadcast threshold)
- Workloads con skew predecible que se beneficien de tuning manual
- Cuando el dataset necesita más memoria que la que Serverless asigna por defecto

Documenta en tu notebook de reflexión:
- ¿En qué tipo de cluster estás corriendo esta actividad?
- Si este notebook fuera un Job nocturno que corre a las 2am con 5GB de datos, ¿qué tipo de cluster elegirías? ¿Por qué?

---

## Entrega en Git

```bash
git add semana_04/actividades/actividad_02/<tu-nombre>/
git commit -m "feat: generic bronze notebook - multi-format, auditable, no hardcode - <tu-nombre>"
git push origin feature/semana04-widgets-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 04] Notebook Bronze genérico — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Encabezado de documentación | Parámetros, retorno y propósito documentados | 10% |
| Función `leer_fuente()` implementada | Soporta csv, json, parquet, delta con sus opciones | 20% |
| Metadatos de auditoría | Columnas `_ingested_at`, `_source_path`, `_source_format` | 15% |
| Schema creado con `IF NOT EXISTS` | Sin errores si corre dos veces | 10% |
| `dbutils.notebook.exit()` con mensaje informativo | Incluye tabla, filas y modo | 10% |
| Las 5 fuentes procesadas exitosamente | Tablas verificadas con `display()` o `.show()` | 15% |
| Pivot de `mcc_codes` implementado | Tabla `bronze.mcc_codes` con columnas `mcc_code` y `description` | 10% |
| Dataset subido al Volumen en la ruta correcta | `/default/<tu_nombre>/semana_04/<nombre_dataset>/` | 5% |
| Reflexión respondida | Reflexión con respuestas técnicas | 5% |

---

## Instrucciones de entrega en Databricks Volumes

Antes de hacer el PR, carga los archivos de datos que usaste al Volumen del entorno Databricks en la siguiente ruta:

```
/Volumes/main/default/<tu_nombre>/semana_04/<nombre_dataset>/
```

Ejemplo:
```
/Volumes/main/default/maria/semana_04/financial_transaction_dataset/
```

Esto forma parte de la calificación. El instructor verificará que los archivos estén accesibles desde esa ruta antes de correr tu notebook.

---

## Notas sobre la entrega en `.ipynb`

- **¿Debo dejar los resultados de `display()` y `show()` en el notebook?** Sí. Los outputs de las celdas (tablas, conteos, schemas) deben quedar visibles en el `.ipynb` que subes al repositorio. El instructor los revisa sin necesidad de ejecutar el notebook.
- **¿Puedo usar imágenes en archivos `.md`?** Sí. Puedes tomar capturas de pantalla de Databricks (por ejemplo, del Spark UI o del Catalog) y adjuntarlas en tu archivo `.md` de reflexión. Usa la sintaxis estándar de Markdown: `![descripción](ruta_imagen.png)`.

---

## Laboratorio

El notebook de laboratorio para esta actividad está en:
[`../../laboratorios/lab_02_bronze_generico.ipynb`](../../laboratorios/lab_02_bronze_generico.ipynb)

Cubre: notebook genérico multiformato (CSV/JSON/Parquet/Delta) parametrizado con widgets, quality gate de nulos antes de la ingestión, adición de columnas de auditoría `_ingest_timestamp / _source_file`, y verificación con `DESCRIBE HISTORY` tras escribir en Delta con distintos modos.

---

## Referencias

- [Delta Lake — Write modes](https://docs.delta.io/latest/delta-intro.html)
- [Databricks — Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [DESCRIBE HISTORY — Delta Lake](https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history)
