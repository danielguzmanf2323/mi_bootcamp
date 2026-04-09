# Actividad 01 — Semana 04: dbutils.widgets — Eliminar el código quemado

**Semana:** 04  
**Tema:** `dbutils.widgets` — parametrizar notebooks para que funcionen en cualquier contexto  
**Nivel:** Junior–Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise

---

## Contexto

En semanas 02 y 03 escribiste código como este:

```python
df = spark.read.format("csv").load("/FileStore/transactions_data.csv")
df.write.saveAsTable("bronze.transactions")
```

Eso funciona exactamente una vez, para exactamente ese archivo, en exactamente ese schema. Si mañana cambia el path, el nombre de la tabla o el entorno (dev/prod), alguien tiene que abrir el notebook y editar manualmente.

**Ese es el problema.** En producción, nadie edita código para cambiar un parámetro. Los parámetros le llegan al notebook desde fuera — desde un Job, desde otro notebook, desde una interfaz — sin tocar el código.

`dbutils.widgets` es el mecanismo de Databricks para hacer exactamente eso.

---

## Material de estudio previo

- ¿Qué es `dbutils` en Databricks? ¿Qué otros módulos tiene además de `widgets`?
- ¿Qué tipos de widgets existen? (`text`, `dropdown`, `combobox`, `multiselect`)
- ¿Cómo se pasan parámetros a un notebook cuando se ejecuta como tarea de un Job?
- ¿Qué es el principio de "no hardcoding" y por qué importa en pipelines de datos?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana04-widgets-<tu-nombre>
```

Crea tu carpeta:
```
semana_04/actividades/actividad_01/<tu-nombre>/
```

---

## Parte 1 — Los 4 tipos de widget

Crea un notebook `widgets_tipos_<tu-nombre>.py` con estas celdas:

```python
# Limpiar widgets anteriores antes de definir nuevos
dbutils.widgets.removeAll()

# 1. TEXT — valor libre de texto
dbutils.widgets.text(
    name="source_path",
    defaultValue="/Volumes/main/landing/raw/transactions_data.csv",
    label="Ruta del archivo fuente"
)

# 2. DROPDOWN — selección única de lista fija
dbutils.widgets.dropdown(
    name="format",
    defaultValue="csv",
    choices=["csv", "json", "parquet", "delta"],
    label="Formato del archivo"
)

# 3. COMBOBOX — selección de lista O valor libre
dbutils.widgets.combobox(
    name="schema_name",
    defaultValue="bronze",
    choices=["bronze", "silver", "gold"],
    label="Schema de destino"
)

# 4. MULTISELECT — selección múltiple
dbutils.widgets.multiselect(
    name="tablas_a_procesar",
    defaultValue="transactions",
    choices=["transactions", "users", "cards", "mcc_codes", "fraud_labels"],
    label="Tablas a procesar"
)
```

```python
# Leer los valores seleccionados
source_path        = dbutils.widgets.get("source_path")
file_format        = dbutils.widgets.get("format")
schema_name        = dbutils.widgets.get("schema_name")
tablas_raw         = dbutils.widgets.get("tablas_a_procesar")

# multiselect devuelve un string separado por comas — hay que parsear
tablas_a_procesar  = [t.strip() for t in tablas_raw.split(",")]

print(f"source_path:        {source_path}")
print(f"format:             {file_format}")
print(f"schema_name:        {schema_name}")
print(f"tablas_a_procesar:  {tablas_a_procesar}")
```

Ejecuta el notebook. Cambia los valores en los widgets de la interfaz y observa cómo cambia el output sin tocar código.

Documenta en markdown:
- ¿Cuándo usarías `dropdown` vs `combobox`?
- ¿Qué devuelve `multiselect` cuando seleccionas más de un valor? ¿Cómo lo procesas?

---

## Parte 2 — Parámetros con valores por defecto útiles

El diseño de los valores por defecto importa. Un widget mal diseñado obliga al usuario a recordar el formato correcto.

```python
dbutils.widgets.removeAll()

# Widget de entorno — controla a qué catalog/schema va la pipeline
dbutils.widgets.dropdown(
    name="environment",
    defaultValue="dev",
    choices=["dev", "prod"],
    label="Entorno"
)

# Widget de tabla destino — con nombre sugerido
dbutils.widgets.text(
    name="table_name",
    defaultValue="transactions",
    label="Nombre de tabla destino"
)

# Widget de modo de escritura
dbutils.widgets.dropdown(
    name="write_mode",
    defaultValue="overwrite",
    choices=["overwrite", "append", "merge"],
    label="Modo de escritura"
)

# Lógica que usa el entorno para decidir el schema completo
environment  = dbutils.widgets.get("environment")
table_name   = dbutils.widgets.get("table_name")
write_mode   = dbutils.widgets.get("write_mode")

# El schema cambia según el entorno
schema_map = {
    "dev":  "bronze_dev",
    "prod": "bronze"
}
target_schema = schema_map[environment]
full_table_name = f"{target_schema}.{table_name}"

print(f"Entorno:      {environment}")
print(f"Tabla final:  {full_table_name}")
print(f"Modo:         {write_mode}")
```

> **Patrón clave:** el widget recibe `dev` o `prod` y el código mapea eso a un schema real. Nunca expongas nombres de schema directamente como widget — podrías escribir en producción por error.

---

## Parte 3 — Reescribir un notebook de semana 02 con widgets

Toma el notebook Bronze de semana 02 (Actividad 04) y reescribe la ingesta de `transactions_data.csv` usando widgets para todos los valores que estaban quemados:

**Valores quemados que debes convertir a widget:**
- Path del archivo fuente
- Formato (`csv`)
- Nombre del schema destino (`bronze`)
- Nombre de la tabla destino (`transactions`)
- Modo de escritura (`overwrite`)

Resultado esperado: un notebook que al cambiarle los widgets ingesta cualquier archivo CSV en cualquier tabla, sin tocar una línea de código.

```python
dbutils.widgets.removeAll()

dbutils.widgets.text("source_path", "/Volumes/main/landing/raw/transactions_data.csv", "Ruta fuente")
dbutils.widgets.dropdown("format", "csv", ["csv", "json", "parquet"], "Formato")
dbutils.widgets.combobox("schema_name", "bronze", ["bronze", "silver", "gold"], "Schema destino")
dbutils.widgets.text("table_name", "transactions", "Tabla destino")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"], "Modo escritura")

# Leer
source_path = dbutils.widgets.get("source_path")
file_format = dbutils.widgets.get("format")
schema_name = dbutils.widgets.get("schema_name")
table_name  = dbutils.widgets.get("table_name")
write_mode  = dbutils.widgets.get("write_mode")

# Crear schema si no existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Leer el archivo
options = {"header": "true", "inferSchema": "true"} if file_format == "csv" else {}
df = spark.read.format(file_format).options(**options).load(source_path)

print(f"Filas leídas: {df.count():,}")
print(f"Columnas: {df.columns}")

# Escribir
df.write.format("delta").mode(write_mode).saveAsTable(f"{schema_name}.{table_name}")

print(f"\nTabla guardada: {schema_name}.{table_name}")
dbutils.notebook.exit(f"OK: {schema_name}.{table_name} | {df.count()} filas")
```

> Nota el `dbutils.notebook.exit()` al final. Cuando este notebook se ejecute como tarea de un Job, ese valor es el resultado que el orquestador puede leer.

Commit esperado:
```bash
git commit -m "feat: rewrite bronze ingestion with dbutils.widgets - no hardcoded values"
```

---

## Parte 4 — Pasar parámetros entre notebooks

En Databricks puedes ejecutar un notebook desde otro y pasarle parámetros. Esto es la base de la orquestación manual (antes de usar Jobs).

Crea un notebook `orchestrator_<tu-nombre>.py`:

```python
# Notebook orquestador — llama a otro notebook con parámetros dinámicos

# Path al notebook genérico (ajusta con tu usuario)
NOTEBOOK_PATH = "/Repos/<tu-usuario>/inetum_data_engineer_bootcamp/semana_04/actividades/actividad_01/<tu-nombre>/bronze_generico"

# Lista de fuentes a procesar
fuentes = [
    {"source_path": "/Volumes/main/landing/raw/transactions_data.csv", "format": "csv",  "schema_name": "bronze", "table_name": "transactions",  "write_mode": "overwrite"},
    {"source_path": "/Volumes/main/landing/raw/users_data.csv",         "format": "csv",  "schema_name": "bronze", "table_name": "users",          "write_mode": "overwrite"},
    {"source_path": "/Volumes/main/landing/raw/mcc_codes.json",         "format": "json", "schema_name": "bronze", "table_name": "mcc_codes",      "write_mode": "overwrite"},
]

resultados = []
for fuente in fuentes:
    print(f"Procesando: {fuente['table_name']}...")
    resultado = dbutils.notebook.run(
        path=NOTEBOOK_PATH,
        timeout_seconds=600,
        arguments=fuente
    )
    resultados.append({"tabla": fuente["table_name"], "resultado": resultado})
    print(f"  → {resultado}")

print("\nResumen de ejecución:")
for r in resultados:
    print(f"  {r['tabla']}: {r['resultado']}")
```

Documenta: ¿qué pasa si uno de los notebooks falla? ¿Cómo capturarías ese error?

---

## Entrega en Git

```bash
git add semana_04/actividades/actividad_01/<tu-nombre>/
git commit -m "feat: widgets activity complete - parametrized notebooks - <tu-nombre>"
git push origin feature/semana04-widgets-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 04] dbutils.widgets — <Tu Nombre>
```

En el PR:
- ¿Cuántos valores quemados encontraste en el notebook Bronze de semana 02?
- ¿Qué diferencia hay entre `dropdown` y `combobox`?
- ¿Qué devuelve `dbutils.notebook.exit()` y para qué sirve?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Los 4 tipos de widget implementados | Con lectura de valores correcta | 20% |
| Patrón dev/prod con schema_map | Widget de entorno mapeado a schema real | 20% |
| Notebook Bronze reescrito sin hardcode | Todos los valores parametrizados | 35% |
| Orquestador con `dbutils.notebook.run()` | Mínimo 3 fuentes procesadas | 20% |
| Commits descriptivos | Mínimo 2 commits | 5% |

---

## Referencias

- [dbutils.widgets — Databricks](https://docs.databricks.com/en/dev-tools/databricks-utils.html#widgets-utility-dbutilswidgets)
- [dbutils.notebook.run — Databricks](https://docs.databricks.com/en/dev-tools/databricks-utils.html#run-command-dbutilsnotebookrun)
