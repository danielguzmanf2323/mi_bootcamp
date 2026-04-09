# Actividad 03 — Semana 04: YAML como fuente de configuración — eliminando el último hardcode

**Semana:** 04  
**Tema:** Leer una pipeline desde un YAML — cero código para agregar una nueva fuente  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
**Prerequisito:** Actividad 02 completada (notebook Bronze genérico funcional)

---

## Contexto

En la Actividad 02 construiste un notebook genérico. Pero todavía existe un problema: para procesar las 5 fuentes del dataset, tienes que llamar a `dbutils.notebook.run()` 5 veces con los parámetros escritos a mano en el orquestador.

Si mañana se agrega una sexta fuente, alguien tiene que abrir el orquestador, localizar el bloque correcto y agregar el llamado a mano. Eso es un cambio de código para un cambio de datos. En producción, eso pasa por revisión de código, aprobación de PR y deploy. Demasiado para agregar un archivo CSV.

La solución es separar **qué** se procesa (datos de configuración) del **cómo** se procesa (código). El archivo YAML es la fuente de configuración. El notebook orquestador es el motor que la ejecuta. Para agregar una fuente: solo editas el YAML.

---

## Objetivo

Construir un orquestador que:

1. Lee `bronze_config.yml` desde el repositorio (sin valores quemados)
2. Por cada fuente definida en el YAML, invoca el notebook genérico de la Actividad 02
3. Registra el resultado de cada invocación
4. Soporta un widget `environment` (dev/prod) que cambia los schemas de destino

---

## Material de estudio previo

- ¿Qué es YAML? ¿En qué se diferencia de JSON?
- ¿Cómo leer un archivo YAML en Python con `yaml.safe_load()`? ¿Por qué no usar `yaml.load()` sin `Loader`?
- ¿Cómo funciona el módulo `os.path` de Python para construir paths?
- ¿Cuál es el path de un archivo en un Databricks Repo? (`/Workspace/Repos/<user>/<repo>/...`)

---

## Instrucciones Git

```bash
git checkout feature/semana04-widgets-<tu-nombre>
```

Crea tus archivos en:
```
semana_04/actividades/actividad_03/<tu-nombre>/
```

---

## El archivo de configuración

El repositorio ya incluye el archivo de referencia en `semana_04/configs/bronze_config.yml`. Úsalo como fuente de datos de configuración. No copies el archivo — en Databricks Repos, el repo completo está montado en el workspace y puedes leer el YAML directamente desde el filesystem.

Revisa el archivo:

```
semana_04/configs/bronze_config.yml
```

Estructura que encontrarás:

```yaml
pipeline:
  name: financial_transactions_bronze
  catalog: main
  environment: dev

sources:
  - name: transactions
    description: "Transacciones financieras principales"
    source_path: "/Volumes/main/landing/raw/transactions_data.csv"
    format: csv
    options:
      header: "true"
      inferSchema: "true"
    destination:
      schema: bronze
      table: transactions
      write_mode: overwrite

  # ... y 4 fuentes más
```

---

## Parte 1 — Resolver el path al YAML desde Databricks

Para leer el YAML, el notebook necesita saber su propia ubicación en el workspace. Databricks proporciona el contexto de ejecución via `dbutils`.

```python
# Obtener el path del notebook actual en el workspace
notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
print(f"Este notebook está en: {notebook_path}")
# Ejemplo: /Repos/user@inetum.com/inetum_data_engineer_bootcamp/semana_04/actividades/actividad_03/...
```

```python
import os

# Subir al root del repositorio (5 niveles: nombre_notebook, actividad_03, actividades, semana_04, repo)
# Ajusta el número de `os.path.dirname()` según la profundidad de tu notebook
repo_root = os.path.dirname(  # semana_04/
             os.path.dirname(  # actividades/
             os.path.dirname(  # actividad_03/
             os.path.dirname(  # <tu-nombre>/
             notebook_path))))

# Prefijo del workspace — en Databricks Repos el path comienza con /Workspace
config_path = f"/Workspace{repo_root}/semana_04/configs/bronze_config.yml"
print(f"Path al YAML: {config_path}")
```

> Alternativa más robusta: recibir el `repo_root` como widget. Esto hace el notebook portable sin depender de la profundidad de anidación.

---

## Parte 2 — Leer el YAML con PyYAML

```python
import yaml

with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

print(f"Pipeline: {config['pipeline']['name']}")
print(f"Entorno en config: {config['pipeline']['environment']}")
print(f"Número de fuentes: {len(config['sources'])}")
```

```python
# Inspeccionar la primera fuente
primera = config["sources"][0]
print(f"\nPrimera fuente:")
print(f"  name:        {primera['name']}")
print(f"  source_path: {primera['source_path']}")
print(f"  format:      {primera['format']}")
print(f"  destino:     {primera['destination']['schema']}.{primera['destination']['table']}")
```

Documenta qué tipo de objeto Python devuelve `yaml.safe_load()`. ¿Es un dict? ¿Una lista? ¿Cómo accedes a campos anidados?

---

## Parte 3 — Widget de entorno para ambientes dev/prod

El entorno no debe estar quemado en el YAML tampoco. El YAML define la configuración base (esquema de datos, paths), pero el entorno lo inyecta quien dispara la pipeline.

```python
dbutils.widgets.removeAll()

dbutils.widgets.dropdown(
    name="environment",
    defaultValue="dev",
    choices=["dev", "prod"],
    label="Entorno de ejecución"
)

environment = dbutils.widgets.get("environment")
print(f"Entorno seleccionado: {environment}")
```

```python
# Mapa de schemas por entorno
SCHEMA_MAP = {
    "dev":  {"bronze": "bronze_dev",  "silver": "silver_dev",  "gold": "gold_dev"},
    "prod": {"bronze": "bronze",      "silver": "silver",      "gold": "gold"},
}

schemas = SCHEMA_MAP[environment]
print(f"Schemas activos para '{environment}':")
for capa, schema in schemas.items():
    print(f"  {capa} → {schema}")
```

---

## Parte 4 — Loop sobre las fuentes del YAML

```python
# Path al notebook genérico de Actividad 02 (ajusta con tu usuario)
BRONZE_NOTEBOOK = (
    f"/Workspace{repo_root}/semana_04/actividades/actividad_02/<tu-nombre>/bronze_generico"
)
print(f"Notebook genérico: {BRONZE_NOTEBOOK}")
```

```python
resultados = []

for source in config["sources"]:
    nombre_tabla = source["name"]
    schema_base  = source["destination"]["schema"]  # ej: "bronze"
    schema_real  = schemas.get(schema_base, schema_base)  # mapear según entorno

    parametros = {
        "source_path": source["source_path"],
        "format":      source["format"],
        "schema_name": schema_real,
        "table_name":  source["destination"]["table"],
        "write_mode":  source["destination"]["write_mode"],
    }

    print(f"\nProcesando: {nombre_tabla} → {schema_real}.{source['destination']['table']}")
    print(f"  Parámetros: {parametros}")

    resultado = dbutils.notebook.run(
        path=BRONZE_NOTEBOOK,
        timeout_seconds=600,
        arguments=parametros
    )

    resultados.append({
        "fuente":    nombre_tabla,
        "destino":   f"{schema_real}.{source['destination']['table']}",
        "resultado": resultado,
    })
    print(f"  → {resultado}")
```

```python
# Resumen final
print("\n" + "=" * 60)
print("RESUMEN DE EJECUCIÓN")
print("=" * 60)
for r in resultados:
    estado = "OK" if r["resultado"].startswith("OK") else "ERROR"
    print(f"  [{estado}] {r['fuente']:20s} → {r['destino']:30s}")
print("=" * 60)
print(f"Total: {len(resultados)} fuentes procesadas | Entorno: {environment}")
```

---

## Parte 5 — Agregar una sexta fuente (solo en el YAML)

Esta es la prueba definitiva del diseño metadata-driven.

**Escenario:** el equipo de datos te entrega un nuevo archivo `merchants_data.csv` con información de comercios. Tienes que ingestarlo en la capa Bronze.

**Lo que NO debes hacer:** abrir el notebook orquestador y agregar código.

**Lo que DEBES hacer:** editar `semana_04/configs/bronze_config.yml` y agregar:

```yaml
  - name: merchants
    description: "Datos de comercios participantes"
    source_path: "/Volumes/main/landing/raw/merchants_data.csv"
    format: csv
    options:
      header: "true"
      inferSchema: "true"
    destination:
      schema: bronze
      table: merchants
      write_mode: overwrite
```

> Nota: no necesitas el archivo real para probar el diseño. Agrega la entrada al YAML y verifica que el orquestador la detecta. La ejecución fallará porque el archivo no existe — pero eso es esperado y correcto. El punto es que el código no cambió.

Verifica:
```python
# Después de editar el YAML, vuelve a ejecutar la celda de lectura
with open(config_path, "r", encoding="utf-8") as f:
    config_nuevo = yaml.safe_load(f)
print(f"Fuentes en el nuevo YAML: {len(config_nuevo['sources'])}")
# Debe mostrar 6
```

---

## Parte 6 — Manejo de errores por fuente

¿Qué pasa si uno de los notebooks falla? Sin manejo de errores, el loop completo se detiene.

```python
import traceback

resultados = []

for source in config["sources"]:
    nombre_tabla = source["name"]
    schema_base  = source["destination"]["schema"]
    schema_real  = schemas.get(schema_base, schema_base)

    parametros = {
        "source_path": source["source_path"],
        "format":      source["format"],
        "schema_name": schema_real,
        "table_name":  source["destination"]["table"],
        "write_mode":  source["destination"]["write_mode"],
    }

    try:
        resultado = dbutils.notebook.run(
            path=BRONZE_NOTEBOOK,
            timeout_seconds=600,
            arguments=parametros
        )
        estado = "OK"
    except Exception as e:
        resultado = f"ERROR: {str(e)[:200]}"
        estado    = "FALLO"

    resultados.append({
        "fuente":    nombre_tabla,
        "destino":   f"{schema_real}.{source['destination']['table']}",
        "resultado": resultado,
        "estado":    estado,
    })

# Si alguno falló, reportar pero no detener el proceso completo
fallidos = [r for r in resultados if r["estado"] == "FALLO"]
if fallidos:
    print(f"\nATENCIÓN: {len(fallidos)} fuente(s) fallaron:")
    for r in fallidos:
        print(f"  {r['fuente']}: {r['resultado']}")
```

Documenta: ¿es correcto continuar procesando si una fuente falla? ¿Cuándo sí y cuándo no?

---

## Entrega en Git

```bash
git add semana_04/actividades/actividad_03/<tu-nombre>/
git add semana_04/configs/bronze_config.yml   # si agregaste la sexta fuente
git commit -m "feat: yaml-driven bronze orchestrator - metadata-driven ingestion - <tu-nombre>"
git push origin feature/semana04-widgets-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 04] Orquestador YAML Bronze — <Tu Nombre>
```

En el PR:
- Muestra el output del resumen final de ejecución (screenshot o texto copiado)
- ¿Cuántas líneas de código tuviste que cambiar para agregar la sexta fuente?
- ¿Qué ventaja tiene que el entorno sea un widget y no una variable en el YAML?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Path al YAML resuelto dinámicamente | Sin el path absoluto quemado | 15% |
| YAML leído correctamente con `yaml.safe_load()` | Estructura accesible como dict Python | 15% |
| Widget de entorno con schema_map | Schemas dev/prod correctamente mapeados | 20% |
| Loop funcional sobre todas las fuentes | 5 tablas Bronze creadas | 30% |
| Manejo de errores por fuente | El loop no se rompe si una fuente falla | 10% |
| Sexta fuente solo en YAML | Sin cambios de código en el orquestador | 10% |

---

## Referencias

- [PyYAML Documentation](https://pyyaml.org/wiki/PyYAMLDocumentation)
- [Databricks Repos — File access](https://docs.databricks.com/en/repos/repos-setup.html)
- [dbutils.notebook.run — Arguments](https://docs.databricks.com/en/dev-tools/databricks-utils.html#run-command-dbutilsnotebookrun)
