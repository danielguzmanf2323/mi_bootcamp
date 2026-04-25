# Semana 04 — Pipelines Metadata-Driven y Databricks Jobs

**Tecnologías:** Databricks Jobs, dbutils.widgets, PyYAML, Delta Lake  
**Entorno:** Databricks Enterprise (Unity Catalog y Volumes requeridos)  
**Dataset:** Financial Transactions — archivos en `/Volumes/main/landing/raw/`

---

> **Convención obligatoria — entorno compartido**
>
> Todos los estudiantes del bootcamp trabajan sobre el mismo catálogo `default` en Databricks Enterprise.
> Para no sobrescribir las tablas de otras personas, **sufija siempre tu nombre en cada tabla que crees**:
>
> ```
> default.bronze.transactions_<tu_nombre>
> default.silver.transactions_<tu_nombre>
> ```
>
> En el notebook genérico de la semana, el sufijo va en el widget `table_name`.
> En el YAML de configuración, va en cada campo `table:` de tu copia personal del archivo.
> Si usas `mode("overwrite")` sin el sufijo, sobrescribirás el trabajo de otro estudiante.

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades guiadas — una por tema de la semana |
| [laboratorios/](laboratorios/) | 4 notebooks de práctica |
| [configs/](configs/) | Archivos de configuración YAML para pipelines metadata-driven |
| [documentos/](documentos/) | Material de referencia de la semana |
| [proyecto/](proyecto/) | Proyecto integrador: pipeline end-to-end orquestado |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | `dbutils.widgets` — parametrizar notebooks | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | Notebook Bronze genérico multi-formato | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | YAML como configuración — PyYAML, loop sin hardcode | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | Silver metadata-driven + Databricks Jobs | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Laboratorios

| Lab | Tema |
|-----|------|
| [lab_01_widgets.ipynb](laboratorios/lab_01_widgets.ipynb) | Práctica con dbutils.widgets |
| [lab_02_bronze_generico.ipynb](laboratorios/lab_02_bronze_generico.ipynb) | Notebook Bronze parametrizado |
| [lab_03_yaml_config.ipynb](laboratorios/lab_03_yaml_config.ipynb) | Lectura de YAML y configuración dinámica |
| [lab_04_silver_jobs.ipynb](laboratorios/lab_04_silver_jobs.ipynb) | Jobs encadenados con dependencias |

---

## Configs

| Archivo | Descripción |
|---------|-------------|
| [configs/bronze_config.yml](configs/bronze_config.yml) | Configuración de fuentes para Bronze |
| [configs/silver_config.yml](configs/silver_config.yml) | Transformaciones declarativas para Silver |

---

## Proyecto de la semana

Pipeline end-to-end con widget maestro dev/prod, 3 capas (Bronze→Silver→Gold)
y Job de 3 tasks encadenadas con análisis de fraude.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semana anterior (03):** los notebooks SQL/PySpark de semana 02-03 son la base que se parametriza aquí.
- **Semana siguiente (05):** los Jobs de esta semana evolucionan a Declarative Pipelines (DLT) con calidad de datos.
