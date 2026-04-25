# Semana 05 — Declarative Pipelines (DLT), CDC y SCD

**Tecnologías:** Delta Live Tables (DLT), Auto Loader, Unity Catalog, Databricks Enterprise  
**Entorno:** Databricks Enterprise edición Standard (Expectations) o Advanced (SCD2/apply_changes)  
**Dataset:** Financial Transactions — procesado en modo incremental con batches simulados

---

> **Convención obligatoria — entorno compartido**
>
> Todos los estudiantes del bootcamp trabajan sobre el mismo catálogo `default` en Databricks Enterprise.
> Para no sobrescribir las tablas de otras personas, **sufija siempre tu nombre en cada tabla que crees**:
>
> ```
> default.bronze.transactions_<tu_nombre>
> default.silver.users_scd2_<tu_nombre>
> ```
>
> En pipelines DLT, el nombre del pipeline y el target schema deben incluir tu nombre.
> Si usas `mode("overwrite")` o `apply_changes` sin el sufijo, sobrescribirás el trabajo de otro estudiante.

---

## Material obligatorio — Databricks Academy

Esta semana debes completar los **3 videos** del path **"Build a data pipeline"**
en Databricks Free Edition (Home → Learn → Build a data pipeline).

| # | Video | Tipo | Tiempo |
|---|-------|------|--------|
| 1 | What is a pipeline? | Video | 4 min |
| 2 | Explore a sample pipeline | Video + demo | 7 min |
| 3 | Build your own pipeline | Video + tutorial | 5 min |

**Tiempo total: ~16 minutos**

**Entrega:** copia, completa y sube el archivo de evidencia a tu PR de la semana:  
→ [documentos/databricks_academy_build_pipeline.md](documentos/databricks_academy_build_pipeline.md)

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades guiadas — una por tema de la semana |
| [laboratorios/](laboratorios/) | 4 notebooks de práctica con DLT |
| [documentos/](documentos/) | Material de referencia de la semana |
| [proyecto/](proyecto/) | Proyecto integrador: pipeline DLT de producción completo |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | DLT fundamentos — `@dlt.table`, `dlt.read()`, Pipeline Parameters | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | Auto Loader + Data Quality Expectations — quarantine pattern | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | CDC y dimensiones lentamente cambiantes — SCD1 y SCD2 | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | Pipeline completo + Notificaciones — Dev vs Prod mode | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Laboratorios

| Lab | Tema |
|-----|------|
| [lab_01_pipeline_basica.ipynb](laboratorios/lab_01_pipeline_basica.ipynb) | Primera pipeline DLT con decoradores |
| [lab_02_autoloader_quality.ipynb](laboratorios/lab_02_autoloader_quality.ipynb) | Auto Loader y Expectations |
| [lab_03_cdc_scd.ipynb](laboratorios/lab_03_cdc_scd.ipynb) | apply_changes(), SCD1 y SCD2 |
| [lab_04_pipeline_completa.ipynb](laboratorios/lab_04_pipeline_completa.ipynb) | Pipeline end-to-end con notificaciones |

---

## Proyecto de la semana

Pipeline DLT de producción: Auto Loader + SCD2 + calidad de datos + Gold analítico
+ notificaciones + `pipeline_spec.json` entregado.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semana anterior (04):** los Jobs parametrizados evolucionan a pipelines declarativos con garantías de calidad.
- **Semana siguiente (06):** dbt (data build tool) — transformaciones SQL declarativas sobre las capas construidas aquí.
