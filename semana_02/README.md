# Semana 02 — PySpark Avanzado y Análisis de Fraude

**Tecnologías:** PySpark, Delta Lake, Databricks Community Edition  
**Dataset:** Financial Transactions (Caixabank Tech) — 1.42 GB, 5 archivos (CSV + JSON)

---

## Material obligatorio — Databricks Academy

Esta semana arrancas con Databricks. Antes de cerrar la semana debes completar los **12 videos**
del path **"Data Engineering"** en Databricks Free Edition (Home → Learn → Data Engineering).

| Módulo | Videos | Tiempo |
|--------|--------|--------|
| Data Engineering Basics | 2 videos | ~10 min |
| Ingestion and Transformation | 5 videos | ~41 min |
| Pipelines | 3 videos | ~23 min |
| Orchestration | 2 videos | ~14 min |

**Tiempo total: ~88 minutos**

Los videos de Pipelines y Orchestration los usarás en semanas 04 y 05 — por ahora velos una vez para tener contexto.

**Entrega:** copia, completa y sube el archivo de evidencia a tu PR de la semana:  
→ [documentos/databricks_academy_data_engineering.md](documentos/databricks_academy_data_engineering.md)

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades guiadas — una por tema de la semana |
| [laboratorios/](laboratorios/) | 3 notebooks de práctica complementaria |
| [documentos/](documentos/) | Material de referencia de la semana |
| [proyecto/](proyecto/) | Proyecto integrador: pipeline de detección de fraude |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | Fundamentos PySpark — schema, limpieza, groupBy | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | JOINs en PySpark — modelo de datos, INNER/LEFT/ANTI | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | Funciones avanzadas — fechas, window functions, acumulados | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | Pipeline Medallón con datos financieros — Bronze → Silver → Gold | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Laboratorios

| Lab | Tema |
|-----|------|
| [lab_01_exploracion.ipynb](laboratorios/lab_01_exploracion.ipynb) | Exploración inicial del dataset financiero |
| [lab_02_joins.ipynb](laboratorios/lab_02_joins.ipynb) | Práctica de JOINs con las 5 tablas |
| [lab_03_funciones_avanzadas.ipynb](laboratorios/lab_03_funciones_avanzadas.ipynb) | Window functions y funciones de fecha |

---

## Proyecto de la semana

Pipeline integrador de detección de fraude: construir la Gold table de transacciones fraudulentas,
el perfil de usuario en riesgo y un informe de hallazgos.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semana anterior (01):** se aplican los mismos patrones de Medallón pero con datos reales a escala.
- **Semana siguiente (03):** SQL avanzado se ejecutará sobre las tablas `silver.*` y `gold.*` construidas aquí.
