# Semana 03 — SQL Avanzado sobre Tablas Delta

**Tecnologías:** Spark SQL, Delta Lake, Databricks Community Edition  
**Dataset:** Tablas `silver.*` y `gold.*` del dataset financiero construidas en semana 02

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades guiadas — una por tema de la semana |
| [laboratorios/](laboratorios/) | 4 notebooks de práctica SQL complementaria |
| [documentos/](documentos/) | Material de referencia de la semana |
| [proyecto/](proyecto/) | Proyecto integrador: capa analítica completa en SQL |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | SQL básico en Databricks — SELECT, GROUP BY, CASE WHEN | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | JOINs en SQL — INNER, LEFT, subqueries, PySpark vs SQL | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | SQL avanzado — CTEs, window functions, QUALIFY | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | Vistas y plan de ejecución — CREATE VIEW, EXPLAIN | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Laboratorios

| Lab | Tema |
|-----|------|
| [lab_01_sql_basico.ipynb](laboratorios/lab_01_sql_basico.ipynb) | Práctica de SQL básico sobre tablas Delta |
| [lab_02_sql_joins.ipynb](laboratorios/lab_02_sql_joins.ipynb) | JOINs y subqueries en Spark SQL |
| [lab_03_sql_avanzado.ipynb](laboratorios/lab_03_sql_avanzado.ipynb) | CTEs y window functions |
| [lab_04_vistas_optimizacion.ipynb](laboratorios/lab_04_vistas_optimizacion.ipynb) | Vistas y plan de ejecución |

---

## Proyecto de la semana

Construir la capa analítica completa en SQL: 3 vistas Gold y 3 nuevas tablas Gold
con informe de 5 hallazgos de negocio.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semana anterior (02):** esta semana consume las tablas que construiste con PySpark.
- **Semana siguiente (04):** los notebooks SQL y PySpark se convierten en pipelines orquestados con Databricks Jobs.
