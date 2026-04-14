# Semana 01 — Fundamentos: Git, Formatos de Datos y Arquitectura Medallón

**Tecnologías:** Git, PySpark, Delta Lake, Databricks Community Edition  
**Dataset:** customers (CSV/JSON/Parquet/Avro) + Maven Fuzzy Factory (6 tablas e-commerce)

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades guiadas — una por tema de la semana |
| [laboratorios/](laboratorios/) | Ejercicios adicionales de práctica libre |
| [documentos/](documentos/) | Material de referencia de la semana |
| [proyecto/](proyecto/) | Proyecto integrador: pipeline completo sobre Maven Fuzzy Factory |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | Git workflow — ramas, commits, PRs, undo | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | Conceptos de Data Engineering — ETL/ELT, capas, roles | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | Formatos de datos — CSV, JSON, Parquet, Avro en PySpark | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | Arquitectura Medallón — pipeline Bronze → Silver → Gold | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Proyecto de la semana

Pipeline completo sobre el dataset Maven Fuzzy Factory (6 tablas de e-commerce):
JOINs, limpieza de datos, construcción de Gold tables y análisis de negocio.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semana siguiente (02):** los patrones PySpark aprendidos aquí se aplican a un dataset real de 1.4 GB con datos financieros.
- La arquitectura Medallón de esta semana es el patrón que se usará en **todas** las semanas siguientes.
