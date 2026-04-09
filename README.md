# Inetum Data Engineer Bootcamp

Repositorio oficial del programa de formación en Data Engineering de Inetum.

---

## Estructura del repositorio

```
semana_XX/
    actividades/     ← actividades guiadas por tema
    documentos/      ← material de referencia
    laboratorios/    ← ejercicios adicionales
    proyecto/        ← proyecto integrador de la semana
```

Cada actividad y proyecto tiene su propio `README.md` con instrucciones, criterios de evaluación y referencias.

---

## Documentación del proyecto

| Documento | Descripción |
|-----------|-------------|
| [GITFLOW.md](GITFLOW.md) | Guía de ramas, commits y pull requests |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Cómo añadir semanas, convenciones de actividades, modelo de fork por cohorte |
| [DESIGN.md](DESIGN.md) | Decisiones pedagógicas y técnicas — el "por qué" del diseño del bootcamp |
| [CHANGELOG.md](CHANGELOG.md) | Cambios por cohorte: qué funcionó, qué se ajustó, problemas conocidos |

---

## Contenido por semana

### Semana 01 — Fundamentos

Dataset: customers (CSV/JSON/Parquet/Avro) + Maven Fuzzy Factory (6 tablas e-commerce)  
Tecnologías: Git, PySpark, Delta Lake, Databricks Community Edition

| Actividad | Tema | Descripción |
|-----------|------|-------------|
| [Actividad 01](semana_01/actividades/actividad_01/README.md) | Git workflow | Clonar repo, crear ramas, commits, PRs. git stash, revert y estrategias de undo |
| [Actividad 02](semana_01/actividades/actividad_02/README.md) | Conceptos Data Engineering | ETL/ELT, batch/streaming, Data Warehouse, Data Lake, Lakehouse, roles y stack selection |
| [Actividad 03](semana_01/actividades/actividad_03/README.md) | Formatos de datos | CSV, JSON, Parquet, Avro en PySpark + SQL con spark.sql() + investigación JOINs |
| [Actividad 04](semana_01/actividades/actividad_04/README.md) | Medallón Architecture | Pipeline Bronze → Silver → Gold con dataset de 2M clientes |
| [Proyecto](semana_01/proyecto/README.md) | Maven Fuzzy Factory | Pipeline completo sobre 6 tablas de e-commerce. JOINs, limpieza, Gold tables, análisis de negocio |

---

### Semana 02 — PySpark Avanzado y Análisis de Fraude

Dataset: [Financial Transactions (Caixabank Tech)](https://www.kaggle.com/datasets/ealtman2019/credit-card-transactions) — 1.42GB, 5 archivos (CSV + JSON)  
Tecnologías: PySpark, Delta Lake, Databricks Community Edition

| Actividad | Tema | Descripción |
|-----------|------|-------------|
| [Actividad 01](semana_02/actividades/actividad_01/README.md) | Fundamentos PySpark | Leer CSV con schema, limpieza de tipos, filtros, columnas calculadas, groupBy, análisis de nulls |
| [Actividad 02](semana_02/actividades/actividad_02/README.md) | JOINs en PySpark | Modelo de datos, INNER/LEFT/ANTI joins con las 5 tablas, pérdida de registros documentada |
| [Actividad 03](semana_02/actividades/actividad_03/README.md) | Funciones avanzadas | Funciones de fecha (date_trunc, datediff), window functions (rank, lag, acumulados, media móvil) |
| [Actividad 04](semana_02/actividades/actividad_04/README.md) | Medallón con fraude | Pipeline Bronze → Silver → Gold con las 5 tablas del dataset financiero |
| [Proyecto](semana_02/proyecto/README.md) | Detección de fraude | Pipeline integrador: Gold de fraude + perfil de usuario en riesgo + informe de hallazgos |

---

### Semana 03 — SQL Avanzado sobre tablas Delta

Dataset: Mismo dataset financiero de semana 02 — consumo de tablas `silver.*` y `gold.*` ya construidas  
Tecnologías: Spark SQL, Delta Lake, Databricks Community Edition

| Actividad | Tema | Descripción |
|-----------|------|-------------|
| [Actividad 01](semana_03/actividades/actividad_01/README.md) | SQL básico en Databricks | SELECT, WHERE, GROUP BY, HAVING, CASE WHEN, funciones de fecha sobre tablas Delta |
| [Actividad 02](semana_03/actividades/actividad_02/README.md) | JOINs en SQL | INNER, LEFT, múltiples JOINs encadenados, subqueries (WHERE/FROM/EXISTS), PySpark vs SQL |
| [Actividad 03](semana_03/actividades/actividad_03/README.md) | SQL avanzado | CTEs encadenadas, window functions (ROW_NUMBER, RANK, LAG, LEAD, NTILE), QUALIFY |
| [Actividad 04](semana_03/actividades/actividad_04/README.md) | Vistas y plan de ejecución | CREATE VIEW, EXPLAIN, mapa completo SQL↔PySpark |
| [Proyecto](semana_03/proyecto/README.md) | Capa analítica para fraude | 3 vistas Gold + 3 nuevas tablas Gold en SQL + informe de 5 hallazgos |

---

### Semanas 04–12 — Próximamente

| Semana | Tema principal | Tecnologías |
|--------|----------------|-------------|
| 04 | Ingesta de datos | TBD |
| 05 | dbt (data build tool) | dbt, Delta Lake |
| 06 | Orquestación con Airflow | Apache Airflow |
| 07–08 | Microsoft Fabric | Microsoft Fabric |
| 09–11 | Proyecto final | Stack completo |
| 12 | Presentaciones finales | — |
