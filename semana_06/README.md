# Semana 06 — Databricks II: Delta Lake Avanzado, Streaming y Performance

**Tecnologías:** Delta Lake, Structured Streaming, Auto Loader, Spark UI, Databricks Enterprise  
**Dataset:** Tablas `bronze.*`, `silver.*`, `gold.*` construidas en semanas 02-05 sobre Financial Transactions  
**Entorno requerido:** Databricks Enterprise (Unity Catalog + Volumes)

> Esta semana no introduce datos nuevos. El foco está en operar con profundidad
> sobre lo que ya construiste — optimizar, escalar y entender qué pasa por debajo.

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades — una por bloque técnico |
| [laboratorios/](laboratorios/) | Notebooks de práctica complementaria |
| [documentos/](documentos/) | Material de referencia de la semana |
| [proyecto/](proyecto/) | Pipeline de streaming end-to-end + optimización documentada |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | Delta Lake en producción — OPTIMIZE, ZORDER, Liquid Clustering, VACUUM, Time Travel, Clone | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | MERGE INTO y upserts — patrones de ingesta incremental, comparativa con DLT | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | Structured Streaming — trigger modes, watermarking, checkpointing | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | Performance tuning — Spark UI, broadcast joins, skew, small files | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Proyecto de la semana

Pipeline de streaming end-to-end que simula ingesta continua del dataset financiero:
Auto Loader en modo streaming + MERGE INTO para mantener estado + Gold optimizada
con ZORDER + informe de decisiones de optimización con evidencia de Spark UI.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semanas anteriores (02-05):** todo lo construido ahí es la materia prima de esta semana.
- **Semanas siguientes (07-09):** Microsoft Fabric — los mismos patrones de calidad y optimización aplican en otra plataforma.
- La semana 06 cierra el bloque Databricks. Al terminarla, el alumno debe poder operar un pipeline en producción, no solo escribirlo.
