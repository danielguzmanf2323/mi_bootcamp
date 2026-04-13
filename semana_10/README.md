# Semana 10 — Cloud para Datos: Azure en Profundidad + Snowflake + Mapa de Nubes

**Tecnologías:** Azure Data Lake Storage Gen2, Azure IAM, Key Vault, Snowflake, AWS, GCP  
**Dataset:** Financial Transactions — subido a ADLS como ejercicio base  
**Entorno requerido:** Azure subscription (coordinar acceso con Inetum) + Snowflake Trial (30 días)

> Esta semana no construye un pipeline nuevo. El objetivo es entender la infraestructura
> sobre la que corren los pipelines que ya sabes construir, y desarrollar el criterio
> para elegir entre plataformas.

---

## Contenido

| Carpeta | Descripción |
|---------|-------------|
| [actividades/](actividades/) | 4 actividades — una por bloque |
| [documentos/](documentos/) | Material de referencia: cheatsheets, tablas de equivalencia |
| [proyecto/](proyecto/) | Architecture Decision Record (ADR) — entregable escrito, sin código |

---

## Actividades

| # | Tema | Enlace |
|---|------|--------|
| 01 | ADLS Gen2 — el sistema de archivos de datos en Azure | [actividades/actividad_01/](actividades/actividad_01/README.md) |
| 02 | Azure IAM para datos — service principals, Key Vault, RBAC | [actividades/actividad_02/](actividades/actividad_02/README.md) |
| 03 | Snowflake — orientación, external stages y comparativa con Databricks/Fabric | [actividades/actividad_03/](actividades/actividad_03/README.md) |
| 04 | AWS y GCP — mapeo de conceptos y traducción de arquitecturas | [actividades/actividad_04/](actividades/actividad_04/README.md) |

---

## Proyecto de la semana

Un **Architecture Decision Record (ADR)**: dado un caso de negocio con requisitos reales,
el alumno justifica qué stack tecnológico elegiría y por qué.
Evaluado como documento técnico — no hay código que entregar.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semanas anteriores (01-09):** todo lo que construiste corrió sobre infraestructura cloud ya configurada. Esta semana entiendes qué había debajo.
- **Semanas siguientes (11-12):** el proyecto final usa ADLS Gen2 como landing zone para el dataset Olist, y OneLake Shortcuts para exponer datos a Fabric.
