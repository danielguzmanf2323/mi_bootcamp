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
| 05 | Modelado dimensional (Kimball) — diseño del star schema del proyecto final | [actividades/actividad_05/](actividades/actividad_05/README.md) |

---

## Proyecto de la semana

**Reflexión comparativa de plataformas:** Databricks, Fabric y Snowflake vistos desde
la experiencia propia de las últimas semanas. Documento escrito, sin código.
El objetivo es desarrollar criterio de elección, no demostrar que se conocen los features.

→ [Ver instrucciones del proyecto](proyecto/README.md)

---

## Conexión con otras semanas

- **Semanas anteriores (01-09):** todo lo que construiste corrió sobre infraestructura cloud ya configurada. Esta semana entiendes qué había debajo.
- **Semanas siguientes (11-12):** el proyecto final usa ADLS Gen2 como landing zone para el dataset Olist, y OneLake Shortcuts para exponer datos a Fabric.
