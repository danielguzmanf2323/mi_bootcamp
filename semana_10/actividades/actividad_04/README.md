# Actividad 04 — Semana 10: AWS y GCP — Mapa de Conceptos y Traducción de Arquitecturas

**Semana:** 10  
**Tema:** Equivalencias entre nubes — de Azure a AWS y GCP  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Conceptual + ejercicio de diseño (no requiere acceso a AWS ni GCP)

---

## Objetivo

Desarrollar el criterio para orientarse en cualquier nube sin haber trabajado en ella.
El objetivo no es aprender AWS ni GCP desde cero — es entender el patrón mental:
cada nube tiene un servicio para almacenar, uno para computar, uno para orquestar.
Una vez entiendes el patrón en Azure (que es lo que conoces), las otras son mapeo.

---

## Material de estudio previo

1. Busca y lee: ¿qué es **Amazon S3** y qué lo diferencia de **ADLS Gen2**? ¿Qué le falta a S3 que tiene ADLS?
2. ¿Qué es **Amazon Athena**? ¿Sobre qué almacenamiento corre? ¿A qué servicio de Azure equivale?
3. ¿Qué es **Google BigQuery**? ¿Tiene separación de cómputo y almacenamiento o es un sistema unificado?
4. ¿Qué es **AWS Glue**? ¿Qué rol cumple en un pipeline de datos en AWS?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-multicloud-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_10/actividades/actividad_04/<tu-nombre>/
notas_<tu-nombre>.md
arquitectura_traducida_<tu-nombre>.md
```

---

## Parte 1 — Tabla de equivalencias completa

Completa esta tabla con tus investigaciones. Añade la columna **diferencia clave**
— no basta con saber el nombre, hay que saber en qué se diferencia.

| Categoría | Azure | AWS | GCP | Diferencia clave |
|-----------|-------|-----|-----|-----------------|
| Object storage | ADLS Gen2 | S3 | GCS | ADLS tiene Hierarchical Namespace nativo; S3 lo simula con prefijos |
| Serverless SQL | Synapse Serverless | Athena | BigQuery | |
| Managed Spark | Databricks / HDInsight | EMR / Databricks | Dataproc / Databricks | |
| Orchestration | Data Factory / Fabric | Step Functions / MWAA | Cloud Composer | |
| Streaming | Event Hubs | Kinesis | Pub/Sub | |
| DWH managed | Synapse Dedicated | Redshift | BigQuery | |
| Secret management | Key Vault | Secrets Manager | Secret Manager | |
| Identity / Auth | Azure AD + RBAC | IAM | IAM | |
| Data catalog | Purview | Glue Data Catalog | Dataplex | |
| Managed Kafka | Event Hubs (Kafka API) | MSK | Confluent / Pub/Sub | |

Commit esperado:
```bash
git commit -m "docs: complete multi-cloud equivalence table"
```

---

## Parte 2 — Traducción de arquitectura: de AWS a Azure

Lee esta arquitectura de un pipeline de datos en AWS y tradúcela a Azure.
Dibuja el diagrama equivalente en texto (ASCII art) en tu documento.

**Arquitectura AWS original:**
```
S3 (raw bucket)
    │
    │  AWS Glue ETL Job (PySpark)
    ▼
S3 (processed bucket, Parquet)
    │
    │  AWS Glue Crawler (actualiza catálogo)
    ▼
AWS Glue Data Catalog
    │
    │  Amazon Athena (SQL sobre S3)
    ▼
Amazon QuickSight (dashboards)
```

**Tu traducción a Azure debe:**
- Usar los servicios equivalentes correctos
- Mantener la misma lógica de pipeline (raw → procesado → consulta → visualización)
- Añadir una nota por cada decisión: ¿por qué ese servicio de Azure y no otro?

Commit esperado:
```bash
git commit -m "docs: AWS to Azure architecture translation with justification"
```

---

## Parte 3 — Traducción inversa: de Azure (nuestro stack) a AWS y GCP

Ahora toma la arquitectura que has construido en el bootcamp y tradúcela a las otras dos nubes.

**Arquitectura Azure (bootcamp):**
```
ADLS Gen2 (landing)
    │  Auto Loader
    ▼
Delta Lake Bronze (Databricks)
    │  PySpark + MERGE INTO
    ▼
Delta Lake Silver (Databricks)
    │  Structured Streaming + ZORDER
    ▼
Delta Lake Gold (Databricks)
    │  OneLake Shortcut
    ▼
Microsoft Fabric (Power BI report)
```

Traduce a:
- **AWS equivalente:** diagrama completo con servicios AWS
- **GCP equivalente:** diagrama completo con servicios GCP

Para cada traducción, identifica **una funcionalidad que no tiene equivalente directo** o donde el equivalente es claramente inferior/superior.

Commit esperado:
```bash
git commit -m "docs: bootcamp Azure stack translated to AWS and GCP"
```

---

## Parte 4 — Cuándo no usar Azure

La honestidad técnica importa. Documenta al menos **dos escenarios reales** en los que
elegirías AWS o GCP sobre Azure para un pipeline de datos, y justifica por qué.

Pistas para investigar:
- ¿Qué nube tiene el mejor ecosistema para ML + datos en el mismo stack?
- ¿Qué nube usarías si el cliente ya tiene todo su negocio en Google Workspace?
- ¿Qué pasa si el cliente necesita procesar datos en regiones donde Azure no tiene datacenter?

No hay respuesta correcta única — se evalúa la calidad del razonamiento, no la respuesta.

Commit esperado:
```bash
git commit -m "docs: scenarios where Azure is not the best choice"
```

---

## Entrega en Git

```bash
git push origin feature/semana10-multicloud-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] Multi-cloud mapping — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Tabla de equivalencias completa | Diferencia clave rellena para todas las filas | 30% |
| [Requerido] Traducción AWS → Azure con justificación | Una nota de decisión por servicio | 25% |
| [Requerido] Traducción bootcamp stack → AWS y GCP | Ambas nubes documentadas | 25% |
| [Recomendado] Escenarios donde no elegirías Azure | Razonamiento técnico sólido | 20% |
| **Total** | | **100%** |

---

## Referencias

- [AWS vs Azure vs GCP — Cloud comparison (Google)](https://cloud.google.com/docs/get-started/aws-azure-gcp-service-comparison)
- [AWS to Azure service comparison (Microsoft docs)](https://learn.microsoft.com/en-us/azure/architecture/aws-professional/services)
- [Google Cloud for AWS professionals](https://cloud.google.com/docs/get-started/aws-azure-gcp-service-comparison)
