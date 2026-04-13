# Actividad 03 — Semana 10: Snowflake — Orientación y Comparativa

**Semana:** 10  
**Tema:** Arquitectura de Snowflake, ingesta desde ADLS y comparativa con Databricks y Fabric  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Snowflake Trial (30 días) + ADLS Gen2 del entorno del bootcamp

> **Prerequisito:** Crear una cuenta de trial en [snowflake.com/try-snowflake](https://signup.snowflake.com/).
> Seleccionar el cloud provider **Azure** y la región **West Europe** para que esté cerca del storage.

---

## Objetivo

Orientarse en Snowflake como plataforma alternativa: entender su arquitectura,
conectarla al mismo ADLS Gen2 que usas con Databricks, ingestar datos
y comparar cómo resuelve Snowflake los mismos problemas que ya resuelves
con Delta Lake y DLT. No es una semana de profundidad en Snowflake — es criterio
para elegir cuándo usarlo.

---

## Material de estudio previo

1. ¿Qué es la separación de **cómputo y almacenamiento** en Snowflake? ¿Cómo se diferencia de Databricks?
2. ¿Qué es un **Virtual Warehouse** en Snowflake? ¿Cómo se factura?
3. ¿Qué es un **Stage** en Snowflake? ¿Qué diferencia hay entre interno y externo?
4. ¿Qué son los **Streams** de Snowflake y a qué equivalen en el stack que ya conoces?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-snowflake-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_10/actividades/actividad_03/<tu-nombre>/
notas_<tu-nombre>.md
```

---

## Parte 1 — Explorar la arquitectura de Snowflake

Una vez en la consola de Snowflake (Snowsight), explora las secciones principales:

```sql
-- Ver los warehouses disponibles
SHOW WAREHOUSES;

-- Ver databases y schemas
SHOW DATABASES;

-- Crear tu propia base de datos para esta actividad
CREATE DATABASE bootcamp_sf;
CREATE SCHEMA bootcamp_sf.raw;
CREATE SCHEMA bootcamp_sf.silver;
CREATE SCHEMA bootcamp_sf.gold;
```

Documenta:
- ¿Qué tamaño tiene el virtual warehouse que te asignaron (`XS`, `S`, `M`...)?
- ¿Está configurado con `AUTO_SUSPEND`? ¿En cuántos segundos?
- ¿Qué significa que el warehouse esté suspendido y cómo afecta al coste?

Commit esperado:
```bash
git commit -m "docs: explore Snowflake architecture - warehouses and auto-suspend"
```

---

## Parte 2 — External Stage sobre ADLS Gen2

Conecta Snowflake al mismo ADLS Gen2 donde subiste el dataset Financial Transactions
en la actividad anterior. Esto es un External Stage.

```sql
-- Crear las credenciales para acceder a ADLS
CREATE STORAGE INTEGRATION azure_bootcamp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://<storage-account>.blob.core.windows.net/landing/');

-- Verificar la integración y obtener el App ID de Snowflake (para darle acceso en Azure)
DESC INTEGRATION azure_bootcamp_integration;
-- Copia el valor de AZURE_MULTI_TENANT_APP_NAME — necesitarás darlo de alta en Azure AD
```

Después de configurar los permisos en Azure (el instructor te guiará):

```sql
-- Crear el stage externo
CREATE STAGE bootcamp_sf.raw.adls_landing
  STORAGE_INTEGRATION = azure_bootcamp_integration
  URL = 'azure://<storage-account>.blob.core.windows.net/landing/raw/financial_transactions/'
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Verificar que Snowflake ve los archivos
LIST @bootcamp_sf.raw.adls_landing;
```

Documenta: ¿qué diferencia hay entre este External Stage y los Volumes de Unity Catalog?

Commit esperado:
```bash
git commit -m "docs: configure Snowflake External Stage over ADLS Gen2"
```

---

## Parte 3 — Ingesta con COPY INTO

```sql
-- Crear la tabla de destino
CREATE TABLE bootcamp_sf.raw.transactions (
  transaction_id   VARCHAR,
  amount           NUMBER(12,2),
  category         VARCHAR,
  status           VARCHAR,
  transaction_date DATE,
  customer_id      VARCHAR
);

-- Cargar datos desde el stage
COPY INTO bootcamp_sf.raw.transactions
FROM @bootcamp_sf.raw.adls_landing/transactions/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';  -- registrar errores sin parar la carga

-- Verificar
SELECT COUNT(*) FROM bootcamp_sf.raw.transactions;

-- Ver el historial de cargas (equivalente a DESCRIBE HISTORY en Delta)
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  table_name => 'TRANSACTIONS',
  start_time => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));
```

Documenta:
- ¿Cuántos registros se cargaron? ¿Hubo errores?
- ¿Qué pasa si ejecutas el `COPY INTO` dos veces? ¿Inserta duplicados?

Commit esperado:
```bash
git commit -m "docs: ingest data with COPY INTO and analyze idempotency"
```

---

## Parte 4 — Streams y Tasks (CDC en Snowflake)

Un **Stream** en Snowflake captura los cambios (INSERT/UPDATE/DELETE) en una tabla.
Equivale al Change Data Feed de Delta Lake + el `apply_changes` de DLT.

```sql
-- Crear un stream sobre la tabla raw
CREATE STREAM bootcamp_sf.raw.transactions_stream
ON TABLE bootcamp_sf.raw.transactions;

-- Insertar algunos registros nuevos para ver el stream en acción
INSERT INTO bootcamp_sf.raw.transactions
VALUES ('TX_STREAM_TEST_001', 99.99, 'food', 'completed', '2024-06-01', 'C999');

-- Ver los cambios capturados por el stream
SELECT * FROM bootcamp_sf.raw.transactions_stream;
-- Nota: las columnas METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID

-- Un Task procesa el stream automáticamente (equivale a un DLT pipeline)
CREATE TASK bootcamp_sf.raw.process_transactions_stream
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 minute'
AS
  INSERT INTO bootcamp_sf.silver.transactions
  SELECT transaction_id, amount, category, status, transaction_date, customer_id
  FROM bootcamp_sf.raw.transactions_stream
  WHERE METADATA$ACTION = 'INSERT';

-- Activar el task
ALTER TASK bootcamp_sf.raw.process_transactions_stream RESUME;
```

Documenta: ¿en qué se parece esto a DLT `apply_changes`? ¿Qué maneja DLT que Streams+Tasks no hace automáticamente?

Commit esperado:
```bash
git commit -m "docs: Snowflake Streams and Tasks vs DLT apply_changes comparison"
```

---

## Parte 5 — Comparativa directa: Snowflake vs Databricks vs Fabric

Completa esta tabla con lo que has visto en las últimas semanas:

| Capacidad | Databricks Delta | Snowflake | Microsoft Fabric |
|-----------|-----------------|-----------|-----------------|
| Formato de almacenamiento | Delta (Parquet + log) | Propietario (columnar) | Delta (OneLake) |
| Ingesta batch | COPY INTO / Auto Loader | COPY INTO | Pipelines / Dataflow |
| CDC incremental | DLT apply_changes | Streams + Tasks | Mirroring / Eventstream |
| Time travel | ✓ (Delta log) | ✓ (nativo) | ✓ (Delta log) |
| Compute model | Clusters (on/off) | Virtual Warehouses (auto-suspend) | Capacity (siempre on) |
| Modelo de coste | Por cluster hora | Por warehouse segundo | Por CU (capacity unit) |
| Cuándo elegirlo | | | |

Añade tu criterio en la última fila: ¿en qué escenario de negocio elegirías cada uno?

Commit esperado:
```bash
git commit -m "docs: Snowflake vs Databricks vs Fabric comparison table"
```

---

## Entrega en Git

```bash
git push origin feature/semana10-snowflake-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] Snowflake — Orientación y Comparativa — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] External Stage sobre ADLS configurado | `LIST @stage` muestra los archivos | 20% |
| [Requerido] COPY INTO ejecutado e idempotencia documentada | Comportamiento en segunda ejecución explicado | 25% |
| [Requerido] Tabla comparativa completa con criterio propio | "Cuándo elegirlo" con justificación real | 30% |
| [Recomendado] Streams + Tasks probados | Comparativa con DLT documentada | 25% |
| **Total** | | **100%** |

---

## Referencias

- [Snowflake Documentation — Getting Started](https://docs.snowflake.com/en/user-guide-getting-started)
- [Snowflake External Stages for Azure](https://docs.snowflake.com/en/user-guide/data-load-azure-config)
- [Snowflake Streams and Tasks](https://docs.snowflake.com/en/user-guide/streams)
