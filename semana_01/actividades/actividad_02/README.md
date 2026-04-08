# Actividad 02 — El mundo de los datos: conceptos fundamentales

**Semana:** 01  
**Tema:** Fundamentos de Data Engineering  
**Nivel:** Junior  
**Modalidad:** Individual

---

## Objetivo

Investigar, entender con tus propias palabras y documentar los conceptos fundamentales del mundo de los datos. El entregable se entrega a través de Git, aplicando el flujo aprendido en la Actividad 01.

---

## Contexto

Antes de escribir un solo pipeline, un Data Engineer necesita tener claro el vocabulario del ecosistema. Esta actividad te desafía a investigar los conceptos más importantes y explicarlos como si tuvieras que enseñárselos a alguien que no sabe nada del tema.

---

## Instrucciones de entrega (Git)

Sigue el mismo flujo de la Actividad 01:

```bash
# 1. Actualizar develop
git checkout develop
git pull origin develop

# 2. Crear tu rama
git checkout -b feature/semana01-conceptos-<tu-nombre>

# 3. Crear tu archivo de entrega en esta carpeta
#    semana_01/actividades/actividad_02/conceptos_<tu-nombre>.md

# 4. Hacer commits por sección (mínimo 4 commits)
git add .
git commit -m "docs: add ETL and ELT explanation"

# 5. Subir la rama
git push origin feature/semana01-conceptos-<tu-nombre>

# 6. Abrir PR hacia develop con título:
#    [Semana 01] Conceptos de Data Engineering — <Tu Nombre>
```

---

## Estructura del archivo de entrega

Dentro de `semana_01/actividades/actividad_02/` crea **una carpeta con tu nombre** y dentro de ella el archivo:

```
semana_01/actividades/actividad_02/<tu-nombre>/conceptos_<tu-nombre>.md
```

Ejemplo: `semana_01/actividades/actividad_02/maria/conceptos_maria.md`

> Esta convención se aplica en todas las actividades del bootcamp.

Crea el archivo `conceptos_<tu-nombre>.md` con la siguiente estructura base.
Puedes ampliar cada sección todo lo que quieras.

```markdown
# Conceptos de Data Engineering — <Tu Nombre>

## 1. ETL vs ELT
## 2. Batch vs Streaming
## 3. Data Warehouse
## 4. Data Lake
## 5. Data Lakehouse
## 6. Pipeline de datos
## 7. Otros conceptos
## 8. Caso práctico imaginario
## 9. Reflexión personal
```

---

## Conceptos a investigar y responder

Para cada concepto responde: **¿qué es?**, **¿para qué sirve?** y **da un ejemplo concreto**.

---

### 1. ETL — Extract, Transform, Load

- ¿Qué significa cada letra?
- ¿En qué orden ocurren los pasos?
- ¿Dónde se hace la transformación?
- Ejemplo: una empresa quiere mover datos de ventas desde un sistema Oracle hacia un Data Warehouse. ¿Cómo aplicarías ETL?

---

### 2. ELT — Extract, Load, Transform

- ¿En qué se diferencia del ETL?
- ¿Cuándo conviene usarlo?
- ¿Por qué herramientas como **dbt** encajan en este modelo?
- Ejemplo: explica por qué en un Data Lakehouse moderno se prefiere ELT.

---

### 3. Batch Processing

- ¿Qué significa procesar datos en batch?
- ¿Cada cuánto tiempo se ejecuta un proceso batch típico?
- Ventajas y desventajas
- Ejemplo: ¿cuándo usarías batch en un pipeline de reportes financieros?

---

### 4. Streaming

- ¿Qué es el procesamiento en tiempo real?
- ¿Qué diferencia hay entre "near real-time" y "real-time"?
- Herramientas comunes: Apache Kafka, Apache Flink, Spark Structured Streaming
- Ejemplo: ¿cuándo usarías streaming en vez de batch?

---

### 5. Data Warehouse

- ¿Qué es y para qué sirve?
- ¿Qué tipo de datos almacena?
- Ejemplos de tecnologías: Snowflake, BigQuery, Redshift, Synapse
- ¿Qué es el esquema estrella (star schema)?

---

### 6. Data Lake

- ¿Qué lo diferencia de un Data Warehouse?
- ¿Qué tipo de archivos se almacenan? (CSV, Parquet, Avro, JSON, imágenes, logs...)
- ¿Cuál es el riesgo de un Data Lake mal gestionado? (pista: "data swamp")
- Ejemplos: Amazon S3, Azure Data Lake Storage, Google Cloud Storage

---

### 7. Data Lakehouse

- ¿Qué problema viene a resolver?
- ¿Qué combina del Data Lake y del Data Warehouse?
- Tecnologías: Delta Lake, Apache Iceberg, Apache Hudi
- ¿Por qué se dice que es la arquitectura moderna de referencia?

---

### 8. Pipeline de datos

- ¿Qué es un pipeline de datos?
- ¿Qué etapas puede tener?
- ¿Qué herramienta se usa para orquestar pipelines? (pista: Apache Airflow)
- Dibuja o describe con texto el flujo de un pipeline simple: fuente → transformación → destino

---

### 9. Otros conceptos — investiga y explica brevemente cada uno

| Concepto | ¿Qué es? | Ejemplo o herramienta asociada |
|----------|----------|-------------------------------|
| Data Mesh | | |
| Data Catalog | | |
| Linaje de datos (Data Lineage) | | |
| Schema-on-read vs Schema-on-write | | |
| Particionado de datos | | |
| Data Mart | | |
| Idempotencia en pipelines | | |
| SLA de datos | | |
| CDC — Change Data Capture | | |
| Formato columnar vs fila (Parquet vs CSV) | | |

---

### 10. Caso práctico imaginario

Imagina que trabajas para una empresa de e-commerce con millones de transacciones al día.
Diseña (solo en texto, sin código) cómo construirías el flujo de datos para responder esta pregunta:

> **"¿Cuáles fueron los 10 productos más vendidos en los últimos 7 días, por región?"**

Incluye:
- ¿De dónde vienen los datos?
- ¿Batch o streaming?
- ¿Cómo los transformas?
- ¿Dónde los almacenas?
- ¿Quién los consume?

No hay respuesta única ni correcta — se evalúa que el razonamiento sea coherente.

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Rama creada con el formato correcto | `feature/semana01-conceptos-<nombre>` | 10% |
| Mínimo 4 commits con mensajes claros | Un commit por sección o grupo de secciones | 20% |
| Todos los conceptos respondidos | Con definición, utilidad y ejemplo | 40% |
| Tabla de conceptos adicionales completa | Todos los 10 conceptos de la tabla | 15% |
| Caso práctico | Coherente y bien razonado | 15% |

---

## Lo que NO se debe hacer

- No copiar y pegar definiciones de Wikipedia sin reescribirlas con tus palabras
- No hacer un solo commit con todo el trabajo
- No dejar secciones vacías o con "pendiente"
- No abrir el PR hacia `main`

---

## Referencia

- [GITFLOW.md](../../GITFLOW.md) — flujo de trabajo con Git en este repositorio
- [Actividad 01](../actividad_01/README.md) — repaso del flujo de Git si tienes dudas
