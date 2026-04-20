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

### 1. ETL — Extract, Transform, Load `[Requerido]`

- ¿Qué significa cada letra?
- ¿En qué orden ocurren los pasos?
- ¿Dónde se hace la transformación?
- Ejemplo: una empresa quiere mover datos de ventas desde un sistema Oracle hacia un Data Warehouse. ¿Cómo aplicarías ETL?

---

### 2. ELT — Extract, Load, Transform `[Requerido]`

- ¿En qué se diferencia del ETL?
- ¿Cuándo conviene usarlo?
- ¿Por qué herramientas como **dbt** encajan en este modelo?
- Ejemplo: explica por qué en un Data Lakehouse moderno se prefiere ELT.

---

### 3. Batch Processing `[Requerido]`

- ¿Qué significa procesar datos en batch?
- ¿Cada cuánto tiempo se ejecuta un proceso batch típico?
- Ventajas y desventajas
- Ejemplo: ¿cuándo usarías batch en un pipeline de reportes financieros?

---

### 4. Streaming `[Requerido]`

- ¿Qué es el procesamiento en tiempo real?
- ¿Qué diferencia hay entre "near real-time" y "real-time"?
- Herramientas comunes: Apache Kafka, Apache Flink, Spark Structured Streaming
- Ejemplo: ¿cuándo usarías streaming en vez de batch?

---

### 5. Data Warehouse `[Requerido]`

- ¿Qué es y para qué sirve?
- ¿Qué tipo de datos almacena?
- Ejemplos de tecnologías: Snowflake, BigQuery, Redshift, Synapse
- ¿Qué es el esquema estrella (star schema)?

---

### 6. Data Lake `[Requerido]`

- ¿Qué lo diferencia de un Data Warehouse?
- ¿Qué tipo de archivos se almacenan? (CSV, Parquet, Avro, JSON, imágenes, logs...)
- ¿Cuál es el riesgo de un Data Lake mal gestionado? (pista: "data swamp")
- Ejemplos: Amazon S3, Azure Data Lake Storage, Google Cloud Storage

---

### 7. Data Lakehouse `[Requerido]`

- ¿Qué problema viene a resolver?
- ¿Qué combina del Data Lake y del Data Warehouse?
- Tecnologías: Delta Lake, Apache Iceberg, Apache Hudi
- ¿Por qué se dice que es la arquitectura moderna de referencia?

---

### 8. Pipeline de datos `[Requerido]`

- ¿Qué es un pipeline de datos?
- ¿Qué etapas puede tener?
- ¿Qué herramienta se usa para orquestar pipelines? (pista: Apache Airflow)
- Dibuja o describe con texto el flujo de un pipeline simple: fuente → transformación → destino

---

### 9. Otros conceptos — investiga y explica brevemente cada uno

Cada concepto está marcado con su nivel de prioridad:
- `[Requerido]` — base mínima esperada para continuar con las siguientes semanas
- `[Recomendado]` — importante para el trabajo real, investígalo aunque sea brevemente
- `[Opcional]` — profundiza si tienes tiempo o curiosidad

| Concepto | ¿Qué es? | Ejemplo o herramienta asociada | Nivel |
|----------|----------|-------------------------------|-------|
| Data Mart | | | `[Requerido]` |
| Particionado de datos | | | `[Requerido]` |
| Formato columnar vs fila (Parquet vs CSV) | | | `[Requerido]` |
| Data Catalog | | | `[Recomendado]` |
| Linaje de datos (Data Lineage) | | | `[Recomendado]` |
| Schema-on-read vs Schema-on-write | | | `[Recomendado]` |
| SLA de datos | | | `[Recomendado]` |
| Idempotencia en pipelines | | | `[Recomendado]` |
| CDC — Change Data Capture | | | `[Opcional]` |
| Data Mesh | | | `[Opcional]` |

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

### 11. Arquitectura de datos de extremo a extremo

Dibuja (en texto o con una tabla) un flujo completo de arquitectura de datos que incluya al menos estas 5 etapas:

```
[Fuente de datos] → [Ingesta] → [Almacenamiento] → [Transformación] → [Consumo]
```

Para cada etapa indica:
- Qué tipo de herramienta o tecnología se usa
- Qué tipo de datos circulan
- Un ejemplo concreto de herramienta (Kafka, S3, dbt, Power BI, etc.)

No hay que ser exhaustivo — se valora que el flujo sea coherente y que los conceptos estén bien ubicados.

Commit esperado:
```bash
git commit -m "docs: add end-to-end data architecture diagram"
```

---

### 12. Roles del equipo de datos

Investiga y diferencia los siguientes roles. Para cada uno responde: ¿qué hace día a día?, ¿con qué herramientas trabaja?, ¿en qué se diferencia del Data Engineer?

| Rol | Responsabilidad principal | Herramientas típicas | Diferencia con Data Engineer |
|-----|--------------------------|---------------------|------------------------------|
| Data Engineer | | | — |
| Data Analyst | | | |
| Data Scientist | | | |
| Analytics Engineer | | | |
| MLOps Engineer | | | |

Commit esperado:
```bash
git commit -m "docs: add data team roles comparison"
```

---

### 13. Elige tu stack

Se te presentan dos escenarios. Para cada uno, elige una arquitectura y justifica tu decisión en 5–10 líneas. No hay respuesta incorrecta, se evalúa el razonamiento.

**Escenario A:**
> Una startup de logística quiere analizar el historial de entregas del último año (10 millones de registros) para generar reportes semanales de rendimiento por ciudad. El equipo de datos tiene 2 personas y presupuesto ajustado.

**Escenario B:**
> Una plataforma de pagos necesita detectar transacciones fraudulentas en menos de 2 segundos desde que ocurren. Procesa 50,000 transacciones por minuto.

Para cada escenario responde:
- ¿Batch o streaming?
- ¿Data Warehouse, Data Lake o Lakehouse?
- ¿Qué herramientas elegirías?
- ¿Por qué?

Commit esperado:
```bash
git commit -m "docs: add stack selection exercise"
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Rama y carpeta con formato correcto | `feature/semana01-conceptos-<nombre>` + carpeta `<nombre>/` | 10% |
| Mínimo 6 commits con mensajes claros | Un commit por sección o grupo de secciones | 15% |
| Conceptos 1–8 respondidos | Con definición, utilidad y ejemplo | 25% |
| Tabla de 10 conceptos adicionales completa | Todos los conceptos de la tabla | 15% |
| Caso práctico | Coherente y bien razonado | 10% |
| Diagrama de arquitectura | Flujo completo con herramientas | 10% |
| Tabla de roles del equipo de datos | Todos los roles completados | 10% |
| Ejercicio "Elige tu stack" | Ambos escenarios justificados | 5% |

---

## Lo que NO se debe hacer

- No copiar y pegar definiciones de Wikipedia sin reescribirlas con tus palabras
- No hacer un solo commit con todo el trabajo
- No dejar secciones vacías o con "pendiente"
- No abrir el PR hacia `main`

---

## Referencias

- [GITFLOW.md](../../GITFLOW.md) — flujo de trabajo con Git en este repositorio
- [Actividad 01](../actividad_01/README.md) — repaso del flujo de Git si tienes dudas
