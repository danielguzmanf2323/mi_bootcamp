# DESIGN — Decisiones de Diseño del Bootcamp

Este documento registra el "por qué" detrás de las decisiones pedagógicas y técnicas del bootcamp. Está pensado para que un instructor nuevo pueda entender la lógica del curso antes de modificarlo.

**Regla:** antes de cambiar algo que esté documentado aquí, leer primero esta sección y actualizar el documento si el cambio se aprueba.

---

## 1. Perfil del estudiante objetivo

El bootcamp está diseñado para profesionales en transición hacia Data Engineering, no para programadores desde cero ni para especialistas ya formados.

**Perfil típico de la primera cohorte:**
- Estadísticos, economistas, analistas de datos con algo de SQL o Python
- Ingenieros de software sin experiencia en datos a escala
- Conocimiento de herramientas de análisis (Excel, R, pandas) pero sin experiencia en Spark ni arquitecturas distribuidas
- Full-time durante el programa — no es un curso nocturno

**Implicación de diseño:** las actividades no pueden asumir fluencia en Python. El código debe estar casi completo, con huecos conceptuales que el estudiante completa, no scaffolding de función vacía que el estudiante llena desde cero.

---

## 2. Modelo de entrega asíncrono con revisión sincrónica

El bootcamp es **asíncrono por defecto**. Cada actividad debe poder completarse de forma autónoma, sin depender de que el instructor esté disponible.

La sesión diaria de 2-3pm es una revisión y espacio de preguntas, no la instrucción principal. Si un estudiante no puede asistir a la sesión sincrónica, no debe bloquearse.

**Implicación de diseño:**  
- Cada actividad tiene todo lo necesario en su README: instrucciones, código de referencia, material de estudio, criterios de evaluación
- La sección "Material de estudio previo" dirige a recursos externos para que el estudiante se auto-instrua antes de empezar
- No hay ejercicios que dependan de que el instructor explique algo primero

---

## 3. PySpark antes que SQL

La secuencia del bootcamp es intencionada: PySpark (semanas 01-02) antes de SQL (semana 03).

**Por qué no SQL primero:**
- SQL es más fácil de aprender, pero esconde la complejidad de lo que pasa debajo
- Si el estudiante aprende SQL primero, no entiende por qué existe PySpark ni qué problema resuelve
- En un entorno real de Data Engineering, el DE construye la pipeline (PySpark/Spark) y el analista consume (SQL). El bootcamp replica ese flujo

**Por qué PySpark sobre pandas:**
- pandas no escala — un dataset de 10GB en pandas falla, en PySpark funciona
- La transición de pandas a PySpark es difícil si se aprende pandas primero: hay que desaprender la ejecución ansiosa (eager execution) y el modelo de memoria
- Al aprender directamente PySpark, el estudiante entiende desde el principio lazy evaluation y el modelo distribuido

**Implicación de diseño:** semana 03 (SQL) usa las tablas que construyó semana 02 (PySpark). El estudiante experimenta el ciclo completo: construir → exponer → consumir.

---

## 4. Arquitectura Medallón desde semana 01

La arquitectura Bronze → Silver → Gold se introduce en semana 01 con un dataset simple (customers), antes de que haya complejidad técnica.

**Por qué tan temprano:**
- Es el patrón más universal en Data Engineering moderno (Delta Lake, dbt, Fabric)
- Aprenderlo con datos simples permite que en semana 02 el estudiante aplique el patrón con datos reales sin distraerse con el concepto
- Refuerza el principio de separación de responsabilidades: cada capa tiene una función específica y no se mezclan

**Regla que nunca debe romperse en ninguna actividad:**  
> Cada capa lee de la capa anterior, nunca del archivo fuente.

---

## 5. Diseño de datasets por semana

| Semana | Dataset | Justificación |
|--------|---------|---------------|
| 01 | customers (CSV/JSON/Parquet/Avro) + Maven Fuzzy Factory | Pequeño, controlado, permite enfocarse en herramientas sin distraerse con limpieza |
| 02-03 | Financial Transactions (1.42GB, 5 tablas) | Tamaño real (PySpark justifica su uso), múltiples tablas (JOINs reales), narrativa de fraude (engagement de perfiles heterogéneos), mix CSV+JSON |
| 04+ | Por definir | Debe introducir escenarios de ingesta (APIs, bases de datos relacionales, streams) |

**Criterios para seleccionar un dataset:**
1. Tiene al menos 2 tablas relacionadas (permite JOINs)
2. El tamaño justifica el uso de Spark sobre pandas (>100MB idealmente)
3. La narrativa es comprensible por alguien sin dominio del sector
4. Los datos tienen problemas de calidad realistas (nulos, tipos incorrectos, duplicados)

---

## 6. Schemas explícitos: bronze, silver, gold

Todas las tablas Delta se guardan con nombre calificado por schema: `bronze.transactions`, `silver.transactions`, `gold.fraude_por_categoria`.

**Por qué no usar el schema `default`:**
- Si todas las tablas van al schema `default`, en semana 03 no se puede hacer `SELECT * FROM silver.transactions` — el estudiante no sabe en qué schema buscar
- Los schemas refuerzan el concepto de capas: el estudiante ve claramente que `bronze` y `gold` son espacios separados con propósitos distintos
- En Databricks Enterprise y Unity Catalog, este patrón es el estándar — el bootcamp prepara para ese entorno

**Patrón de inicialización obligatorio al inicio de cada Bronze notebook:**
```python
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
```

---

## 7. Modelo de repositorio: instructor vs estudiantes

El repositorio tiene dos modos de uso:

**Modo desarrollo (este repo):**
- Solo instructores hacen PR aquí
- El contenido representa la versión validada del bootcamp
- Branches: `main` (estable), `develop` (integración), `docs/`, `feature/`, `fix/`

**Modo cohorte (fork del repo):**
- Cada cohorte hace fork de este repo
- Los estudiantes trabajan en el fork — crean sus propias ramas (`feature/semana0X-actividad0X-<nombre>`)
- Los estudiantes hacen PR dentro del fork, hacia `develop` del fork
- No hay PR de estudiantes hacia el repo instructor

**Por qué fork y no rama por estudiante en el mismo repo:**
- Con 20 estudiantes y 4 actividades por semana, el repo instructor tendría 80+ ramas activas en semana 01 — inmanejable
- El fork aísla el trabajo de cada cohorte — el historial queda limpio
- El instructor puede hacer PR de fixes desde el fork hacia el repo instructor sin mezclar trabajo de estudiantes

---

## 8. Criterios de evaluación en cada actividad

Cada actividad tiene una tabla de criterios de evaluación que suma 100%.

**Por qué son obligatorios:**
- El bootcamp es async — sin criterios explícitos, el estudiante no sabe qué se espera de él
- Los criterios también guían al instructor al revisar el PR: no hay ambigüedad sobre qué constituye una entrega completa
- En una cohorte de 20 personas, la revisión debe ser eficiente — los criterios permiten revisar en paralelo con el mismo estándar

**Niveles de prioridad en los criterios:**
- `[Requerido]` — sin esto, la entrega no está completa
- `[Recomendado]` — añade profundidad, se espera de perfiles más avanzados
- `[Opcional]` — para quienes terminan antes o quieren explorar más allá

---

## 9. Uso de Databricks Community Edition (semanas 1-3)

Databricks CE es gratuito y suficiente para los primeros conceptos. Las limitaciones conocidas:

- El cluster se apaga después de inactividad de ~2h
- La memoria del cluster gratuito es limitada (~15GB) — datasets muy grandes pueden fallar
- El metastore persiste si se usa Hive Metastore, pero hay comportamientos inconsistentes entre sesiones

**Implicación de diseño:**
- Semana 01 usa datasets pequeños (customers, ~2M filas) — no tiene problemas
- Semana 02 usa `transactions_data.csv` (1.42GB) — puede ser lento con `inferSchema`. Considerar schema explícito en cohortes futuras
- A partir de semana 03 o 04 (cuando esté disponible), migrar a Databricks Enterprise elimina estas limitaciones

**Decisión para cohortes futuras:** documentar en la actividad si el dataset puede necesitar una muestra para CE:
```python
# Si el cluster de CE tiene problemas de memoria, trabajar con muestra:
df_sample = df.sample(fraction=0.10, seed=42)
```

---

## 10. Semanas 04-12 — arco pendiente

El arco planificado (sujeto a revisión tras primera cohorte):

| Semana | Tema | Dependencia clave |
|--------|------|-------------------|
| 04 | Ingesta de datos (APIs, JDBC, archivos incrementales) | Requiere Databricks Enterprise o similar |
| 05 | dbt (data build tool) | Requiere haber solidificado SQL en semana 03 |
| 06 | Orquestación con Airflow | Requiere entender pipelines de semanas 01-04 |
| 07-08 | Microsoft Fabric | Requiere trial de 60 días — coordinar con Inetum |
| 09-11 | Proyecto final end-to-end | Requiere todo el stack anterior |
| 12 | Presentaciones | El proyecto final de cada estudiante |

**Decisión sobre dbt:** evaluar si adelantar a semana 04 en lugar de ingesta. dbt es SQL + pipeline structure — conecta directamente con semana 03 y tiene menor barrera de entrada que configurar conectores de ingesta.
