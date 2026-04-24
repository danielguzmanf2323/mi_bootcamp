# Databricks Academy — Data Engineering Learning Path

**Semana:** 02  
**Obligatorio:** Sí — forma parte de la calificación de la semana  
**Plataforma:** Databricks Free Edition → Home → Learn → Data Engineering

---

## Instrucciones

Durante la semana 02 debes completar los 12 videos del path **"Data Engineering"** disponible
en la pantalla de inicio de tu cuenta Databricks Free Edition:

1. Inicia sesión en [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. En el menú lateral ve a **Home**
3. Busca la sección **Learn** y selecciona **Data Engineering**
4. Completa los 12 videos en orden — están agrupados en 4 módulos

---

## Lista de videos

### Módulo 1 — Data Engineering Basics

| # | Título | Duración | ¿Completado? |
|---|--------|----------|--------------|
| 1 | Data Engineering Basics | 5 min | ☐ |
| 2 | Intro to Lakeflow | 5 min | ☐ |

### Módulo 2 — Ingestion and Transformation

| # | Título | Duración | ¿Completado? |
|---|--------|----------|--------------|
| 3 | Ingestion with Lakeflow Connect | 5 min | ☐ |
| 4 | Ingesting Data into Delta Lake | 18 min | ☐ |
| 5 | Data Transformation with the Medallion Architecture | 5 min | ☐ |
| 6 | Transforming Data with the Medallion Architecture | 11 min | ☐ |
| 7 | Ingesting and Manipulation Lab | 2 min | ☐ |

### Módulo 3 — Pipelines

| # | Título | Duración | ¿Completado? |
|---|--------|----------|--------------|
| 8 | ETL with Lakeflow Spark Declarative Pipelines | 2 min | ☐ |
| 9 | Creating and Managing Lakeflow Spark Declarative Pipelines | 15 min | ☐ |
| 10 | Monitoring and Optimizing Pipelines | 6 min | ☐ |

### Módulo 4 — Orchestration

| # | Título | Duración | ¿Completado? |
|---|--------|----------|--------------|
| 11 | Orchestration with Lakeflow Jobs | 4 min | ☐ |
| 12 | Creating a Simple Lakeflow Job | 10 min | ☐ |

**Tiempo total estimado:** ~88 minutos

---

## Entrega de evidencia

Crea una carpeta con tu nombre dentro de `semana_02/documentos/evidencia_academy/` y sube:

### 1. Este archivo completado

Copia este archivo como `databricks_academy_<tu-nombre>.md`, marca los videos completados
con `☑` y responde brevemente en la sección de reflexión al final.

### 2. Captura de pantalla del progreso

Toma una captura de pantalla de la pantalla de progreso en Databricks Academy
(donde se ven los videos completados) y adjúntala al mismo directorio:

```
semana_02/documentos/evidencia_academy/<tu-nombre>/
├── databricks_academy_<tu-nombre>.md
└── progreso_academy_<tu-nombre>.png
```

Para insertar la imagen en el markdown:
```markdown
![Progreso Databricks Academy](progreso_academy_<tu-nombre>.png)
```

---

## Resumen de videos (completar con tus propias palabras)

Escribe 3-5 líneas por video — no copies la descripción del curso, usa tus propias palabras.

### Módulo 1 — Data Engineering Basics

**Video 1 — Data Engineering Basics**
> (¿Qué hace un Data Engineer? ¿Qué herramientas usa? ¿Qué aprendiste de nuevo?)

**Video 2 — Intro to Lakeflow**
> (¿Qué es Lakeflow? ¿Qué componentes tiene? ¿Qué problema resuelve en comparación con hacer todo a mano?)

---

### Módulo 2 — Ingestion and Transformation

**Video 3 — Ingestion with Lakeflow Connect**
> (¿Qué es Lakeflow Connect? ¿Qué fuentes de datos soporta? ¿Cómo se diferencia de leer un CSV manualmente?)

**Video 4 — Ingesting Data into Delta Lake**
> (¿Qué pasos sigue una ingesta en Delta Lake? ¿Qué opciones de escritura viste — overwrite, append, merge?)

**Video 5 — Data Transformation with the Medallion Architecture**
> (¿Qué es la arquitectura Medallón? ¿Qué responsabilidad tiene cada capa — Bronze, Silver, Gold?)

**Video 6 — Transforming Data with the Medallion Architecture**
> (¿Qué transformaciones concretas mostraron en Silver? ¿Cómo se escribe desde Bronze hacia Silver en el ejemplo?)

**Video 7 — Ingesting and Manipulation Lab**
> (¿Qué hiciste en el lab? ¿Cuál fue el paso más útil para ti?)

---

### Módulo 3 — Pipelines

**Video 8 — ETL with Lakeflow Spark Declarative Pipelines**
> (¿Qué es una Declarative Pipeline? ¿En qué se diferencia de un notebook normal?)

**Video 9 — Creating and Managing Lakeflow Spark Declarative Pipelines**
> (¿Qué pasos siguieron para crear la pipeline? ¿Qué interfaz usaron para monitorearla?)

**Video 10 — Monitoring and Optimizing Pipelines**
> (¿Qué métricas se monitorean? ¿Qué opciones de optimización mencionaron?)

---

### Módulo 4 — Orchestration

**Video 11 — Orchestration with Lakeflow Jobs**
> (¿Qué es un Job en Databricks? ¿Cómo se diferencia de ejecutar un notebook a mano?)

**Video 12 — Creating a Simple Lakeflow Job**
> (¿Qué pasos seguiste para crear el Job? ¿Qué opciones de schedule viste?)

---

## Reflexión final (completar antes del PR)

**1. ¿Qué es Lakeflow y qué problema resuelve en comparación con hacer pipelines "a mano" con notebooks?**

> (tu respuesta aquí)

**2. ¿Qué diferencia hay entre un Lakeflow Job y un Lakeflow Spark Declarative Pipeline? ¿Cuándo usarías cada uno?**

> (tu respuesta aquí)

**3. ¿Qué concepto de los 12 videos te costó más entender? ¿Por qué?**

> (tu respuesta aquí)

**4. ¿Qué aspecto de los videos crees que vas a usar más en las actividades del bootcamp?**

> (tu respuesta aquí)

---

## Criterio de evaluación

| Criterio | Puntaje dentro de semana 02 |
|----------|----------------------------|
| 12 videos completados (captura de pantalla del progreso) | 5% |
| Resumen de cada video escrito con tus propias palabras | 5% |
| Reflexión final respondida con criterio técnico | 5% |

> Los videos de **Módulo 3 (Pipelines)** y **Módulo 4 (Orchestration)** los profundizarás
> en semanas 04 y 05 respectivamente — por ahora es suficiente con verlos una vez para
> tener contexto. No te preocupes si no entiendes todo aún.
