# CHANGELOG — Inetum Data Engineer Bootcamp

Registro de cambios relevantes por cohorte. El objetivo es que un instructor nuevo pueda entender qué funcionó, qué se ajustó y por qué, sin necesidad de leer el historial completo de Git.

El formato de cada entrada sigue: **qué cambió**, **por qué**, **impacto en cohortes futuras**.

---

## [Cohorte 1] — Abril 2026

**Instructor:** Jose Barrera  
**Grupo:** ~20 personas (mix de estadísticos, economistas e ingenieros de software en transición)  
**Modalidad:** Full-time 5×8h, asíncrono con revisión sincrónica 2-3pm  
**Entorno semanas 1-3:** Databricks Community Edition

### Semanas desarrolladas

- Semana 01: Fundamentos (Git, conceptos DE, formatos, Medallón)
- Semana 02: PySpark avanzado + pipeline de fraude
- Semana 03: SQL avanzado sobre tablas Delta (consumidor, no constructor)

### Semanas pendientes de desarrollo

- Semanas 04-12: estructura de carpetas creada, contenido pendiente

### Dataset financiero — decisión de selección

**Dataset:** Financial Transactions (Caixabank Tech) — 1.42GB, 5 archivos  
**Alternativas evaluadas:**
- Vehicle Sales Data (88MB, 1 tabla) — descartado por no tener JOINs
- Retail Transactional Dataset (80MB, 1 tabla) — descartado por ser flat
- **Financial Transactions — seleccionado** por: JOINs reales entre 5 tablas, mix CSV + JSON, tamaño suficiente para que PySpark justifique su uso, narrativa de fraude atractiva para perfiles heterogéneos

### Cambios de diseño aplicados durante desarrollo

**Schemas explícitos bronze/silver/gold**
- Motivo: sin schemas calificados (`bronze.transactions`), todo iba al schema `default` y semana 03 no podría hacer `SELECT * FROM silver.transactions`
- Cambio aplicado en: Act 04 semana 02 + Proyecto semana 02
- Impacto futuro: es un prerequisito para que semana 03 funcione sin reconfigurar nada

**PySpark SQL sobre pandasql**
- pandasql eliminado de todas las actividades por inestabilidad en Databricks CE
- Reemplazado por `spark.sql()` + `createOrReplaceTempView()` como patrón canónico

**Duración de actividades: de ~2h a ~4h**
- Todas las actividades de semana 01 extendidas con: investigación adicional (git stash/revert, arquitecturas de referencia), roles en el equipo de datos, escenarios de elección de stack
- Motivo: el grupo tiene backgrounds heterogéneos — los más avanzados necesitan profundidad, no solo ejercicios rápidos

**Semana 03 como "consumidor" de semana 02**
- Decisión deliberada: los estudiantes no reconstruyen la pipeline en semana 03, usan las tablas que dejaron en semana 02
- Esto simula el flujo real: Data Engineering entrega → Analytics consume
- Prerequisito: las tablas de semana 02 deben estar en el workspace antes de empezar semana 03

### Problemas conocidos pendientes de validar (Cohorte 1)

- [ ] El paso de subida de archivos al FileStore de Databricks no está documentado en ninguna actividad — el junior piloto debe confirmar si hay gap aquí
- [ ] `QUALIFY` en semana 03 Act 03 depende de la versión de Databricks Runtime — validar compatibilidad
- [ ] `inferSchema=true` sobre `transactions_data.csv` (1.42GB) puede ser lento o fallar en clusters gratuitos de CE — considerar schema explícito o muestra
- [ ] Persistencia de tablas Delta entre sesiones en Databricks CE — confirmar que el metastore sobrevive al apagado del cluster

---

## Plantilla para cohortes futuras

```markdown
## [Cohorte N] — Mes Año

**Instructor:** 
**Grupo:** 
**Modalidad:** 
**Entorno:**

### Semanas desarrolladas

### Cambios respecto a cohorte anterior

### Problemas encontrados y soluciones aplicadas

### Recomendaciones para la siguiente cohorte
```

---

## Convención de versiones

No usamos semver para el contenido — usamos `Cohorte N` como identificador. Un cambio es relevante para el CHANGELOG si:

- Modifica la secuencia pedagógica de una semana
- Cambia el dataset o la tecnología base
- Resuelve un problema que bloqueó a más de un estudiante
- Introduce un nuevo prerequisito entre semanas
