# CONTRIBUTING — Guía para Instructores y Colaboradores

Este documento explica cómo mantener y extender el bootcamp de forma coherente. Está dirigido a instructores o colaboradores que trabajen sobre el repositorio después de la primera cohorte.

---

## Principios de diseño que deben mantenerse

Antes de añadir o modificar contenido, entiende estas decisiones de fondo (ver [DESIGN.md](DESIGN.md) para el razonamiento completo):

1. **PySpark antes que SQL.** Los estudiantes aprenden a procesar datos a escala antes de consultar tablas pre-construidas. No invertir ese orden.
2. **Cada semana construye sobre la anterior.** Los datos producidos en una semana son consumidos en la siguiente. No interrumpir esa cadena.
3. **Schemas explícitos siempre.** Toda tabla Delta se guarda con nombre calificado: `bronze.x`, `silver.x`, `gold.x`. Nunca al schema `default`.
4. **Duración estimada ~4h por actividad.** Las actividades no son tutoriales rápidos — incluyen investigación, decisiones de diseño y criterios de evaluación.
5. **Async por defecto.** Todo el contenido debe poder completarse sin depender de una sesión sincrónica. Las sesiones de revisión (2-3pm) son complemento, no requisito.

---

## Estructura del repositorio

```
semana_XX/
    actividades/
        actividad_01/README.md    ← instrucciones completas, autónomas
        actividad_02/README.md
        actividad_03/README.md
        actividad_04/README.md
    documentos/                   ← material de referencia de la semana
    laboratorios/                 ← ejercicios opcionales adicionales
    proyecto/README.md            ← proyecto integrador de la semana
```

Las semanas vacías tienen `.gitkeep` en las carpetas para mantener la estructura. No eliminarlos.

---

## Cómo añadir una nueva semana

### 1. Crear la rama correcta

```bash
git checkout develop
git pull origin develop
git checkout -b docs/week_X_activities
```

### 2. Usar la plantilla de actividad

Cada `README.md` de actividad debe tener estas secciones en este orden:

```markdown
# Actividad XX — Semana XX: <Título>

**Semana:** XX
**Tema:** <una línea>
**Nivel:** Junior | Junior-Intermedio | Intermedio
**Modalidad:** Individual
**Entorno:** <Databricks CE | Databricks Enterprise | Microsoft Fabric>

---

## Contexto
## Dataset
## Material de estudio previo
## Instrucciones Git
## Parte 1 — ...
## Parte N — ...
## Entrega en Git
## Criterios de evaluación
## Referencias
```

La sección **Contexto** debe explicar por qué esta actividad importa y cómo conecta con lo anterior. No es opcional.

### 3. Convenciones de datasets

- Los archivos de datos nunca van en el repo (ver `.gitignore`)
- Los datasets van en Google Drive bajo la estructura: `data_engineering_files/semana_XX_actividad_XX/`
- El link de Drive en cada actividad debe apuntar a esa carpeta específica, no a la raíz del Drive
- Documenta en la actividad: nombre del archivo, formato, tamaño aproximado, número de filas si aplica

### 4. Convenciones de tablas Delta

| Capa | Prefijo | Ejemplo |
|------|---------|---------|
| Ingesta cruda | `bronze.` | `bronze.transactions` |
| Limpieza y enriquecimiento | `silver.` | `silver.transactions` |
| Agregaciones de negocio | `gold.` | `gold.fraude_por_categoria` |
| Vistas de consumo | `gold.v_` | `gold.v_analisis_fraude` |

Los schemas se crean siempre con `CREATE SCHEMA IF NOT EXISTS` al inicio del notebook Bronze de cada semana.

### 5. Actualizar el README raíz

Cuando una semana esté completa, añadir su sección en [README.md](README.md) siguiendo el mismo formato que las semanas anteriores:

```markdown
### Semana XX — <Título>

Dataset: <descripción del dataset>
Tecnologías: <lista>

| Actividad | Tema | Descripción |
|-----------|------|-------------|
| [Actividad 01](...) | ... | ... |
...
```

Mover esa semana fuera de la tabla "Próximamente".

### 6. PR hacia develop — no hacia main

```bash
git push origin docs/week_X_activities
# PR: docs/week_X_activities → develop
# develop → main solo cuando la semana esté validada por al menos un estudiante piloto
```

---

## Cómo modificar contenido existente

- Usa una rama `fix/` para correcciones de errores en actividades ya entregadas
- Usa una rama `docs/` para mejoras de documentación o adición de contenido
- Nunca modificar actividades de una semana en curso sin consenso del instructor responsable
- Si un cambio es retroactivo (afecta cómo los estudiantes ya procesaron sus datos), documenta la versión en [CHANGELOG.md](CHANGELOG.md)

---

## Cómo escalar el bootcamp a nuevas cohortes

El modelo de fork está pensado así:

1. **Este repo** (`inetum_data_engineer_bootcamp`) es el repositorio instructor — solo instructores hacen PR aquí
2. **Cada cohorte** hace fork de este repo. El fork es el espacio de trabajo de los estudiantes
3. Los estudiantes trabajan en el fork, nunca hacen PR al repo instructor
4. Al terminar cada cohorte, los ajustes de diseño (fixes, mejoras de actividades) se incorporan al repo instructor vía PR desde la rama `fix/` o `docs/` correspondiente

Para lanzar una nueva cohorte:
```bash
# Desde la UI de GitHub: Fork del repo instructor
# Ajustar el README del fork para la nueva cohorte (fechas, grupo, instructor)
# NO modificar actividades ya validadas sin abrir primero un issue en el repo instructor
```

---

## Criterios de calidad mínimos para una actividad

Antes de hacer PR, verifica:

- [ ] El contexto explica por qué esta actividad importa y conecta con la semana anterior
- [ ] El dataset tiene link de Drive funcionando y descripción de archivos
- [ ] El material de estudio previo tiene al menos 4 preguntas de investigación
- [ ] Las instrucciones Git incluyen nombre de rama y estructura de carpeta `<tu-nombre>/`
- [ ] Cada parte tiene al menos un commit esperado documentado
- [ ] Los criterios de evaluación suman 100% y cada criterio tiene descripción clara
- [ ] Hay al menos una referencia a documentación oficial
- [ ] La actividad está referenciada en el README raíz

---

## Contacto y decisiones de diseño

Para cambios que afecten la arquitectura pedagógica del bootcamp (orden de semanas, stack tecnológico, modelo de datos base), abrir un issue en el repo antes de implementar. Las decisiones de diseño fundamentales están documentadas en [DESIGN.md](DESIGN.md).
