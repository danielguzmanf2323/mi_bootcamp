# Presentación Final — Semana 12

**Duración:** 20 minutos por alumno/pareja + 10 minutos de preguntas  
**Audiencia:** Instructores, compañeros, stakeholders de Inetum (si aplica)  
**Formato:** Demo en vivo del pipeline + slides opcionales

---

## Estructura de la presentación

### 1. Contexto y problema (3 min)
- ¿Qué dataset usaste y qué representa el negocio de Olist?
- ¿Qué pregunta de negocio responde tu pipeline?
- Una frase de por qué el problema importa

### 2. Arquitectura (4 min)
- Diagrama del pipeline completo (puede ser el del `arquitectura.md`)
- Stack tecnológico elegido y por qué
- Una decisión de diseño que tomaste y que no era obvia

### 3. Demo en vivo (8 min)

**Obligatorio mostrar:**
- [ ] El pipeline Bronze corriendo (aunque sea en replay) con datos llegando al ADLS
- [ ] Una tabla Silver con las transformaciones aplicadas (`display()` en Databricks)
- [ ] El star schema en Gold (`SHOW TABLES IN gold` + `SELECT COUNT(*)` de cada tabla)
- [ ] El OneLake Shortcut en Fabric apuntando a la Gold
- [ ] El report en Fabric con al menos 2 métricas de negocio

**Opcional pero valorado:**
- [ ] El componente de streaming en ejecución en tiempo real
- [ ] El Spark UI de una operación costosa

### 4. Calidad de datos (2 min)
- ¿Qué problemas de calidad encontraste en Olist?
- ¿Cómo los trataste? ¿Qué descartaste y por qué?

### 5. Retrospectiva (3 min)
- ¿Qué cambiarías si lo volvieras a hacer?
- ¿Qué parte del pipeline te costó más y qué aprendiste de eso?
- Una recomendación para la siguiente cohorte

---

## Entregables antes de la presentación

Antes del jueves (día de revisión con instructor), sube a tu carpeta de proyecto:

```
semana_12/proyecto/<tu-nombre>/
├── presentacion/
│   └── slides.<pdf|pptx|md>    ← si usas slides (opcional)
└── checklist_entrega.md        ← checklist firmado (ver abajo)
```

**Checklist de entrega:**

```markdown
# Checklist Entrega Final — <Tu Nombre>

## Pipeline
- [ ] Bronze: 9 tablas Delta con schema explícito y columna de auditoría
- [ ] Silver: tablas transformadas con delivery_delay_days calculado
- [ ] Gold: fact_orders + dim_customers + dim_sellers + dim_products + dim_date
- [ ] OneLake Shortcut configurado en Fabric
- [ ] Report en Fabric con 4+ métricas

## Documentación
- [ ] arquitectura.md con diagrama y decisiones técnicas
- [ ] modelo_dimensional.md con descripción de cada tabla y columna
- [ ] calidad_datos.md con anomalías encontradas y cómo se trataron
- [ ] README.md con guía de ejecución del pipeline

## Git
- [ ] Todos los notebooks committeados en la rama del proyecto
- [ ] PR abierto hacia develop con título [Proyecto Final] <Tu Nombre>
- [ ] Sin credenciales ni datasets subidos al repo
```

---

## Criterios de evaluación de la presentación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Demo en vivo completa | Los 5 puntos obligatorios mostrados | 40% |
| [Requerido] Arquitectura explicada con claridad | El diagrama se entiende sin ayuda externa | 20% |
| [Requerido] Decisión técnica propia justificada | "Elegí X porque Y" — no "lo hice así porque lo vimos en clase" | 20% |
| [Recomendado] Calidad de datos tratada honestamente | Qué descartaste y por qué, sin ocultar problemas | 10% |
| [Opcional] Retrospeciva con aprendizaje real | Algo concreto que cambiarías, no genérico | 10% |
| **Total** | | **100%** |

---

## Nota para el instructor

La presentación no es un examen de contenido — es una evaluación de criterio técnico.
Las preguntas deben ir al "por qué" de las decisiones, no al "qué" del código.

Ejemplos de preguntas buenas:
- "¿Por qué `fact_orders` tiene `payment_value` y no `price`?"
- "¿Qué pasaría con tu pipeline si Olist cambia el schema de `orders` mañana?"
- "¿Por qué elegiste MERGE INTO en Silver en lugar de overwrite?"
- "¿Qué harías diferente si el dataset fuera 100x más grande?"
