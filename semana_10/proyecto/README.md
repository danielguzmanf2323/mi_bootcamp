# Proyecto Semana 10 — Reflexión Comparativa de Plataformas

**Semana:** 10  
**Modalidad:** Individual  
**Entregable:** Documento de reflexión — no hay código

---

## Objetivo

Has trabajado en profundidad con Databricks (semanas 01-06), vas a trabajar con Fabric (semanas 07-09),
y esta semana te has orientado en Snowflake y el mapa multi-cloud.

Este documento es el momento de parar y reflexionar con criterio propio:
qué has aprendido de cada plataforma, dónde brilla cada una y dónde no.
No es un examen de contenido — es una muestra de cómo piensas como Data Engineer.

---

## Tu entregable

Un documento `reflexion_plataformas_<tu-nombre>.md` con las siguientes secciones:

---

### 1. Databricks — lo que aprendiste haciendo

Lleva 6 semanas usando Databricks. Más allá de repetir lo que hace,
describe tu experiencia real con la plataforma:

- ¿Qué te resultó más difícil de entender al principio y cómo lo superaste?
- ¿Hubo alguna decisión de diseño en tus actividades que tomaste mal y tuviste que corregir? ¿Cuál y por qué?
- ¿Qué feature de Databricks te parece más potente para un entorno de producción real?
- ¿Qué le falta o qué te generó fricción?

---

### 2. Microsoft Fabric — primeras impresiones

Llevas 3 semanas trabajando con Fabric. Compáralo con lo que ya conocías de Databricks:

- ¿Qué es más fácil en Fabric que en Databricks? ¿Por qué crees que es así?
- ¿Qué es más limitado o más rígido?
- ¿El modelo de OneLake + Shortcuts te parece una ventaja o una restricción? Justifica.
- ¿Para qué tipo de equipo o empresa crees que Fabric es la opción natural?

---

### 3. Snowflake — orientación rápida

Solo has tenido un día con Snowflake. Con esa perspectiva limitada pero fresca:

- ¿Qué te llamó más la atención de su arquitectura (separación cómputo/storage, virtual warehouses)?
- ¿En qué escenario concreto elegirías Snowflake sobre Databricks o Fabric?
- ¿Qué necesitarías aprender de Snowflake antes de recomendarlo a un cliente?

---

### 4. Tabla comparativa propia

Completa esta tabla con tu criterio — no hay respuestas correctas,
hay respuestas bien o mal justificadas:

| Criterio | Databricks | Microsoft Fabric | Snowflake |
|----------|-----------|-----------------|-----------|
| Curva de aprendizaje | | | |
| Mejor para pipelines complejos | | | |
| Mejor para analítica self-service | | | |
| Coste en equipos pequeños | | | |
| Integración con ecosistema Microsoft | | | |
| Control sobre la infraestructura | | | |
| Cuándo lo recomendarías | | | |

---

### 5. Una decisión honesta

Si mañana un cliente te pide que le ayudes a elegir plataforma para un proyecto nuevo,
y después de estas 10 semanas solo puedes recomendar **una** de las tres:

- ¿Cuál recomendarías para un equipo técnico con experiencia en Spark?
- ¿Cuál recomendarías para una empresa con todo en Microsoft 365?
- ¿Cuál recomendarías para un equipo pequeño que quiere empezar rápido con SQL?

No justifiques con los nombres de los features — justifica con lo que viviste.

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-reflexion-<tu-nombre>
```

Carpeta de entrega:
```
semana_10/proyecto/<tu-nombre>/
reflexion_plataformas_<tu-nombre>.md
```

```bash
git push origin feature/semana10-reflexion-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] Reflexión Comparativa — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Reflexión Databricks con experiencia propia | Menciona algo concreto — una dificultad real, una corrección real | 25% |
| [Requerido] Comparativa Fabric vs Databricks argumentada | No genérica — basada en lo que viviste en las actividades | 25% |
| [Requerido] Tabla comparativa completa con criterio propio | "Cuándo lo recomendarías" con justificación | 25% |
| [Requerido] Decisión final justificada con experiencia | "Lo elegí porque lo viví" — no "porque lo dice la documentación" | 25% |
| **Total** | | **100%** |

---

> Este tipo de reflexión es lo que diferencia a un Data Engineer que sabe usar herramientas
> de uno que sabe elegirlas. Ambas skills importan en un proyecto real.
