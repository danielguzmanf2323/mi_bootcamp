# Proyecto Semana 10 — Architecture Decision Record (ADR)

**Semana:** 10  
**Modalidad:** Individual  
**Entregable:** Documento técnico escrito — no hay código

---

## Qué es un ADR

Un **Architecture Decision Record** es un documento corto que registra
una decisión arquitectónica importante: el contexto, las opciones evaluadas,
la decisión tomada y las consecuencias. Es un artefacto estándar en equipos
de ingeniería — documentar por qué elegiste algo es tan importante como saber usarlo.

En esta actividad, tú eres el Data Engineer al que el cliente le pide que
evalúe y recomiende un stack tecnológico para su nuevo proyecto de datos.

---

## El caso de negocio

**Cliente:** cadena de retail con 200 tiendas en España y Portugal  
**Situación actual:** datos en Excel y un ERP on-premise (SAP), sin plataforma de datos

**Requisitos del proyecto:**

| Requisito | Detalle |
|-----------|---------|
| Volumen inicial | ~50 GB de histórico de ventas (5 años), crecimiento de ~500 MB/mes |
| Fuentes de datos | SAP (JDBC), archivos CSV desde TPV de tiendas, API de proveedor logístico |
| Latencia aceptable | Reportes de ventas: T+1 (datos del día anterior). Alertas de stock: < 1 hora |
| Equipo técnico del cliente | 2 analistas con SQL básico, sin experiencia en cloud ni Spark |
| Presupuesto cloud | Moderado — el cliente quiere empezar sin sobreescalar |
| Stack del cliente | Microsoft 365 + Azure AD ya contratados. El CTO prefiere minimizar vendors |
| Restricciones | Datos de clientes no pueden salir de la UE. Cumplimiento GDPR requerido |

---

## Tu entregable

Un documento `ADR_stack_retail_<tu-nombre>.md` con esta estructura:

---

### 1. Contexto

Describe el problema que hay que resolver y las restricciones más relevantes.
No copies los requisitos — sintetízalos con tus palabras, destacando lo que
más condiciona la decisión técnica.

---

### 2. Opciones evaluadas

Evalúa al menos **3 opciones** de stack. Por cada una:
- Qué plataformas incluye (storage, compute, orquestación, visualización)
- Por qué encaja o no con los requisitos del cliente

Ejemplo de opciones a considerar (no son las únicas):
- **Opción A:** Databricks + ADLS Gen2 + Power BI
- **Opción B:** Microsoft Fabric (all-in-one)
- **Opción C:** Snowflake + Azure Data Factory + Power BI
- **Opción D:** AWS (S3 + Glue + Athena + QuickSight)

---

### 3. Decisión

Elige una opción y justifícala. La justificación debe responder a:
- ¿Por qué esta opción sobre las demás?
- ¿Qué requisito fue el más determinante en tu decisión?
- ¿Qué sacrificias con esta elección?

No hay respuesta correcta — se evalúa la solidez del razonamiento.

---

### 4. Consecuencias

¿Qué implica esta decisión a largo plazo?
- Qué skills necesitará el equipo del cliente
- Qué limitaciones tendrá el sistema si el volumen crece 10x
- Qué debería revisarse en 12-18 meses

---

### 5. Riesgos y mitigaciones

Identifica al menos 2 riesgos técnicos o de negocio de tu decisión
y cómo los mitigarías.

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-adr-<tu-nombre>
```

Carpeta de entrega:
```
semana_10/proyecto/<tu-nombre>/
ADR_stack_retail_<tu-nombre>.md
```

```bash
git push origin feature/semana10-adr-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] ADR Stack Retail — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Contexto sintetizado con restricciones clave | No es copiar los requisitos — es interpretarlos | 15% |
| [Requerido] 3 opciones evaluadas con pros/contras reales | Argumentos técnicos, no genéricos | 30% |
| [Requerido] Decisión justificada con requisito determinante identificado | "Elegí X porque el requisito Y hace que Z sea inviable" | 30% |
| [Requerido] Consecuencias y riesgos documentados | Al menos 2 riesgos con mitigación | 25% |
| **Total** | | **100%** |

---

> **Nota:** Este documento es el tipo de entregable que un Data Engineer senior
> produce en la fase de preventa o diseño de un proyecto. No es un examen —
> es una muestra de criterio técnico.
