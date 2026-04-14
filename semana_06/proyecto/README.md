# Proyecto Semana 06 — Pipeline de Streaming con Optimización Documentada

**Semana:** 06  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise

---

## Objetivo

Construir un pipeline de streaming end-to-end que integre todo lo aprendido
en la semana: Auto Loader en modo streaming, MERGE INTO para mantener estado,
optimización de la Gold table con ZORDER, y un informe técnico de decisiones
con evidencia del Spark UI.

Este proyecto replica un patrón real de producción: ingesta continua
+ estado actualizado + capa analítica optimizada.

---

## Dataset

Financial Transactions — mismo dataset de semanas 02-05.
Las tablas `bronze.*`, `silver.*` y `gold.*` de semanas anteriores
son el punto de partida. El pipeline de este proyecto corre **sobre** esa base,
simulando la llegada continua de nuevas transacciones.

---

## Contexto

El equipo de negocio necesita un dashboard de fraude que se actualice
en tiempo real (o cuasi-real) a medida que llegan nuevas transacciones.
Tu tarea es construir el pipeline que alimenta ese dashboard:
desde la llegada de los archivos hasta la Gold table optimizada
que el equipo de analítica puede consultar.

---

## Arquitectura del pipeline

```
Volumen ADLS (landing/)
        │
        │  Auto Loader streaming
        ▼
bronze.transactions_stream      ← append, checkpointed
        │
        │  MERGE INTO (upsert con deduplicación)
        ▼
silver.transactions_stream      ← estado siempre actualizado
        │
        │  Streaming aggregation + watermark
        ▼
gold.fraude_streaming           ← ventanas de 1h, optimizada con ZORDER
```

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana06-proyecto-<tu-nombre>
```

Carpeta de entrega:
```
semana_06/proyecto/<tu-nombre>/
```

Entregables:
- `pipeline_streaming.ipynb` — notebook con todo el pipeline
- `informe_<tu-nombre>.md` — decisiones técnicas + evidencia Spark UI

---

## Requisitos técnicos

### Parte 1 — Bronze: ingesta en streaming

- Auto Loader leyendo desde `/Volumes/main/landing/raw/transactions/`
- Schema explícito (no `inferSchema`)
- Trigger `processingTime` de 30 segundos
- Checkpoint en `/Volumes/main/checkpoints/proyecto_s06/bronze/`
- Escribe a `bronze.transactions_stream` en modo **append**

Verificación requerida: parar y reiniciar el stream. Contar registros antes y después.
Documentar que no hay duplicados.

---

### Parte 2 — Silver: MERGE INTO con deduplicación

- Lee desde `bronze.transactions_stream` (no en streaming — batch sobre la Bronze)
- Deduplica el batch por `transaction_id` (window function, `row_number`)
- Aplica `MERGE INTO silver.transactions_stream`:
  - `WHEN MATCHED AND status <> source.status` → UPDATE
  - `WHEN NOT MATCHED` → INSERT
- Ejecutar como Databricks Job con trigger `availableNow` (no streaming continuo)

Justifica en el informe por qué esta capa usa batch sobre Bronze y no streaming directo.

---

### Parte 3 — Gold: agregación en streaming con watermark

- Lee desde `bronze.transactions_stream` en modo streaming
- Aplica watermark de 1 hora
- Agrega en ventanas de 1 hora por `category`:
  - `num_transactions`
  - `total_amount`
  - `num_fraudulent` (si tienes esa columna disponible)
- Escribe a `gold.fraude_streaming` en modo **update**
- Checkpoint en `/Volumes/main/checkpoints/proyecto_s06/gold/`

---

### Parte 4 — Optimización de la Gold table

Una vez que la Gold table tiene datos:

```sql
-- Optimizar para el patrón de query más frecuente del dashboard
OPTIMIZE gold.fraude_streaming ZORDER BY (window, category)
```

Ejecuta la query del dashboard antes y después del OPTIMIZE.
Mide tiempos. Documenta en el informe.

---

### Parte 5 — Informe técnico

El archivo `informe_<tu-nombre>.md` debe incluir:

1. **Arquitectura:** diagrama ASCII (como el de arriba) de tu pipeline real
2. **Decisiones técnicas** (mínimo 3):
   - ¿Por qué trigger `processingTime` en Bronze y `availableNow` en Silver?
   - ¿Por qué `outputMode("update")` en Gold y no `append`?
   - ¿Por qué `ZORDER BY (window, category)` y no otra columna?
3. **Evidencia de Spark UI:** screenshot del stage de streaming con métricas anotadas
4. **Resultado del OPTIMIZE:** número de archivos antes/después y mejora de tiempo de query

---

## Entrega en Git

```bash
git push origin feature/semana06-proyecto-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 06] Proyecto Streaming Pipeline — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Bronze streaming con checkpoint funcional | Parada y reinicio sin duplicados verificado | 20% |
| [Requerido] Silver con MERGE + deduplicación | Upsert correcto, condicional en update | 20% |
| [Requerido] Gold con watermark y agregaciones | outputMode correcto, tabla consultable | 20% |
| [Requerido] Informe con 3 decisiones técnicas justificadas | No basta describir — hay que justificar | 25% |
| [Recomendado] OPTIMIZE con evidencia antes/después | Tiempos medidos, Spark UI anotado | 15% |
| **Total** | | **100%** |
