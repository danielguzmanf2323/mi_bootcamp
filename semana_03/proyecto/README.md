# Proyecto Integrador — Semana 03: Capa Analítica SQL para el Equipo de Fraude

**Semana:** 03  
**Tipo:** Proyecto integrador  
**Modalidad:** Individual (presentación en sesión sincrónica)  
**Entorno:** Databricks Community Edition

---

## Contexto

Semanas 01 y 02 fuiste Data Engineer: construiste pipelines, ingestaste datos, limpiaste y transformaste.

Esta semana el rol cambia. El equipo de fraude del banco te pide que les entregues:

1. Un conjunto de **vistas** bien nombradas sobre las que puedan trabajar sin tocar Bronze o Silver.
2. Un conjunto de **tablas Gold nuevas** respondiendo preguntas que las de semana 02 no cubrían.
3. Un **informe SQL** completo que demuestre los patrones del fraude usando las herramientas de semana 03 (CTEs, window functions, JOINs complejos).

Tu entrega final es SQL que cualquier analista del equipo pueda retomar, leer y extender.

---

## Prerrequisito

Las tablas de semana 02 disponibles:

```sql
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;
```

Si no las tienes, ejecuta los notebooks de Actividad 04 y el Proyecto de semana 02 primero.

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana03-proyecto-<tu-nombre>
```

Estructura esperada:

```
semana_03/proyecto/<tu-nombre>/
    01_vistas_<tu-nombre>.sql
    02_gold_nuevo_<tu-nombre>.sql
    03_informe_sql_<tu-nombre>.sql
    04_reflexion_<tu-nombre>.md
```

---

## Parte 1 — Capa de Vistas para el Equipo de Fraude

El equipo de fraude solo debe ver lo que necesita. Sin PII innecesaria, sin columnas técnicas, sin Silver completo.

Crea estas vistas. Cada una debe tener un comentario que explique para qué sirve.

### Vista 1: `gold.v_transacciones_fraude`

Solo transacciones con etiqueta de fraude confirmada:

```sql
CREATE OR REPLACE VIEW gold.v_transacciones_fraude
COMMENT 'Transacciones confirmadas como fraudulentas. Solo para equipo de fraude.'
AS
SELECT
    transaction_id,
    transaction_date,
    anio,
    mes,
    hora,
    dia_semana,
    es_fin_de_semana,
    ABS(amount) AS amount,
    merchant_name,
    merchant_category,
    mcc,
    card_type
FROM silver.transactions
WHERE is_fraud = 1;
```

### Vista 2: `gold.v_perfil_riesgo_usuario`

Perfil de riesgo de cada usuario, listo para usar en dashboards:

Debe incluir: `user_id`, `total_transacciones`, `total_fraudes`, `tasa_fraude_pct`, `monto_total`, `monto_fraudulento`, `primera_transaccion`, `ultima_transaccion`, `dias_activo`, `categoria_riesgo` (Alta/Media/Baja, define tu criterio).

### Vista 3: `gold.v_alertas_comercio`

Comercios donde la tasa de fraude supera 2x la media global:

```sql
CREATE OR REPLACE VIEW gold.v_alertas_comercio
COMMENT 'Comercios con tasa de fraude superior al doble de la media global.'
AS
WITH media_global AS (
    SELECT ROUND(SUM(is_fraud) / COUNT(*) * 100, 4) AS tasa_global
    FROM silver.transactions
    WHERE is_fraud IS NOT NULL
),
por_comercio AS (
    SELECT
        merchant_name,
        merchant_category,
        COUNT(*) AS total_transacciones,
        SUM(is_fraud) AS total_fraudes,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct
    FROM silver.transactions
    WHERE is_fraud IS NOT NULL
    GROUP BY merchant_name, merchant_category
    HAVING COUNT(*) >= 50  -- mínimo volumen para que el dato sea representativo
)
SELECT
    c.*,
    g.tasa_global,
    ROUND(c.tasa_fraude_pct / g.tasa_global, 2) AS ratio_vs_media
FROM por_comercio c
CROSS JOIN media_global g
WHERE c.tasa_fraude_pct > g.tasa_global * 2
ORDER BY c.tasa_fraude_pct DESC;
```

Verifica con:
```sql
SELECT COUNT(*) AS comercios_en_alerta FROM gold.v_alertas_comercio;
SELECT * FROM gold.v_alertas_comercio LIMIT 10;
```

Commit esperado:
```bash
git commit -m "feat: create gold views for fraud team - 3 views"
```

---

## Parte 2 — Nuevas Tablas Gold (SQL puro)

Las tablas Gold de semana 02 las creaste con PySpark. Ahora crea 3 tablas Gold nuevas usando solo SQL.

### Tabla 1: `gold.fraude_por_franja_horaria`

Consolida el fraude por franja del día (madrugada, mañana, tarde, noche) y día laborable vs fin de semana.

```sql
CREATE OR REPLACE TABLE gold.fraude_por_franja_horaria AS
SELECT
    CASE
        WHEN hora BETWEEN 0 AND 5   THEN 'Madrugada (00-05h)'
        WHEN hora BETWEEN 6 AND 11  THEN 'Mañana (06-11h)'
        WHEN hora BETWEEN 12 AND 17 THEN 'Tarde (12-17h)'
        WHEN hora BETWEEN 18 AND 23 THEN 'Noche (18-23h)'
    END AS franja_horaria,
    es_fin_de_semana,
    COUNT(*) AS total_transacciones,
    SUM(is_fraud) AS total_fraudes,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct,
    ROUND(AVG(ABS(amount)), 2) AS ticket_promedio
FROM silver.transactions
WHERE is_fraud IS NOT NULL
GROUP BY 1, 2
ORDER BY tasa_fraude_pct DESC;
```

### Tabla 2: `gold.evolucion_mensual_fraude`

Evolución mes a mes para detectar tendencias:

```sql
CREATE OR REPLACE TABLE gold.evolucion_mensual_fraude AS
WITH mensual AS (
    SELECT
        anio,
        mes,
        COUNT(*) AS total_transacciones,
        SUM(is_fraud) AS total_fraudes,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 4) AS tasa_fraude_pct,
        ROUND(SUM(ABS(amount)), 2) AS monto_total
    FROM silver.transactions
    WHERE is_fraud IS NOT NULL
    GROUP BY anio, mes
)
SELECT
    *,
    LAG(tasa_fraude_pct, 1) OVER (ORDER BY anio, mes) AS tasa_mes_anterior,
    ROUND(tasa_fraude_pct - LAG(tasa_fraude_pct, 1) OVER (ORDER BY anio, mes), 4)
        AS variacion_tasa_pct
FROM mensual
ORDER BY anio, mes;
```

### Tabla 3: `gold.segmentos_usuario_riesgo`

Segmentación de usuarios en clusters de riesgo usando cuartiles:

```sql
CREATE OR REPLACE TABLE gold.segmentos_usuario_riesgo AS
WITH metricas_usuario AS (
    SELECT
        client_id AS user_id,
        COUNT(*) AS total_transacciones,
        SUM(is_fraud) AS total_fraudes,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct,
        ROUND(SUM(ABS(amount)), 2) AS monto_total,
        ROUND(AVG(ABS(amount)), 2) AS ticket_promedio,
        COUNT(DISTINCT card_type) AS num_tipos_tarjeta,
        COUNT(DISTINCT merchant_category) AS num_categorias
    FROM silver.transactions
    WHERE is_fraud IS NOT NULL
    GROUP BY client_id
    HAVING COUNT(*) >= 10
),
con_cuartiles AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY tasa_fraude_pct DESC) AS cuartil_fraude,
        NTILE(4) OVER (ORDER BY monto_total DESC) AS cuartil_gasto
    FROM metricas_usuario
)
SELECT
    *,
    CASE
        WHEN cuartil_fraude = 1 AND cuartil_gasto = 1 THEN 'Alto riesgo - Alto valor'
        WHEN cuartil_fraude = 1 THEN 'Alto riesgo - Bajo valor'
        WHEN cuartil_gasto = 1 THEN 'Bajo riesgo - Alto valor'
        ELSE 'Perfil estándar'
    END AS segmento
FROM con_cuartiles;
```

Commit esperado:
```bash
git commit -m "feat: 3 new gold tables created with SQL - temporal, evolution, segments"
```

---

## Parte 3 — Informe SQL: 5 Hallazgos

Escribe un notebook SQL documentado. Para cada hallazgo: una query + una celda markdown con la conclusión de negocio.

Los hallazgos deben ser **nuevos** — no repitas lo que ya está en las tablas Gold de semana 02.

### Hallazgo obligatorio 1: ¿El fraude es predecible por secuencia?

Usando LAG y LEAD, analiza si las transacciones fraudulentas tienden a ocurrir en grupos (varias seguidas del mismo usuario) o de forma aislada.

### Hallazgo obligatorio 2: ¿Los comercios con más fraude también tienen más volumen?

¿Hay correlación entre el volumen de transacciones de un comercio y su tasa de fraude? ¿O el fraude se concentra en comercios pequeños?

### Hallazgo obligatorio 3: ¿El fraude tiene estacionalidad anual?

Usando `gold.evolucion_mensual_fraude`, identifica si hay meses del año donde el fraude siempre es mayor, independientemente del año.

### Hallazgos 4 y 5: libres `[Recomendado]`

Dos hallazgos de tu elección usando las herramientas de la semana. Deben estar justificados con SQL y conclusión escrita.

---

## Parte 4 — Reflexión final

En `04_reflexion_<tu-nombre>.md`, responde:

**Sobre SQL vs PySpark:**
- ¿Qué operaciones hiciste en semana 03 que habrían sido más difíciles en PySpark?
- ¿Qué operaciones de semana 02 (PySpark) serían difíciles de expresar en SQL?

**Sobre la arquitectura:**
- ¿Qué ventaja tiene exponer vistas en lugar de tablas Gold directamente al equipo de fraude?
- Si tuvieras que añadir un nuevo dataset (por ejemplo, datos de geolocalización de los comercios), ¿en qué capa lo insertarías y por qué?

**Sobre calidad de SQL:**
- ¿Cómo nombrarías las CTEs para que otro analista entienda tu query sin leer los comentarios?
- ¿Qué harías diferente en semana 02 sabiendo lo que sabes ahora?

---

## Entrega en Git

```bash
git add semana_03/proyecto/<tu-nombre>/
git commit -m "feat: semana 03 project - sql views, gold tables, hallazgos - <tu-nombre>"
git push origin feature/semana03-proyecto-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 03 - Proyecto] Capa Analítica SQL — <Tu Nombre>
```

En el PR:
- ¿Cuántos comercios están en alerta según `gold.v_alertas_comercio`?
- El hallazgo más sorprendente del informe
- ¿Qué segmento de usuario tiene la tasa de fraude más alta?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| 3 vistas creadas con COMMENT | Correctas y útiles para el equipo | 20% |
| 3 tablas Gold nuevas en SQL | Con CTEs o window functions | 25% |
| Hallazgos 1, 2, 3 obligatorios | Query + conclusión de negocio | 30% |
| Hallazgos 4 y 5 libres | Creatividad + rigor analítico | 10% |
| Reflexión: 6 preguntas respondidas | Con criterio técnico | 10% |
| Git: commits por parte, PR bien descrito | Mínimo 4 commits | 5% |

---

## Referencias

- [Actividad 04 semana 03](../actividades/actividad_04/README.md) — CREATE VIEW
- [Actividad 03 semana 03](../actividades/actividad_03/README.md) — CTEs y window functions
- [Proyecto semana 02](../../semana_02/proyecto/README.md) — tablas Gold base que consumes aquí
