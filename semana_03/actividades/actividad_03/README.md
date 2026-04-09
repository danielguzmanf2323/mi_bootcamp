# Actividad 03 — Semana 03: SQL Avanzado — CTEs y Window Functions

**Semana:** 03  
**Tema:** CTEs, subqueries anidadas y window functions en Spark SQL  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

Ya sabes filtrar, agrupar y joinear en SQL. Ahora el salto de calidad:

- **CTEs** (`WITH`): SQL que se lee como una historia, no como un laberinto de subqueries anidadas
- **Window Functions en SQL**: `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `SUM OVER` — el equivalente SQL exacto de lo que hiciste en PySpark semana 02

El objetivo de esta actividad es que puedas escribir SQL complejo que un analista senior pueda leer y mantener. El criterio de calidad no es solo que funcione — es que sea legible.

---

## Prerrequisito

```sql
SHOW TABLES IN silver;
-- Necesitas silver.transactions disponible
```

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana03-sql-avanzado-<tu-nombre>
```

Crea tu carpeta:
```
semana_03/actividades/actividad_03/<tu-nombre>/
```

---

## Parte 1 — CTEs: WITH

Una CTE (`Common Table Expression`) es como darle nombre a una subquery para usarla después. Hace el SQL más legible.

### 1.1 CTE básica vs subquery equivalente

Primero la versión con subquery (difícil de leer):

```sql
-- SIN CTE: usuarios con más fraude que el promedio de fraudes por usuario
SELECT user_id, total_fraudes
FROM (
    SELECT client_id AS user_id, SUM(is_fraud) AS total_fraudes
    FROM silver.transactions
    GROUP BY client_id
) resumen
WHERE total_fraudes > (
    SELECT AVG(fraudes_por_usuario)
    FROM (
        SELECT client_id, SUM(is_fraud) AS fraudes_por_usuario
        FROM silver.transactions
        GROUP BY client_id
    ) sub
)
ORDER BY total_fraudes DESC;
```

Ahora la misma query con CTEs (legible):

```sql
-- CON CTEs: mismo resultado, mucho más legible
WITH fraudes_por_usuario AS (
    SELECT
        client_id AS user_id,
        COUNT(*) AS total_transacciones,
        SUM(is_fraud) AS total_fraudes,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct
    FROM silver.transactions
    GROUP BY client_id
),
promedio_fraudes AS (
    SELECT AVG(total_fraudes) AS media_fraudes
    FROM fraudes_por_usuario
)
SELECT
    f.user_id,
    f.total_transacciones,
    f.total_fraudes,
    f.tasa_fraude_pct,
    p.media_fraudes,
    ROUND(f.total_fraudes - p.media_fraudes, 2) AS desviacion_vs_media
FROM fraudes_por_usuario f
CROSS JOIN promedio_fraudes p
WHERE f.total_fraudes > p.media_fraudes
ORDER BY f.total_fraudes DESC
LIMIT 20;
```

Documenta en markdown: ¿cuántas CTEs puedes encadenar? ¿Puede una CTE referenciar a otra CTE definida antes?

---

### 1.2 CTEs múltiples encadenadas

```sql
-- Análisis de fraude por categoría, comparando con la media global
WITH total_global AS (
    SELECT
        COUNT(*) AS tx_totales,
        SUM(is_fraud) AS fraudes_totales,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 4) AS tasa_global
    FROM silver.transactions
),
por_categoria AS (
    SELECT
        merchant_category,
        COUNT(*) AS tx_categoria,
        SUM(is_fraud) AS fraudes_categoria,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_categoria
    FROM silver.transactions
    WHERE merchant_category IS NOT NULL
    GROUP BY merchant_category
)
SELECT
    c.merchant_category,
    c.tx_categoria,
    c.fraudes_categoria,
    c.tasa_categoria,
    g.tasa_global,
    ROUND(c.tasa_categoria - g.tasa_global, 2) AS diferencia_vs_global,
    CASE
        WHEN c.tasa_categoria > g.tasa_global * 1.5 THEN 'Alto riesgo'
        WHEN c.tasa_categoria > g.tasa_global THEN 'Riesgo elevado'
        ELSE 'Riesgo normal'
    END AS clasificacion_riesgo
FROM por_categoria c
CROSS JOIN total_global g
ORDER BY c.tasa_categoria DESC;
```

Commit esperado:
```bash
git commit -m "feat: CTEs - chained common table expressions for fraud analysis"
```

---

## Parte 2 — Window Functions en SQL

Las window functions en SQL tienen la misma lógica que en PySpark, pero con sintaxis `OVER (PARTITION BY ... ORDER BY ...)`.

### 2.1 ROW_NUMBER, RANK y DENSE_RANK

```sql
-- Ranking de usuarios por gasto total, dentro de cada tipo de tarjeta
WITH gasto_usuario AS (
    SELECT
        client_id AS user_id,
        card_type,
        ROUND(SUM(amount), 2) AS gasto_total,
        COUNT(*) AS num_transacciones
    FROM silver.transactions
    WHERE card_type IS NOT NULL
    GROUP BY client_id, card_type
)
SELECT
    user_id,
    card_type,
    gasto_total,
    num_transacciones,
    ROW_NUMBER() OVER (PARTITION BY card_type ORDER BY gasto_total DESC) AS row_num,
    RANK()       OVER (PARTITION BY card_type ORDER BY gasto_total DESC) AS rank_gasto,
    DENSE_RANK() OVER (PARTITION BY card_type ORDER BY gasto_total DESC) AS dense_rank_gasto
FROM gasto_usuario
QUALIFY ROW_NUMBER() OVER (PARTITION BY card_type ORDER BY gasto_total DESC) <= 5;
```

> **Investigar:** ¿Qué es `QUALIFY`? ¿Es estándar SQL o específico de Spark/Databricks?  
> Documenta la diferencia entre `ROW_NUMBER`, `RANK` y `DENSE_RANK` cuando hay empates.

---

### 2.2 LAG y LEAD — comparar con la fila anterior o siguiente

```sql
-- Para cada usuario, ¿cuánto gastó en la compra anterior y en la siguiente?
WITH tx_ordenadas AS (
    SELECT
        client_id AS user_id,
        transaction_date,
        amount,
        merchant_name,
        is_fraud
    FROM silver.transactions
    WHERE client_id IS NOT NULL
)
SELECT
    user_id,
    transaction_date,
    amount,
    LAG(amount, 1)  OVER (PARTITION BY user_id ORDER BY transaction_date) AS monto_anterior,
    LEAD(amount, 1) OVER (PARTITION BY user_id ORDER BY transaction_date) AS monto_siguiente,
    ROUND(
        amount - LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY transaction_date),
        2
    ) AS variacion_vs_anterior,
    is_fraud
FROM tx_ordenadas
LIMIT 50;
```

```sql
-- Detectar saltos bruscos: transacciones donde el monto es el doble del anterior
WITH tx_con_lag AS (
    SELECT
        client_id AS user_id,
        transaction_date,
        amount,
        merchant_name,
        is_fraud,
        LAG(amount, 1) OVER (PARTITION BY client_id ORDER BY transaction_date) AS monto_anterior
    FROM silver.transactions
)
SELECT *
FROM tx_con_lag
WHERE monto_anterior IS NOT NULL
  AND amount > monto_anterior * 2
  AND amount > 100   -- filtrar solo montos significativos
ORDER BY amount DESC
LIMIT 20;
```

---

### 2.3 Acumulados y media móvil

```sql
-- Gasto acumulado por usuario ordenado por fecha
SELECT
    client_id AS user_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY client_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS gasto_acumulado,
    AVG(amount) OVER (
        PARTITION BY client_id
        ORDER BY transaction_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS media_movil_3
FROM silver.transactions
WHERE client_id = (SELECT client_id FROM silver.transactions LIMIT 1)
ORDER BY transaction_date;
```

---

### 2.4 NTILE — dividir en cuartiles

```sql
-- Clasificar usuarios en cuartiles según su gasto total
WITH gasto_por_usuario AS (
    SELECT
        client_id AS user_id,
        ROUND(SUM(amount), 2) AS gasto_total,
        COUNT(*) AS num_transacciones,
        SUM(is_fraud) AS total_fraudes
    FROM silver.transactions
    GROUP BY client_id
)
SELECT
    user_id,
    gasto_total,
    num_transacciones,
    total_fraudes,
    NTILE(4) OVER (ORDER BY gasto_total) AS cuartil_gasto,
    NTILE(10) OVER (ORDER BY gasto_total) AS decil_gasto
FROM gasto_por_usuario
ORDER BY gasto_total DESC
LIMIT 30;
```

> ¿Qué porcentaje del fraude total se concentra en el cuartil de mayor gasto?

Commit esperado:
```bash
git commit -m "feat: window functions in SQL - rank, lag, running totals, ntile"
```

---

## Parte 3 — CTE + Window Function combinados

El patrón más poderoso: CTE para preparar los datos, window function para el análisis:

```sql
-- Top comercio por categoría en cada mes (sin perder detalle de filas)
WITH volumen_comercio_mes AS (
    SELECT
        merchant_name,
        merchant_category,
        mes,
        anio,
        COUNT(*) AS num_transacciones,
        ROUND(SUM(amount), 2) AS monto_total
    FROM silver.transactions
    WHERE merchant_category IS NOT NULL
    GROUP BY merchant_name, merchant_category, mes, anio
),
ranking_en_categoria AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY merchant_category, anio, mes
            ORDER BY monto_total DESC
        ) AS rank_mes
    FROM volumen_comercio_mes
)
SELECT
    merchant_category,
    anio,
    mes,
    merchant_name,
    num_transacciones,
    monto_total
FROM ranking_en_categoria
WHERE rank_mes = 1
ORDER BY anio, mes, merchant_category;
```

---

## Parte 4 — Análisis final: 3 preguntas, SQL complejo

Responde cada una con al menos una CTE y una window function. Código + conclusión en markdown.

1. **Usuarios que aceleran en fraude:** Identifica usuarios donde la segunda mitad de sus transacciones (cronológicamente) tiene mayor tasa de fraude que la primera mitad.

2. **Categorías con fraude concentrado en un solo horario:** ¿Hay categorías donde más del 60% del fraude ocurre en una única hora del día?

3. **El "efecto vecino":** Para cada transacción fraudulenta, ¿cuánto tiempo pasó desde la transacción anterior del mismo usuario? ¿Las transacciones fraudulentas tienden a ocurrir cerca de otras transacciones?

---

## Entrega en Git

```bash
git add semana_03/actividades/actividad_03/<tu-nombre>/
git commit -m "feat: advanced SQL - CTEs and window functions - <tu-nombre>"
git push origin feature/semana03-sql-avanzado-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 03] SQL Avanzado — <Tu Nombre>
```

En el PR:
- ¿Cuál de las 3 preguntas del análisis final fue más difícil de expresar en SQL?
- ¿Qué es `QUALIFY` y por qué es útil con window functions?
- Un fragmento de SQL del que estés orgulloso/a

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| CTEs básicas con legibilidad | Al menos 2 CTEs, bien nombradas | 20% |
| CTEs encadenadas | Una CTE referencia a otra | 15% |
| Window functions: ROW_NUMBER/RANK/DENSE_RANK | Con QUALIFY o equivalente | 20% |
| LAG/LEAD implementados | Con análisis de variación | 15% |
| Acumulados y media móvil | ROWS BETWEEN correctamente usado | 10% |
| Análisis final: 3 preguntas | Una con CTE + window, conclusión escrita | 20% |

---

## Referencias

- [Spark SQL Window Functions](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [QUALIFY clause en Databricks](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-qualify.html)
- [Actividad 03 semana 02](../../semana_02/actividades/actividad_03/README.md) — mismo concepto en PySpark (referencia para comparar)
