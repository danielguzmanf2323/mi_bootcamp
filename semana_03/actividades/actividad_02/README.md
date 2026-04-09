# Actividad 02 — Semana 03: JOINs en SQL

**Semana:** 03  
**Tema:** JOINs en Spark SQL — INNER, LEFT, FULL OUTER, subqueries correlacionadas  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

En semana 02 hiciste JOINs en PySpark con la API de DataFrames. Ahora repites el mismo concepto pero en SQL puro.

El objetivo no es solo que funcione — es que compares ambos enfoques y entiendas cuándo cada uno es más legible, más mantenible o más eficiente.

Al terminar esta actividad deberías poder responder: ¿cuándo usarías PySpark y cuándo SQL para un JOIN?

---

## Prerrequisito

Las mismas tablas de semana 02 en los schemas `bronze`, `silver`, `gold`. Verifica:

```sql
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
```

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana03-sql-joins-<tu-nombre>
```

Crea tu carpeta:
```
semana_03/actividades/actividad_02/<tu-nombre>/
```

---

## Parte 1 — Revisión del modelo de datos en SQL

Antes de joinear, documenta el modelo. En semana 02 lo hiciste en código. Ahora hazlo en SQL:

```sql
-- ¿Cuántos usuarios únicos hay en bronze.users?
SELECT COUNT(DISTINCT id) AS usuarios_unicos FROM bronze.users;

-- ¿Cuántas transacciones hay por usuario en bronze.transactions?
SELECT
    client_id,
    COUNT(*) AS num_transacciones
FROM bronze.transactions
GROUP BY client_id
ORDER BY num_transacciones DESC
LIMIT 10;

-- ¿Hay usuarios en transactions que no existan en users?
SELECT COUNT(DISTINCT t.client_id) AS usuarios_en_tx_sin_perfil
FROM bronze.transactions t
LEFT JOIN bronze.users u ON t.client_id = u.id
WHERE u.id IS NULL;
```

Documenta: ¿cuántos usuarios huérfanos encontraste? ¿Qué impacto tiene esto en el análisis?

---

## Parte 2 — INNER JOIN: solo registros que coinciden en ambos lados

```sql
-- Transacciones con su información de usuario (solo las que tienen usuario registrado)
SELECT
    t.id AS transaction_id,
    t.date AS transaction_date,
    t.amount,
    t.merchant_name,
    u.id AS user_id,
    u.birth_year,
    u.gender
FROM bronze.transactions t
INNER JOIN bronze.users u ON t.client_id = u.id
LIMIT 20;
```

```sql
-- ¿Cuántos registros quedan tras el INNER JOIN?
-- Compara con el total original de transactions
SELECT 'total_transactions' AS tabla, COUNT(*) AS filas FROM bronze.transactions
UNION ALL
SELECT 'inner_join_users', COUNT(*)
FROM bronze.transactions t
INNER JOIN bronze.users u ON t.client_id = u.id;
```

Documenta: ¿cuántos registros se perdieron? ¿Por qué?

---

## Parte 3 — LEFT JOIN: mantener todos los registros de la tabla izquierda

```sql
-- Transacciones con info de tarjeta — aunque no haya tarjeta registrada
SELECT
    t.id AS transaction_id,
    t.amount,
    t.merchant_name,
    c.card_type,
    c.credit_limit,
    CASE WHEN c.id IS NULL THEN 'sin_tarjeta' ELSE 'con_tarjeta' END AS estado_tarjeta
FROM bronze.transactions t
LEFT JOIN bronze.cards c ON t.card_id = c.id
LIMIT 20;
```

```sql
-- ¿Cuántas transacciones no tienen tarjeta asociada?
SELECT
    CASE WHEN c.id IS NULL THEN 'sin_tarjeta' ELSE 'con_tarjeta' END AS estado,
    COUNT(*) AS total
FROM bronze.transactions t
LEFT JOIN bronze.cards c ON t.card_id = c.id
GROUP BY 1;
```

---

## Parte 4 — Múltiples JOINs encadenados

Reconstruye en SQL puro el JOIN que hiciste en PySpark en semana 02:

```sql
-- Recrear silver.transactions desde las tablas Bronze usando solo SQL
SELECT
    t.id             AS transaction_id,
    t.date           AS transaction_date,
    CAST(REGEXP_REPLACE(t.amount, '[$,]', '') AS DOUBLE) AS amount,
    t.merchant_name,
    t.merchant_city,
    t.merchant_state,
    t.mcc,
    m.edited_description AS merchant_category,
    t.use_chip,
    t.client_id      AS user_id,
    u.birth_year,
    u.gender,
    u.per_capita_income,
    t.card_id,
    c.card_type,
    c.credit_limit,
    f.label          AS is_fraud,
    HOUR(CAST(t.date AS TIMESTAMP))      AS hora,
    DAYOFWEEK(CAST(t.date AS TIMESTAMP)) AS dia_semana,
    MONTH(CAST(t.date AS TIMESTAMP))     AS mes,
    YEAR(CAST(t.date AS TIMESTAMP))      AS anio,
    CASE WHEN DAYOFWEEK(CAST(t.date AS TIMESTAMP)) IN (1, 7) THEN 1 ELSE 0 END AS es_fin_de_semana
FROM bronze.transactions t
LEFT JOIN bronze.users u
    ON t.client_id = u.id
LEFT JOIN bronze.cards c
    ON t.card_id = c.id
LEFT JOIN bronze.mcc_codes m
    ON t.mcc = m.mcc
LEFT JOIN bronze.fraud_labels f
    ON t.id = f.id
LIMIT 100;
```

> **Investigar:** ¿Cuándo el optimizador de Spark (Catalyst) decide reordenar tus JOINs? ¿Qué es un broadcast join y cuándo ocurre automáticamente?

Commit esperado:
```bash
git commit -m "feat: reconstruct silver table using only SQL JOINs from bronze"
```

---

## Parte 5 — Subqueries

### 5.1 Subquery en WHERE

```sql
-- Transacciones de usuarios cuyo ingreso per cápita está por encima del promedio
SELECT
    t.id AS transaction_id,
    t.amount,
    t.merchant_name,
    u.per_capita_income
FROM bronze.transactions t
INNER JOIN bronze.users u ON t.client_id = u.id
WHERE u.per_capita_income > (
    SELECT AVG(per_capita_income) FROM bronze.users
)
ORDER BY u.per_capita_income DESC
LIMIT 20;
```

### 5.2 Subquery en FROM (tabla derivada)

```sql
-- Top 10 usuarios por gasto total, solo entre los que tienen más de 50 transacciones
SELECT *
FROM (
    SELECT
        client_id,
        COUNT(*) AS num_transacciones,
        ROUND(SUM(CAST(REGEXP_REPLACE(amount, '[$,]', '') AS DOUBLE)), 2) AS gasto_total
    FROM bronze.transactions
    GROUP BY client_id
) resumen_usuario
WHERE num_transacciones > 50
ORDER BY gasto_total DESC
LIMIT 10;
```

### 5.3 EXISTS — verificar existencia

```sql
-- Usuarios que tienen AL MENOS UNA transacción fraudulenta
SELECT DISTINCT u.id, u.birth_year, u.gender
FROM bronze.users u
WHERE EXISTS (
    SELECT 1
    FROM bronze.transactions t
    INNER JOIN bronze.fraud_labels f ON t.id = f.id
    WHERE t.client_id = u.id
      AND f.label = 1
)
LIMIT 20;
```

---

## Parte 6 — Comparación PySpark vs SQL

Escribe el equivalente en ambos lenguajes y compara. Elige UNA de estas consultas:

**Opción A:** Usuarios con más de 5 transacciones fraudulentas, ordenados por tasa de fraude.

**Opción B:** Top 5 categorías MCC por monto total en transacciones de fin de semana.

Estructura esperada en el notebook:

```
Celda 1 (markdown): Enunciado de la consulta
Celda 2 (SQL):      Implementación en SQL
Celda 3 (Python):   Implementación en PySpark
Celda 4 (markdown): Reflexión
  - ¿Cuál es más legible?
  - ¿Cuál sería más fácil de mantener si cambia el requerimiento?
  - ¿En qué contexto elegirías cada uno?
```

Commit esperado:
```bash
git commit -m "feat: pyspark vs sql comparison - joins and aggregations"
```

---

## Entrega en Git

```bash
git add semana_03/actividades/actividad_02/<tu-nombre>/
git commit -m "feat: sql joins activity complete - <tu-nombre>"
git push origin feature/semana03-sql-joins-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 03] JOINs en SQL — <Tu Nombre>
```

En el PR responde:
- ¿Cuántos registros huérfanos encontraste entre transactions y users?
- ¿Cuál enfoque elegiste para la comparación PySpark vs SQL? ¿Por qué?
- ¿Qué es un broadcast join y cuándo lo usarías?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Análisis de modelo de datos en SQL | Conteos, huérfanos documentados | 15% |
| INNER vs LEFT JOIN implementados | Con explicación de pérdida de registros | 20% |
| JOINs múltiples encadenados | Reconstrucción de Silver desde Bronze | 25% |
| Subqueries (WHERE, FROM, EXISTS) | Las 3 variantes implementadas | 25% |
| Comparación PySpark vs SQL | Con reflexión escrita | 15% |

---

## Referencias

- [Spark SQL Joins](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html)
- [Actividad 02 semana 02](../../semana_02/actividades/actividad_02/README.md) — mismo concepto en PySpark
