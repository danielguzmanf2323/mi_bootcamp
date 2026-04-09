# Actividad 01 — Semana 03: SQL en Databricks sobre tablas Delta

**Semana:** 03  
**Tema:** SQL sobre tablas Delta — SELECT, filtros, agrupaciones, HAVING  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

En semana 02 construiste una pipeline completa y dejaste las tablas listas en los schemas `bronze`, `silver` y `gold`. Ahora cambias de rol: eres el **analista** que consume esas tablas.

Esta semana no construyes pipelines. Escribes SQL.

La diferencia práctica: en semana 02 necesitabas entender PySpark para procesar los datos. En semana 03 cualquier analista con SQL puede responder preguntas de negocio sobre las mismas tablas sin tocar el código de la pipeline.

---

## Prerrequisito

Necesitas tener las tablas de semana 02 disponibles en tu workspace de Databricks:

```sql
SHOW TABLES IN silver;
SHOW TABLES IN gold;
```

Si no las tienes, ejecuta primero los notebooks de Actividad 04 y el Proyecto de semana 02.

Las tablas que usarás esta semana:

| Schema | Tabla | Descripción |
|--------|-------|-------------|
| `silver` | `transactions` | Tabla maestra enriquecida (5 tablas unidas) |
| `gold` | `fraude_por_categoria` | Fraude agregado por categoría MCC |
| `gold` | `fraude_por_tarjeta` | Fraude agregado por tipo de tarjeta |
| `gold` | `fraude_temporal` | Fraude por hora, día y mes |
| `gold` | `usuarios_riesgo` | Perfil de riesgo por usuario |

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana03-sql-basico-<tu-nombre>
```

Crea tu carpeta:
```
semana_03/actividades/actividad_01/<tu-nombre>/
```

Trabaja en un único notebook: `sql_basico_<tu-nombre>.sql` (o `.py` si usas `spark.sql()`).

---

## Parte 0 — Explorar las tablas antes de consultar

Antes de cualquier `SELECT`, explora la estructura:

```sql
-- ¿Qué tablas tengo disponibles?
SHOW TABLES IN silver;
SHOW TABLES IN gold;

-- ¿Cómo está definida la tabla?
DESCRIBE TABLE silver.transactions;
DESCRIBE TABLE EXTENDED silver.transactions;

-- ¿Cuántas filas tiene?
SELECT COUNT(*) AS total_filas FROM silver.transactions;

-- Ver una muestra
SELECT * FROM silver.transactions LIMIT 10;
```

Documenta en markdown:
- ¿Cuántas columnas tiene `silver.transactions`?
- ¿Cuál es el tipo de dato de `amount`, `transaction_date`, `is_fraud`?
- ¿Cuántas filas tiene la tabla?

---

## Parte 1 — SELECT, WHERE y ORDER BY

### 1.1 Filtros básicos

```sql
-- Transacciones fraudulentas con monto mayor a $500
SELECT
    transaction_id,
    transaction_date,
    amount,
    merchant_name,
    card_type
FROM silver.transactions
WHERE is_fraud = 1
  AND amount > 500
ORDER BY amount DESC
LIMIT 20;
```

```sql
-- Transacciones del mes de enero de cualquier año
SELECT *
FROM silver.transactions
WHERE mes = 1
ORDER BY transaction_date
LIMIT 20;
```

### 1.2 Filtros con texto

```sql
-- Transacciones en categorías que contengan "food" o "restaurant"
SELECT
    transaction_id,
    merchant_name,
    merchant_category,
    amount,
    is_fraud
FROM silver.transactions
WHERE LOWER(merchant_category) LIKE '%food%'
   OR LOWER(merchant_category) LIKE '%restaurant%'
ORDER BY amount DESC;
```

### 1.3 Valores nulos

```sql
-- ¿Hay transacciones sin categoría de comercio asignada?
SELECT COUNT(*) AS sin_categoria
FROM silver.transactions
WHERE merchant_category IS NULL;

-- ¿Hay transacciones sin etiqueta de fraude?
SELECT COUNT(*) AS sin_label_fraude
FROM silver.transactions
WHERE is_fraud IS NULL;
```

Responde: ¿qué harías con los registros sin etiqueta de fraude en un análisis real?

Commit esperado:
```bash
git commit -m "feat: basic SELECT WHERE ORDER BY queries on silver.transactions"
```

---

## Parte 2 — GROUP BY y HAVING

### 2.1 Agrupaciones básicas

```sql
-- Número de transacciones y monto total por tipo de tarjeta
SELECT
    card_type,
    COUNT(*) AS total_transacciones,
    ROUND(SUM(amount), 2) AS monto_total,
    ROUND(AVG(amount), 2) AS ticket_promedio,
    ROUND(MAX(amount), 2) AS monto_maximo
FROM silver.transactions
GROUP BY card_type
ORDER BY total_transacciones DESC;
```

```sql
-- Fraude por mes — ¿en qué mes ocurrió más fraude?
SELECT
    anio,
    mes,
    COUNT(*) AS total_transacciones,
    SUM(is_fraud) AS total_fraudes,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct
FROM silver.transactions
GROUP BY anio, mes
ORDER BY anio, mes;
```

### 2.2 HAVING — filtrar sobre agregaciones

```sql
-- Categorías de comercio con más de 1000 transacciones Y tasa de fraude mayor al 5%
SELECT
    merchant_category,
    COUNT(*) AS total_transacciones,
    SUM(is_fraud) AS total_fraudes,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct
FROM silver.transactions
GROUP BY merchant_category
HAVING COUNT(*) > 1000
   AND SUM(is_fraud) / COUNT(*) * 100 > 5
ORDER BY tasa_fraude_pct DESC;
```

> **Investigar:** ¿Cuál es la diferencia entre `WHERE` y `HAVING`? ¿Por qué no puedes usar `tasa_fraude_pct > 5` directamente en el `HAVING` si lo defines en el `SELECT`?

---

## Parte 3 — Funciones de fecha en SQL

```sql
-- Distribución de transacciones por hora del día
SELECT
    hora,
    COUNT(*) AS total_transacciones,
    SUM(is_fraud) AS fraudes,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct
FROM silver.transactions
GROUP BY hora
ORDER BY hora;
```

```sql
-- ¿Cuántos días hay entre la primera y última transacción del dataset?
SELECT
    MIN(transaction_date) AS primera_transaccion,
    MAX(transaction_date) AS ultima_transaccion,
    DATEDIFF(MAX(transaction_date), MIN(transaction_date)) AS dias_de_historia
FROM silver.transactions;
```

```sql
-- Transacciones de los últimos 30 días del período registrado
SELECT *
FROM silver.transactions
WHERE transaction_date >= DATE_ADD(
    (SELECT MAX(transaction_date) FROM silver.transactions),
    -30
)
ORDER BY transaction_date DESC
LIMIT 30;
```

Commit esperado:
```bash
git commit -m "feat: group by, having, date functions on silver.transactions"
```

---

## Parte 4 — Consultas sobre tablas Gold

Las tablas Gold ya están agregadas. Úsalas para responder preguntas rápidamente:

```sql
-- ¿Qué tipo de tarjeta tiene mayor tasa de fraude?
SELECT *
FROM gold.fraude_por_tarjeta
ORDER BY tasa_fraude_pct DESC;

-- Top 10 categorías con mayor monto fraudulento absoluto
SELECT
    merchant_category,
    total_fraudes,
    tasa_fraude_pct,
    ROUND(monto_total * tasa_fraude_pct / 100, 2) AS monto_estimado_fraude
FROM gold.fraude_por_categoria
ORDER BY monto_estimado_fraude DESC
LIMIT 10;
```

Reflexiona en markdown: ¿cuándo conviene consultar `gold.*` vs consultar `silver.transactions`? ¿Cuándo es una mala idea usar Gold directamente?

---

## Parte 5 — CASE WHEN en SQL

```sql
-- Clasificar transacciones por tamaño de monto
SELECT
    CASE
        WHEN amount < 10    THEN '< $10'
        WHEN amount < 50    THEN '$10 - $50'
        WHEN amount < 100   THEN '$50 - $100'
        WHEN amount < 500   THEN '$100 - $500'
        WHEN amount < 1000  THEN '$500 - $1000'
        ELSE '> $1000'
    END AS rango_monto,
    COUNT(*) AS total_transacciones,
    SUM(is_fraud) AS total_fraudes,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct
FROM silver.transactions
GROUP BY 1
ORDER BY MIN(amount);
```

---

## Entrega en Git

```bash
git add semana_03/actividades/actividad_01/<tu-nombre>/
git commit -m "feat: sql basics activity complete - <tu-nombre>"
git push origin feature/semana03-sql-basico-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 03] SQL Básico — <Tu Nombre>
```

En el PR responde:
- ¿En qué mes ocurrió el pico de fraude?
- ¿Qué categoría de comercio tiene más fraude con más de 1000 transacciones?
- ¿Cuándo usas `HAVING` en lugar de `WHERE`?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Exploración inicial documentada | DESCRIBE, conteos, muestra | 15% |
| SELECT + WHERE + ORDER BY | Al menos 3 consultas con filtros distintos | 20% |
| GROUP BY + HAVING correctos | Agrupaciones con métricas de fraude | 25% |
| Funciones de fecha | Al menos 2 consultas con DATEDIFF / DATE_ADD | 15% |
| Consultas sobre Gold + reflexión | Cuándo usar Gold vs Silver | 15% |
| CASE WHEN implementado | Segmentación por rango de monto | 10% |

---

## Referencias

- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
- [Actividad 04 semana 02](../../semana_02/actividades/actividad_04/README.md) — donde se construyeron las tablas que usas aquí
