# Actividad 04 — Semana 03: Vistas, Optimización y el Mapa SQL ↔ PySpark

**Semana:** 03  
**Tema:** CREATE VIEW, plan de ejecución y mapa completo SQL ↔ PySpark  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

Esta actividad cierra la semana con tres conceptos clave para cualquier Data Engineer:

1. **Vistas (`CREATE VIEW`):** cómo exponer datos limpios y seguros sin duplicar tablas — el mecanismo que usarás para servir datos a equipos de analistas sin darles acceso a Bronze o Silver directamente.

2. **Plan de ejecución (`EXPLAIN`):** cómo Spark decides ejecutar tu query — por qué una query puede ser lenta aunque parezca simple, y cómo leer el plan para diagnosticarlo.

3. **Mapa SQL ↔ PySpark:** el mapa completo entre los dos lenguajes que usaste durante semanas 02 y 03. Necesitas tener esto claro antes de empezar semana 04.

---

## Prerrequisito

```sql
SHOW TABLES IN silver;
SHOW TABLES IN gold;
```

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana03-vistas-<tu-nombre>
```

Crea tu carpeta:
```
semana_03/actividades/actividad_04/<tu-nombre>/
```

---

## Parte 1 — CREATE VIEW

Una vista es una query guardada que se comporta como una tabla. No duplica datos — ejecuta la query cada vez que la consultas.

### 1.1 Vista para el equipo de fraude

```sql
-- Vista que expone solo las columnas relevantes para el equipo de fraude
-- Sin PII completa, sin columnas técnicas internas
CREATE OR REPLACE VIEW gold.v_analisis_fraude AS
SELECT
    transaction_id,
    transaction_date,
    mes,
    anio,
    hora,
    dia_semana,
    es_fin_de_semana,
    amount,
    ABS(amount) AS amount_abs,
    merchant_name,
    merchant_category,
    mcc,
    card_type,
    is_fraud
FROM silver.transactions
WHERE is_fraud IS NOT NULL;

-- Verificar
SELECT COUNT(*) FROM gold.v_analisis_fraude;
DESCRIBE TABLE gold.v_analisis_fraude;
```

### 1.2 Vista de resumen para dashboard

```sql
-- Vista Gold ya agregada — ideal para conectar con Power BI o Tableau
CREATE OR REPLACE VIEW gold.v_dashboard_fraude AS
WITH resumen AS (
    SELECT
        anio,
        mes,
        merchant_category,
        card_type,
        hora,
        es_fin_de_semana,
        COUNT(*) AS total_transacciones,
        SUM(is_fraud) AS total_fraudes,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS tasa_fraude_pct,
        ROUND(SUM(ABS(amount)), 2) AS monto_total,
        ROUND(AVG(ABS(amount)), 2) AS ticket_promedio
    FROM silver.transactions
    WHERE is_fraud IS NOT NULL
    GROUP BY anio, mes, merchant_category, card_type, hora, es_fin_de_semana
)
SELECT * FROM resumen;

SELECT * FROM gold.v_dashboard_fraude LIMIT 10;
```

### 1.3 Vista temporal (solo para la sesión)

```sql
-- Vista temporal: no persiste entre sesiones, útil para exploración
CREATE OR REPLACE TEMPORARY VIEW v_fraude_hoy AS
SELECT *
FROM silver.transactions
WHERE is_fraud = 1
  AND transaction_date >= DATE_ADD(
      (SELECT MAX(transaction_date) FROM silver.transactions),
      -90
  );

SELECT COUNT(*) AS fraudes_ultimos_90_dias FROM v_fraude_hoy;
```

Documenta en markdown:
- ¿Cuál es la diferencia entre `CREATE VIEW` y `CREATE TEMPORARY VIEW`?
- ¿Cuándo usarías una vista en lugar de una tabla Gold?
- ¿Puede una vista referenciar a otra vista?

Commit esperado:
```bash
git commit -m "feat: create views for fraud team and dashboard consumption"
```

---

## Parte 2 — EXPLAIN: leer el plan de ejecución

Databricks Spark ejecuta tus queries en etapas. `EXPLAIN` muestra el plan antes de ejecutar.

```sql
-- Plan de una query simple
EXPLAIN
SELECT merchant_category, COUNT(*) AS total
FROM silver.transactions
GROUP BY merchant_category
ORDER BY total DESC;
```

```sql
-- Plan extendido — más detalle del optimizador Catalyst
EXPLAIN EXTENDED
SELECT t.*, u.birth_year
FROM silver.transactions t
INNER JOIN bronze.users u ON t.user_id = u.id
WHERE t.is_fraud = 1;
```

```sql
-- Plan físico — qué operaciones realmente ejecuta Spark
EXPLAIN FORMATTED
SELECT *
FROM silver.transactions
WHERE card_type = 'Credit'
  AND is_fraud = 1;
```

Documenta en markdown:
- ¿Qué diferencia hay entre `EXPLAIN`, `EXPLAIN EXTENDED` y `EXPLAIN FORMATTED`?
- ¿Qué es un `BroadcastHashJoin` y cuándo aparece en el plan?
- ¿Qué es un `SortMergeJoin`? ¿Cuándo es mejor o peor que BroadcastHashJoin?

> No necesitas entender todo el plan — enfócate en identificar los nodos principales: `Scan`, `Filter`, `HashAggregate`, `Exchange`, `SortMergeJoin`, `BroadcastHashJoin`.

---

## Parte 3 — Mapa completo SQL ↔ PySpark

Construye la tabla de equivalencias como proof-of-concept: para cada operación, muestra el código en ambos lenguajes y el resultado.

Trabaja en un notebook con celdas alternando SQL y Python.

### Set 1: Lectura y selección

```python
# PySpark
df = spark.table("silver.transactions")
df.select("transaction_id", "amount", "merchant_category", "is_fraud").limit(5).show()
```

```sql
-- SQL equivalente
SELECT transaction_id, amount, merchant_category, is_fraud
FROM silver.transactions
LIMIT 5;
```

---

### Set 2: Filtros

```python
# PySpark
from pyspark.sql import functions as F
df.filter((F.col("is_fraud") == 1) & (F.col("amount") > 500)).count()
```

```sql
-- SQL
SELECT COUNT(*)
FROM silver.transactions
WHERE is_fraud = 1 AND amount > 500;
```

---

### Set 3: Agrupaciones

```python
# PySpark
df.groupBy("merchant_category") \
  .agg(
      F.count("*").alias("total"),
      F.round(F.avg("amount"), 2).alias("ticket_promedio"),
      F.sum("is_fraud").alias("fraudes")
  ) \
  .orderBy(F.col("total").desc()) \
  .limit(10) \
  .show()
```

```sql
-- SQL
SELECT
    merchant_category,
    COUNT(*) AS total,
    ROUND(AVG(amount), 2) AS ticket_promedio,
    SUM(is_fraud) AS fraudes
FROM silver.transactions
GROUP BY merchant_category
ORDER BY total DESC
LIMIT 10;
```

---

### Set 4: Window Functions

```python
# PySpark
from pyspark.sql import Window
windowSpec = Window.partitionBy("card_type").orderBy(F.col("amount").desc())
df.withColumn("rank", F.rank().over(windowSpec)).filter(F.col("rank") <= 3).show()
```

```sql
-- SQL
SELECT *
FROM silver.transactions
QUALIFY RANK() OVER (PARTITION BY card_type ORDER BY amount DESC) <= 3;
```

---

### Set 5: Crear tabla desde query

```python
# PySpark
resumen = df.groupBy("card_type").agg(F.sum("is_fraud").alias("total_fraudes"))
resumen.write.format("delta").mode("overwrite").saveAsTable("gold.resumen_por_tarjeta")
```

```sql
-- SQL
CREATE OR REPLACE TABLE gold.resumen_por_tarjeta AS
SELECT card_type, SUM(is_fraud) AS total_fraudes
FROM silver.transactions
GROUP BY card_type;
```

---

### Set 6: Documentar con tu propio ejemplo

Elige UNA operación de semana 02 que hiciste en PySpark y escribe su equivalente en SQL. Explica cuál prefieres y por qué.

Commit esperado:
```bash
git commit -m "feat: full SQL-PySpark equivalence map with proof of concept"
```

---

## Parte 4 — Reflexión de cierre de semana

En una celda markdown, responde:

1. **¿Cuándo SQL sobre Delta?** ¿Qué tipo de tareas resolverías con SQL en lugar de PySpark?
2. **¿Cuándo PySpark?** ¿Qué operaciones son difíciles o imposibles en SQL puro?
3. **Vistas vs tablas Gold:** ¿Cuál es la diferencia de coste entre consultar una vista y una tabla Delta pre-agregada?
4. **Para semana 04:** Si el equipo de fraude necesita datos actualizados cada hora (streaming), ¿cambiaría algo en la arquitectura que construiste?

---

## Parte 5 — Shuffle, tipos de JOIN y skew

Esta parte amplía el `EXPLAIN` de la Parte 2 con los conceptos de performance que más impactan en producción. Los viste mencionados en semana 02 como investigación — aquí los compruebas con código.

### ¿Qué es un shuffle?

Un shuffle ocurre cuando Spark necesita mover datos entre executors — al hacer un `groupBy`, un `orderBy`, o un JOIN entre tablas grandes. Es la operación más costosa: implica serialización, transferencia de red y reordenamiento en disco.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import pyspark.sql.functions as F

df_tx    = spark.table("silver.transactions")
df_users = spark.table("silver.users")
df_mcc   = spark.table("bronze.mcc_codes")

# Ver el plan de un JOIN grande (SortMergeJoin esperado)
df_join_grande = df_tx.join(df_users, on="user_id", how="left")
print("=== JOIN grande (transactions + users) ===")
df_join_grande.explain(mode="simple")
```

Busca en el plan:
- `Exchange hashpartitioning` → aquí está el shuffle
- `SortMergeJoin` → JOIN después del shuffle (ambas tablas grandes)
- `BroadcastHashJoin` → JOIN sin shuffle (una tabla es pequeña)

### BroadcastHashJoin vs SortMergeJoin

| | BroadcastHashJoin | SortMergeJoin |
|---|---|---|
| **Cuándo** | Una tabla cabe en memoria del driver (~10MB por defecto) | Ambas tablas son grandes |
| **Shuffle** | No — la tabla pequeña se copia a todos los executors | Sí — ambas se redistribuyen |
| **Velocidad** | Muy rápido | Depende del volumen |

```python
# Forzar BroadcastHashJoin con mcc_codes (tabla pequeña = JSON de categorías)
df_con_broadcast    = df_tx.join(broadcast(df_mcc), on="mcc_code", how="left")
df_sin_broadcast    = df_tx.join(df_mcc, on="mcc_code", how="left")

print("=== CON broadcast() ===")
df_con_broadcast.explain(mode="simple")

print("\n=== SIN broadcast() ===")
df_sin_broadcast.explain(mode="simple")
```

Documenta: ¿cuál de las dos tiene `Exchange` en el plan? ¿Cuál debería ser más rápida y por qué?

### Particiones: repartition vs coalesce

```python
print(f"Particiones actuales: {df_tx.rdd.getNumPartitions()}")

# repartition — redistribuye con shuffle, puede aumentar o reducir
df_repart = df_tx.repartition(32)
print(f"Después de repartition(32): {df_repart.rdd.getNumPartitions()}")

# coalesce — solo reduce, SIN shuffle (fusiona particiones localmente)
df_coal = df_tx.coalesce(4)
print(f"Después de coalesce(4): {df_coal.rdd.getNumPartitions()}")
```

Regla: usa `repartition()` para redistribuir o aumentar. Usa `coalesce()` solo para reducir antes de escribir a disco (evitas el shuffle overhead).

### Detectar skew

El skew ocurre cuando los datos están distribuidos de forma desigual entre particiones. Una partición tiene 10 millones de filas mientras las demás tienen 10 mil. El executor que la procesa se vuelve el cuello de botella — las otras tasks terminan y ese executor sigue trabajando solo.

```python
# Detectar skew: buscar columnas con distribución muy desigual
distribucion = (
    df_tx
    .groupBy("merchant_city")
    .count()
    .orderBy(F.desc("count"))
)
distribucion.show(20)

# Si el top-1 tiene 10x más registros que el top-20 → potencial skew en JOINs por esa columna
top1  = distribucion.first()["count"]
top20 = distribucion.collect()[19]["count"]
print(f"Ratio top1/top20: {top1/top20:.1f}x")
```

En el Spark UI, el skew se ve como: un stage donde la mayoría de tasks terminan en 2s pero una task tarda 45s. Esa es la partición grande.

Commit esperado:
```bash
git commit -m "feat: shuffle analysis, broadcast hint, skew detection - sem03 act04"
```

---

## Entrega en Git

```bash
git add semana_03/actividades/actividad_04/<tu-nombre>/
git commit -m "feat: views, explain, SQL-PySpark map - <tu-nombre>"
git push origin feature/semana03-vistas-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 03] Vistas y Mapa SQL↔PySpark — <Tu Nombre>
```

En el PR:
- ¿Cuándo usarías `CREATE VIEW` vs `saveAsTable`?
- ¿Qué tipo de join viste en el plan `EXPLAIN` para tus queries?
- Tu respuesta a la pregunta 4 de la reflexión (streaming)

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| 3 vistas creadas (fraude, dashboard, temporal) | Con diferencias documentadas | 25% |
| EXPLAIN analizado | Los 3 tipos con interpretación escrita | 20% |
| Mapa SQL↔PySpark: los 5 sets | Código ejecutable en ambos | 35% |
| Set 6: ejemplo propio | De semana 02, con reflexión | 10% |
| Reflexión de cierre | 4 preguntas respondidas | 10% |

---

## Laboratorio

El notebook de laboratorio para esta actividad está en:
[`../../laboratorios/lab_04_vistas_optimizacion.ipynb`](../../laboratorios/lab_04_vistas_optimizacion.ipynb)

Cubre: creación de dos vistas con `CREATE OR REPLACE VIEW`, análisis del plan de ejecución con `EXPLAIN FORMATTED` (FileScan, Exchange/shuffle), tabla comparativa SQL ↔ PySpark (`Window`, `groupBy`, `filter`), y preguntas de negocio consultando directamente las vistas.

---

## Referencias

- [CREATE VIEW en Databricks](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-view.html)
- [EXPLAIN en Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)
- [Catalyst Optimizer](https://www.databricks.com/glossary/catalyst-optimizer)
- [Actividad 03 semana 02](../../semana_02/actividades/actividad_03/README.md) — window functions en PySpark (para comparar)
