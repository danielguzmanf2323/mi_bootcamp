# Actividad 04 — Semana 02: Arquitectura Medallón con Datos de Fraude

**Semana:** 02  
**Tema:** Implementar una pipeline Medallón (Bronze → Silver → Gold) con las 5 tablas del dataset financiero  
**Nivel:** Junior–Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

En semana 01 construiste la arquitectura Medallón con un dataset simple de clientes. Ahora el reto escala:

- 5 tablas de distintos formatos (CSV y JSON)
- Transformaciones complejas en Silver (limpieza + JOINs)
- Capa Gold con tablas orientadas a negocio para análisis de fraude
- Cada capa se escribe como tabla Delta para que persista

La diferencia entre un Data Analyst y un Data Engineer está aquí: no basta con analizar los datos, hay que dejarlos listos y confiables para que otros los consuman.

---

## Dataset

**[inetum_data_engineer_bootcamp / semana_02 / financial_transaction_dataset](https://gfi1.sharepoint.com/sites/JUNIORDATAENGINEERSDEVTEAM/Documents%20partages/Forms/AllItems.aspx?id=%2Fsites%2FJUNIORDATAENGINEERSDEVTEAM%2FDocuments%20partages%2FGeneral%2Finetum%5Fdata%5Fengineer%5Fbootcamp&viewid=532715df%2D69df%2D4d0e%2D8785%2Daf7a4ccf2983)**

> Navega dentro del sitio a: `General / inetum_data_engineer_bootcamp / semana_02 / financial_transaction_dataset`

Archivos:
- `transactions_data.csv` — transacciones financieras
- `users_data.csv` — datos demográficos de usuarios
- `cards_data.csv` — tipos y metadatos de tarjetas
- `mcc_codes.json` — categorías de comercios
- `train_fraud_labels.json` — etiquetas de fraude por transacción

---

## Material de estudio previo

- ¿Qué diferencia hay entre Bronze, Silver y Gold?
- ¿Por qué cada capa lee de la capa anterior y no del archivo fuente?
- ¿Qué ventajas tiene guardar como Delta Lake en lugar de CSV/Parquet?
- ¿Qué es `saveAsTable` y cómo difiere de `write.parquet`?
- ¿Qué es un schema (base de datos) en Databricks? ¿Cómo se crea con `CREATE SCHEMA`?
- ¿Cuál es la diferencia entre `saveAsTable("tabla")` y `saveAsTable("schema.tabla")`?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana02-medallon-<tu-nombre>
```

Crea tu carpeta con 3 notebooks (nombra los archivos con tu nombre):

```
semana_02/actividades/actividad_04/<tu-nombre>/
    bronze_<tu-nombre>.py
    silver_<tu-nombre>.py
    gold_<tu-nombre>.py
```

---

## Diseño previo obligatorio [Requerido]

Antes de escribir código, documenta en una celda markdown (en `bronze_<tu-nombre>.py`) el diseño de tu pipeline:

```
FUENTES
  transactions_data.csv   →  bronze.transactions
  users_data.csv          →  bronze.users
  cards_data.csv          →  bronze.cards
  mcc_codes.json          →  bronze.mcc_codes
  train_fraud_labels.json →  bronze.fraud_labels

BRONZE → SILVER
  bronze.transactions + bronze.users + bronze.cards + bronze.mcc_codes + bronze.fraud_labels
    → limpieza de tipos, estandarización de columnas
    → silver.transactions (tabla maestra enriquecida)

SILVER → GOLD
  silver.transactions → gold.fraude_por_categoria  (fraude por MCC)
  silver.transactions → gold.fraude_por_tarjeta    (fraude por card_type)
  silver.transactions → gold.fraude_temporal       (fraude por hora/día/mes)
  silver.transactions → gold.usuarios_riesgo       (usuarios con mayor tasa de fraude)
```

---

## Notebook 1: Bronze

**Regla Bronze:** ingesta fiel de la fuente. No transformar datos. Solo leer y guardar.

```python
# BRONZE — Ingesta sin transformaciones
# Objetivo: guardar cada archivo fuente como tabla Delta en el schema Bronze

from pyspark.sql import functions as F

MI_NOMBRE = "<tu_nombre>"  # ej: "maria" — sin espacios, en minúsculas

# Crear schemas si no existen (solo necesitas hacerlo una vez)
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# 1. transactions
df_bronze_tx = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("/FileStore/transactions_data.csv")

df_bronze_tx.write.format("delta").mode("overwrite").saveAsTable(f"bronze.transactions_{MI_NOMBRE}")
print(f"bronze.transactions_{MI_NOMBRE}: {df_bronze_tx.count():,} filas | {len(df_bronze_tx.columns)} columnas")

# 2. users
df_bronze_users = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("/FileStore/users_data.csv")

df_bronze_users.write.format("delta").mode("overwrite").saveAsTable(f"bronze.users_{MI_NOMBRE}")
print(f"bronze.users_{MI_NOMBRE}: {df_bronze_users.count():,} filas")

# 3. cards
df_bronze_cards = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("/FileStore/cards_data.csv")

df_bronze_cards.write.format("delta").mode("overwrite").saveAsTable(f"bronze.cards_{MI_NOMBRE}")
print(f"bronze.cards_{MI_NOMBRE}: {df_bronze_cards.count():,} filas")

# 4. mcc_codes (JSON)
df_bronze_mcc = spark.read.option("multiLine", "true").json("/FileStore/mcc_codes.json")
df_bronze_mcc.write.format("delta").mode("overwrite").saveAsTable(f"bronze.mcc_codes_{MI_NOMBRE}")
print(f"bronze.mcc_codes_{MI_NOMBRE}: {df_bronze_mcc.count():,} filas")

# 5. fraud_labels (JSON)
df_bronze_fraud = spark.read.option("multiLine", "true").json("/FileStore/train_fraud_labels.json")
df_bronze_fraud.write.format("delta").mode("overwrite").saveAsTable(f"bronze.fraud_labels_{MI_NOMBRE}")
print(f"bronze.fraud_labels_{MI_NOMBRE}: {df_bronze_fraud.count():,} filas")

print("\nBronze completo. Tablas disponibles:")
display(spark.sql("SHOW TABLES IN bronze"))
```

Commit esperado:
```bash
git commit -m "feat: bronze layer - ingest all 5 financial tables as delta"
```

---

## Notebook 2: Silver

**Regla Silver:** leer de Bronze, limpiar tipos, estandarizar nombres, hacer JOINs, eliminar duplicados. No agregar.

```python
# SILVER — Limpieza y enriquecimiento
# Lee SIEMPRE desde las tablas Bronze, nunca desde los archivos fuente

# Cargar desde Bronze
df_tx       = spark.table("bronze.transactions")
df_users    = spark.table("bronze.users")
df_cards    = spark.table("bronze.cards")
df_mcc      = spark.table("bronze.mcc_codes")
df_fraud    = spark.table("bronze.fraud_labels")

# --- Limpieza de transactions ---
df_tx_clean = df_tx \
    .withColumnRenamed("id", "transaction_id") \
    .withColumnRenamed("client_id", "user_id") \
    .withColumn("amount", F.regexp_replace(F.col("amount"), "[$,]", "").cast("double")) \
    .withColumn("transaction_date", F.to_timestamp("date", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("hora", F.hour("transaction_date")) \
    .withColumn("dia_semana", F.dayofweek("transaction_date")) \
    .withColumn("es_fin_de_semana", F.when(F.dayofweek("transaction_date").isin(1, 7), 1).otherwise(0)) \
    .withColumn("mes", F.month("transaction_date")) \
    .withColumn("anio", F.year("transaction_date")) \
    .drop("date")

# --- Limpieza de users ---
# Ajusta los nombres de columna según el schema real del archivo
df_users_clean = df_users.withColumnRenamed("id", "user_id")

# --- Limpieza de cards ---
df_cards_clean = df_cards.withColumnRenamed("id", "card_id")

# --- Limpieza de mcc ---
# Ajustar según estructura real del JSON de mcc_codes
df_mcc_clean = df_mcc  # modificar si hay anidamiento

# --- Limpieza de fraud labels ---
df_fraud_clean = df_fraud \
    .withColumnRenamed("id", "transaction_id") \
    .withColumnRenamed("label", "is_fraud") \
    .withColumn("is_fraud", F.col("is_fraud").cast("integer"))

# --- JOIN en secuencia ---
df_silver = df_tx_clean \
    .join(df_users_clean, on="user_id", how="left") \
    .join(df_cards_clean, on="card_id", how="left") \
    .join(df_mcc_clean, df_tx_clean["mcc"] == df_mcc_clean["mcc"], how="left") \
    .join(df_fraud_clean, on="transaction_id", how="left")

# Verificar duplicados en la llave primaria
dup_count = df_silver.groupBy("transaction_id").count().filter(F.col("count") > 1).count()
print(f"Transacciones duplicadas tras JOIN: {dup_count}")

# Guardar Silver — sufija tu nombre (MI_NOMBRE definido en el notebook Bronze)
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"silver.transactions_{MI_NOMBRE}")
print(f"silver.transactions_{MI_NOMBRE}: {df_silver.count():,} filas | {len(df_silver.columns)} columnas")
```

> **Obligatorio:** documenta en markdown qué columnas tenía Bronze y cuáles quedan en Silver. ¿Cuáles eliminaste? ¿Por qué?

Commit esperado:
```bash
git commit -m "feat: silver layer - clean types, joins, enriched transactions table"
```

---

## Notebook 3: Gold

**Regla Gold:** leer de Silver, agregar, resumir. Tablas orientadas a responder preguntas de negocio concretas.

```python
# GOLD — Tablas analíticas para consumo
# Lee SIEMPRE desde tu tabla Silver (sufijada con tu nombre)

MI_NOMBRE = "<tu_nombre>"  # ej: "maria"
df_silver = spark.table(f"silver.transactions_{MI_NOMBRE}")

# ------------------------------------------------
# GOLD 1: Fraude por categoría de comercio
# ------------------------------------------------
df_gold_categoria = df_silver \
    .groupBy("merchant_category", "mcc") \
    .agg(
        F.count("transaction_id").alias("total_transacciones"),
        F.sum("is_fraud").alias("total_fraudes"),
        F.round(F.sum("is_fraud") / F.count("transaction_id") * 100, 2).alias("tasa_fraude_pct"),
        F.sum("amount").alias("monto_total"),
        F.avg("amount").alias("ticket_promedio")
    ) \
    .orderBy(F.col("tasa_fraude_pct").desc())

df_gold_categoria.write.format("delta").mode("overwrite").saveAsTable(f"gold.fraude_por_categoria_{MI_NOMBRE}")

# ------------------------------------------------
# GOLD 2: Fraude por tipo de tarjeta
# ------------------------------------------------
df_gold_tarjeta = df_silver \
    .groupBy("card_type") \
    .agg(
        F.count("transaction_id").alias("total_transacciones"),
        F.sum("is_fraud").alias("total_fraudes"),
        F.round(F.sum("is_fraud") / F.count("transaction_id") * 100, 2).alias("tasa_fraude_pct"),
        F.avg("amount").alias("monto_promedio_fraude")
    ) \
    .orderBy(F.col("tasa_fraude_pct").desc())

df_gold_tarjeta.write.format("delta").mode("overwrite").saveAsTable(f"gold.fraude_por_tarjeta_{MI_NOMBRE}")

# ------------------------------------------------
# GOLD 3: Fraude temporal — por hora, día y mes
# ------------------------------------------------
df_gold_temporal = df_silver \
    .groupBy("anio", "mes", "dia_semana", "hora") \
    .agg(
        F.count("transaction_id").alias("total_transacciones"),
        F.sum("is_fraud").alias("total_fraudes"),
        F.round(F.sum("is_fraud") / F.count("transaction_id") * 100, 2).alias("tasa_fraude_pct")
    ) \
    .orderBy("anio", "mes", "dia_semana", "hora")

df_gold_temporal.write.format("delta").mode("overwrite").saveAsTable(f"gold.fraude_temporal_{MI_NOMBRE}")

# ------------------------------------------------
# GOLD 4: Usuarios de alto riesgo
# ------------------------------------------------
df_gold_usuarios = df_silver \
    .groupBy("user_id") \
    .agg(
        F.count("transaction_id").alias("total_transacciones"),
        F.sum("is_fraud").alias("total_fraudes"),
        F.round(F.sum("is_fraud") / F.count("transaction_id") * 100, 2).alias("tasa_fraude_pct"),
        F.sum("amount").alias("monto_total"),
        F.countDistinct("card_id").alias("num_tarjetas")
    ) \
    .filter(F.col("total_transacciones") >= 5) \  # solo usuarios con historial suficiente
    .orderBy(F.col("tasa_fraude_pct").desc())

df_gold_usuarios.write.format("delta").mode("overwrite").saveAsTable(f"gold.usuarios_riesgo_{MI_NOMBRE}")

print("Tablas Gold disponibles:")
display(spark.sql("SHOW TABLES IN gold"))
```

---

## Validación final con SQL

```python
# Responde estas preguntas usando spark.sql() sobre las tablas Gold

# 1. Top 5 categorías de comercio con mayor tasa de fraude
spark.sql("""
    SELECT merchant_category, tasa_fraude_pct, total_transacciones, total_fraudes
    FROM gold.fraude_por_categoria
    ORDER BY tasa_fraude_pct DESC
    LIMIT 5
""").show(truncate=False)

# 2. ¿A qué hora del día hay más fraude?
spark.sql("""
    SELECT hora, SUM(total_fraudes) AS fraudes_totales, AVG(tasa_fraude_pct) AS tasa_promedio
    FROM gold.fraude_temporal
    GROUP BY hora
    ORDER BY fraudes_totales DESC
""").show()

# 3. ¿El fraude es más frecuente en fin de semana?
spark.sql("""
    SELECT dia_semana, SUM(total_fraudes) AS fraudes, AVG(tasa_fraude_pct) AS tasa
    FROM gold.fraude_temporal
    GROUP BY dia_semana
    ORDER BY dia_semana
""").show()
```

Commit esperado:
```bash
git commit -m "feat: gold layer - 4 fraud analysis tables + SQL validation"
```

---

## Entrega en Git

```bash
git add semana_02/actividades/actividad_04/<tu-nombre>/
git commit -m "feat: medallion pipeline bronze-silver-gold fraud dataset - <tu-nombre>"
git push origin feature/semana02-medallon-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 02] Arquitectura Medallón — <Tu Nombre>
```

Incluye en el PR:
- ¿Qué categoría de comercio tiene más fraude?
- ¿A qué hora del día es más probable el fraude?
- ¿Qué problema de calidad de datos encontraste en Bronze y cómo lo resolviste en Silver?

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Diseño documentado antes de codificar | Diagrama + descripción de cada tabla | 10% |
| Bronze: 5 tablas ingestadas como Delta | Sin transformaciones en Bronze, `inferSchema=false` | 20% |
| Silver: limpieza correcta + JOIN completo | Tipos correctos, sin errores de join | 25% |
| Gold: 4 tablas analíticas de fraude | Con métricas correctas por dimensión | 25% |
| Dataset subido al Volumen en la ruta correcta | `/default/<tu_nombre>/semana_02/financial_transaction_dataset/` | 5% |
| Validación SQL sobre tablas Gold | Al menos 3 consultas de negocio | 5% |
| Commits descriptivos | Mínimo 3 commits, uno por capa | 5% |
| Outputs visibles en el notebook | Resultados de `display()` / `show()` presentes en el `.ipynb` | 5% |

---

## Instrucciones de entrega en Databricks Volumes

Carga los archivos del dataset financiero al Volumen del entorno Databricks en la siguiente ruta antes de hacer el PR:

```
/Volumes/main/default/<tu_nombre>/semana_02/financial_transaction_dataset/
```

El instructor verificará que los archivos estén en esa ruta para poder ejecutar tu notebook.

---

## Referencias

- [Delta Lake `saveAsTable`](https://docs.databricks.com/en/delta/index.html)
- [Actividad 02 semana 02](../actividad_02/README.md) — JOINs base
- [Actividad 04 semana 01](../../../semana_01/actividades/actividad_04/README.md) — Medallón con customers (referencia)
