# Actividad 02 — Semana 02: JOINs en PySpark — Conectando el modelo de datos

**Semana:** 02  
**Tema:** JOINs en PySpark — inner, left, right, anti  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

En la Actividad 01 exploraste `transactions_data.csv` de forma aislada. El problema: esa tabla sola no responde muchas preguntas de negocio.

- ¿Qué tipo de tarjeta usó el cliente? → necesitas `cards_data.csv`
- ¿A qué categoría pertenece el comercio? → necesitas `mcc_codes.json`
- ¿Esta transacción fue fraude? → necesitas `train_fraud_labels.json`
- ¿Cuántos años tiene el cliente que hizo esa compra? → necesitas `users_data.csv`

Esta actividad te enseña a conectar esas tablas con JOINs en PySpark.

---

## Dataset

Los mismos 5 archivos de semana 02. Si no los tienes aún:

**[data_engineering_files/semana_02_actividad_01](https://drive.google.com/drive/folders/1NPcvkwEyU5t9euXay3Uzxo02LqY_Ptb9)**

---

## Material de estudio previo

- ¿Qué es un JOIN y qué tipos existen? (`INNER`, `LEFT`, `RIGHT`, `FULL`, `ANTI`)
- ¿Cuándo pierdes registros con un `INNER JOIN`?
- ¿Qué es una llave foránea (FK)?
- ¿Qué pasa cuando haces JOIN con duplicados en la llave?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana02-joins-<tu-nombre>
```

Crea tu carpeta:
```
semana_02/actividades/actividad_02/<tu-nombre>/
```

---

## Parte 1 — Modelo de datos: entender antes de joinear

Antes de escribir código, documenta en una celda markdown el modelo de relaciones de este dataset.

```markdown
## Modelo de datos — Financial Transactions

| Tabla | Llave primaria | Llave foránea hacia |
|-------|----------------|---------------------|
| transactions_data | id | client_id → users_data, card_id → cards_data, mcc → mcc_codes, id → fraud_labels |
| users_data | id | — |
| cards_data | id | client_id → users_data |
| mcc_codes | mcc | — |
| train_fraud_labels | id | id → transactions_data |
```

Completa la tabla y dibuja el diagrama de relaciones en texto (como hiciste en el proyecto de semana 1).

Commit esperado:
```bash
git commit -m "docs: add data model diagram for financial transactions"
```

---

## Parte 2 — Cargar todas las tablas

```python
from pyspark.sql import functions as F

# Tablas CSV
df_transactions = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/FileStore/transactions_data.csv")
df_users = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/FileStore/users_data.csv")
df_cards = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load("/FileStore/cards_data.csv")

# Tablas JSON — formato diferente al CSV
df_mcc = spark.read.option("multiLine", "true").json("/FileStore/mcc_codes.json")
df_fraud = spark.read.option("multiLine", "true").json("/FileStore/train_fraud_labels.json")

# Verificar conteos
for nombre, df in [("transactions", df_transactions), ("users", df_users), 
                    ("cards", df_cards), ("mcc", df_mcc), ("fraud", df_fraud)]:
    print(f"{nombre}: {df.count():,} registros | {len(df.columns)} columnas")
```

> **Investigar:** ¿Por qué los archivos JSON se leen diferente al CSV? ¿Qué hace `multiLine`?

Commit esperado:
```bash
git commit -m "feat: load all 5 financial tables into PySpark DataFrames"
```

---

## Parte 3 — Limpieza previa antes del JOIN

Antes de unir tablas, estandariza las llaves de unión:

```python
# Limpiar amount en transactions
df_transactions = df_transactions \
    .withColumn("amount", F.regexp_replace(F.col("amount"), "[$,]", "").cast("double")) \
    .withColumn("transaction_date", F.to_timestamp("date", "yyyy-MM-dd HH:mm:ss")) \
    .withColumnRenamed("id", "transaction_id") \
    .withColumnRenamed("client_id", "user_id")

# Verificar tipos de las llaves en cada tabla — deben coincidir
print("transactions - user_id:", df_transactions.schema["user_id"].dataType)
print("users - id:", df_users.schema["id"].dataType)
print("transactions - card_id:", df_transactions.schema["card_id"].dataType)
print("cards - id:", df_cards.schema["id"].dataType)
```

> **Problema común:** si `user_id` en transactions es string y `id` en users es integer, el JOIN puede fallar o producir 0 resultados sin error. Documenta qué encontraste y cómo lo resolviste.

---

## Parte 4 — JOINs uno por uno

### JOIN 1: transactions + users (INNER JOIN)

```python
df_tx_users = df_transactions.join(
    df_users.withColumnRenamed("id", "user_id"),
    on="user_id",
    how="inner"
)

print(f"Transactions originales: {df_transactions.count():,}")
print(f"Después del JOIN con users: {df_tx_users.count():,}")
print(f"¿Perdimos registros? {df_transactions.count() - df_tx_users.count():,}")
```

Responde en markdown:
- ¿Se perdieron registros? ¿Por qué puede pasar eso con INNER JOIN?
- ¿Qué columnas nuevas ganamos al unir con `users`?

---

### JOIN 2: + cards (LEFT JOIN)

```python
df_tx_users_cards = df_tx_users.join(
    df_cards.withColumnRenamed("id", "card_id"),
    on="card_id",
    how="left"
)

# ¿Cuántas transacciones no tienen tarjeta asociada?
sin_tarjeta = df_tx_users_cards.filter(F.col("card_type").isNull()).count()
print(f"Transacciones sin tarjeta asociada: {sin_tarjeta:,}")
```

Responde: ¿Por qué usamos LEFT en vez de INNER aquí? ¿Qué perderíamos con INNER?

---

### JOIN 3: + mcc_codes (LEFT JOIN)

```python
# mcc_codes puede tener estructura anidada — explorar primero
df_mcc.printSchema()
df_mcc.show(5, truncate=False)

# Ajustar según la estructura real del JSON
df_full = df_tx_users_cards.join(
    df_mcc,
    df_tx_users_cards["mcc"] == df_mcc["mcc"],
    how="left"
)

# Ver categorías de comercio más frecuentes
df_full.groupBy("merchant_description") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .limit(10) \
    .show(truncate=False)
```

> **Pista:** El JSON de MCC puede tener el código como string o integer. Verifica y castea si es necesario.

---

### JOIN 4: + fraud_labels (LEFT JOIN)

```python
# fraud_labels: id de transacción + label (0=legítima, 1=fraude)
df_fraud_renamed = df_fraud \
    .withColumnRenamed("id", "transaction_id") \
    .withColumnRenamed("label", "is_fraud")

df_final = df_full.join(df_fraud_renamed, on="transaction_id", how="left")

# ¿Cuántas transacciones tienen etiqueta de fraude?
print("Distribución de labels:")
df_final.groupBy("is_fraud").count().show()

# ¿Cuántas no tienen label?
sin_label = df_final.filter(F.col("is_fraud").isNull()).count()
print(f"Sin etiqueta de fraude: {sin_label:,}")
```

---

## Parte 5 — Anti JOIN: encontrar lo que no coincide

```python
# Transacciones que NO tienen usuario registrado
tx_sin_usuario = df_transactions.join(
    df_users.withColumnRenamed("id", "user_id"),
    on="user_id",
    how="left_anti"
)
print(f"Transacciones sin usuario: {tx_sin_usuario.count():,}")

# Usuarios que NUNCA hicieron una transacción
usuarios_sin_tx = df_users.join(
    df_transactions.withColumnRenamed("user_id", "id"),
    on="id",
    how="left_anti"
)
print(f"Usuarios sin transacciones: {usuarios_sin_tx.count():,}")
```

Documenta: ¿qué te dice esto sobre la calidad del modelo de datos?

---

## Parte 6 — Análisis sobre el DataFrame unido

Con `df_final` (las 5 tablas unidas), responde estas preguntas usando PySpark:

1. ¿Cuál es el tipo de tarjeta con mayor tasa de fraude?
2. ¿Qué categoría de comercio (MCC) concentra más transacciones fraudulentas?
3. ¿Cuál es el monto promedio de una transacción fraudulenta vs una legítima?
4. ¿El fraude es más frecuente en fin de semana o entre semana?

```python
# Ejemplo de estructura para responder la pregunta 3
df_final.groupBy("is_fraud") \
    .agg(
        F.count("transaction_id").alias("total"),
        F.avg("amount").alias("monto_promedio"),
        F.percentile_approx("amount", 0.5).alias("mediana_monto")
    ) \
    .show()
```

---

## Entrega en Git

```bash
git add semana_02/actividades/actividad_02/<tu-nombre>/
git commit -m "feat: add joins notebook with 5-table financial model - <tu-nombre>"
git push origin feature/semana02-joins-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 02] JOINs en PySpark — <Tu Nombre>
```

Describe en el PR:
- ¿Cuántos registros perdiste con el INNER JOIN de users? ¿Por qué?
- ¿Qué tipo de tarjeta tiene más fraude?
- El hallazgo más interesante del análisis

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Modelo de datos documentado | Diagrama + tabla de relaciones | 15% |
| Las 5 tablas cargadas correctamente | Incluyendo los JSON | 15% |
| 4 JOINs implementados con tipo correcto | INNER / LEFT / LEFT_ANTI | 30% |
| Pérdida de registros documentada | Con explicación de por qué | 15% |
| Análisis de fraude sobre DataFrame unido | Las 4 preguntas respondidas | 20% |
| Commits descriptivos | Mínimo 3 commits | 5% |

---

## Referencias

- [PySpark DataFrame Join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [Actividad 01 semana 02](../actividad_01/README.md) — base para esta actividad
