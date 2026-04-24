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

Los mismos 5 archivos de semana 02. Si no los tienes aún, descárgalos del sitio de Teams del bootcamp (SharePoint):

**[inetum_data_engineer_bootcamp / semana_02 / financial_transaction_dataset](https://gfi1.sharepoint.com/sites/JUNIORDATAENGINEERSDEVTEAM/Documents%20partages/Forms/AllItems.aspx?id=%2Fsites%2FJUNIORDATAENGINEERSDEVTEAM%2FDocuments%20partages%2FGeneral%2Finetum%5Fdata%5Fengineer%5Fbootcamp&viewid=532715df%2D69df%2D4d0e%2D8785%2Daf7a4ccf2983)**

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

# Ruta base del volumen en Unity Catalog
VOL = "/Volumes/workspace/default/week_2"

# Tablas CSV
df_transactions = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load(f"{VOL}/transactions_data.csv")
df_users = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load(f"{VOL}/users_data.csv")
df_cards = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
    .load(f"{VOL}/cards_data.csv")

# MCC codes — JSON de un solo objeto: cada clave es un código MCC, el valor es la descripción
# Spark lo lee como 1 fila × N columnas → se pivota en la celda siguiente
df_mcc_raw = spark.read.option("multiLine", "true").json(f"{VOL}/mcc_codes.json")
```

El JSON de MCC tiene formato ancho (1 fila, una columna por código). Hay que pivotarlo a formato largo (`mcc | description`) antes del JOIN:

```python
# Pivotar mcc_codes: de ancho (1 fila × N cols) a largo (N filas × 2 cols)
mcc_cols = df_mcc_raw.columns
stack_expr = (
    f"stack({len(mcc_cols)}, "
    + ", ".join([f"'{c}', `{c}`" for c in mcc_cols])
    + ") as (mcc_str, description)"
)
df_mcc = (
    df_mcc_raw
    .select(F.expr(stack_expr))
    .withColumn("mcc", F.col("mcc_str").cast("int"))
    .drop("mcc_str")
)
print(f"Categorías MCC: {df_mcc.count()}")
df_mcc.show(5, truncate=False)
```

Fraud labels está en Parquet (ya disponible en el volumen `week_2`). Si por alguna razón el archivo no está, la celda siguiente lo convierte como fallback:

```python
# Fraud labels — leer desde Parquet
# El archivo train_fraud_labels.parquet ya está subido al volumen week_2.
# Si no lo tienes, esta celda convierte el JSON como alternativa.

try:
    df_fraud = spark.read.parquet(f"{VOL}/train_fraud_labels.parquet")
    print("✓ Parquet cargado correctamente")
except Exception:
    print("⚠ Parquet no encontrado — convirtiendo desde JSON (puede tardar varios minutos)...")
    df_fraud = spark.read.json(f"{VOL}/train_fraud_labels.json")
    df_fraud.write.mode("overwrite").parquet(f"{VOL}/train_fraud_labels.parquet")
    df_fraud = spark.read.parquet(f"{VOL}/train_fraud_labels.parquet")
    print("✓ Conversión completada y Parquet relanzado")
```

```python
# Verificar conteos
for nombre, df in [("transactions", df_transactions), ("users", df_users),
                    ("cards", df_cards), ("mcc", df_mcc), ("fraud", df_fraud)]:
    print(f"{nombre}: {df.count():,} registros | {len(df.columns)} columnas")
```

> **Investigar:** ¿Por qué los archivos JSON se leen diferente al CSV? ¿Qué hace `multiLine`? ¿Qué ventajas tiene Parquet sobre JSON para procesamiento en Spark?

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
# df_mcc ya está en formato largo (mcc int | description string) tras el pivote de Parte 2
# transactions.mcc también es int con inferSchema → JOIN directo por "mcc"
df_full = df_tx_users_cards.join(df_mcc, on="mcc", how="left")

# Ver categorías de comercio más frecuentes
df_full.groupBy("description") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .limit(10) \
    .show(truncate=False)
```

> **Nota:** Si el JOIN produce 0 coincidencias, verifica que `transactions.mcc` sea integer con `df_tx_users_cards.schema["mcc"].dataType`. Si es string, añade `.withColumn("mcc", F.col("mcc").cast("int"))` antes del JOIN.

---

### JOIN 4: + fraud_labels (LEFT JOIN)

```python
# fraud_labels: id de transacción + target ("Yes"=fraude, "No"=legítima)
df_fraud_renamed = df_fraud \
    .withColumnRenamed("id", "transaction_id") \
    .withColumnRenamed("target", "is_fraud")

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
# is_fraud tiene valores "Yes" (fraude) y "No" (legítima)
df_final.groupBy("is_fraud") \
    .agg(
        F.count("transaction_id").alias("total"),
        F.avg("amount").alias("monto_promedio"),
        F.percentile_approx("amount", 0.5).alias("mediana_monto")
    ) \
    .show()
```

---

## Persistir df_final en Delta

Antes de cerrar el notebook, guarda `df_final` como tabla Delta en Unity Catalog. La **Actividad 03** la leerá desde ahí sin repetir todos los JOINs.

```python
# Guardar df_final como tabla Delta en el esquema default del catálogo workspace
# Esto crea workspace.default.financial_final accesible desde cualquier notebook
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.financial_final")

print("✓ df_final guardado como workspace.default.financial_final")
print(f"  Filas: {df_final.count():,} | Columnas: {len(df_final.columns)}")
print(f"  Columnas: {df_final.columns}")
```

> `saveAsTable` registra la tabla en el catálogo Unity Catalog — cualquier notebook del workspace puede leerla con `spark.table("workspace.default.financial_final")` o desde el Data Explorer. Si prefieres un esquema propio, cámbialo por `workspace.<tu_esquema>.financial_final`.

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

## Laboratorio

Completa el notebook de laboratorio con un dataset relacional de tu elección:

**[`semana_02/laboratorios/lab_02_joins.ipynb`](../../laboratorios/lab_02_joins.ipynb)**

El notebook te guía por 6 partes: descripción del dataset y modelo de relaciones, perfil técnico de cada tabla (nulos, duplicados en llaves, cardinalidades), transformaciones previas al JOIN, INNER + LEFT + Anti JOIN con análisis de impacto, análisis de negocio y reflexión final.

---

## Referencias(https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [Actividad 01 semana 02](../actividad_01/README.md) — base para esta actividad
