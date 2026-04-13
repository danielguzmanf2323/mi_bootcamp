# Actividad 02 — Semana 06: MERGE INTO y Patrones de Ingesta Incremental

**Semana:** 06  
**Tema:** Upserts, deduplicación y patrones de ingesta incremental  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise

---

## Objetivo

Dominar `MERGE INTO` como herramienta central de ingesta incremental en Delta Lake:
cuándo usarlo, cómo construir los distintos patrones (upsert, deduplicación, SCD1),
y cuándo elegirlo sobre las alternativas que ya conoces (DLT `apply_changes`, overwrite).

---

## Dataset

Las tablas Delta de semanas anteriores + un batch de datos nuevos que simularás
en el notebook. No necesitas descargar nada.

El batch simulado contiene:
- Transacciones completamente nuevas
- Transacciones que ya existen en Silver pero con campos actualizados (e.g., estado cambiado de `pending` a `completed`)
- Duplicados dentro del propio batch (mismo `transaction_id` dos veces)

---

## Material de estudio previo

1. ¿Qué diferencia hay entre un **upsert** y una **sobreescritura completa** en términos de coste y riesgo?
2. ¿Cómo funciona el `MERGE INTO` de SQL estándar? ¿Qué cláusulas tiene y qué hace cada una?
3. ¿En qué casos `MERGE INTO` en Spark puede ser muy lento? ¿Por qué?
4. ¿Qué es la **deduplicación idempotente** y por qué importa en pipelines de datos?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana06-merge-incremental-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_06/actividades/actividad_02/<tu-nombre>/
```

---

## Parte 1 — Generar el batch de datos nuevos

Simula la llegada de un nuevo batch con los tres tipos de registros:

```python
from pyspark.sql import Row
from datetime import datetime

# Leer silver actual para conocer su estructura
silver = spark.table("silver.transactions")

# Batch con 3 tipos de registros:
batch_data = [
    # 1. Registros completamente nuevos
    Row(transaction_id="TX_NEW_001", amount=150.0, category="travel",
        status="completed", transaction_date="2024-06-01", customer_id="C001"),
    Row(transaction_id="TX_NEW_002", amount=75.5, category="food",
        status="pending", transaction_date="2024-06-01", customer_id="C002"),

    # 2. Registros existentes con actualización de estado
    # (usa transaction_ids que ya existan en tu silver.transactions)
    Row(transaction_id="<ID_EXISTENTE_1>", amount=200.0, category="shopping",
        status="completed", transaction_date="2024-05-15", customer_id="C003"),

    # 3. Duplicado dentro del mismo batch (mismo transaction_id)
    Row(transaction_id="TX_NEW_001", amount=150.0, category="travel",
        status="completed", transaction_date="2024-06-01", customer_id="C001"),
]

df_batch = spark.createDataFrame(batch_data)
df_batch.write.format("delta").saveAsTable("bronze.transactions_batch_test")
```

Commit esperado:
```bash
git commit -m "feat: create simulated incremental batch for merge testing"
```

---

## Parte 2 — MERGE INTO básico (upsert)

```sql
MERGE INTO silver.transactions AS target
USING bronze.transactions_batch_test AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
  UPDATE SET
    target.status = source.status,
    target.amount = source.amount

WHEN NOT MATCHED THEN
  INSERT (transaction_id, amount, category, status, transaction_date, customer_id)
  VALUES (source.transaction_id, source.amount, source.category,
          source.status, source.transaction_date, source.customer_id)
```

Después del MERGE:
1. Verifica que los registros nuevos se insertaron
2. Verifica que los existentes se actualizaron
3. Verifica cuántas veces aparece `TX_NEW_001` — ¿el duplicado del batch entró dos veces?

Documenta el problema que encontraste con los duplicados.

Commit esperado:
```bash
git commit -m "feat: implement basic upsert with MERGE INTO"
```

---

## Parte 3 — Deduplicación antes del MERGE

El MERGE del paso anterior no maneja duplicados dentro del propio batch.
La solución estándar: deduplicar el source antes de usarlo.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Deduplicar el batch: quedarse con la fila más reciente por transaction_id
w = Window.partitionBy("transaction_id").orderBy(F.col("transaction_date").desc())

df_deduped = (
    spark.table("bronze.transactions_batch_test")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

df_deduped.createOrReplaceTempView("batch_deduped")
```

Luego ejecuta el MERGE usando `batch_deduped` como source. Verifica que ahora
`TX_NEW_001` aparece una sola vez.

Commit esperado:
```bash
git commit -m "feat: add deduplication step before MERGE"
```

---

## Parte 4 — Patrón SCD1 con MERGE (solo actualizar si hay cambio real)

Un refinamiento importante: solo actualizar si el valor realmente cambió.
Evita reescrituras innecesarias y facilita el audit trail.

```sql
MERGE INTO silver.transactions AS target
USING batch_deduped AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED AND target.status <> source.status THEN
  UPDATE SET target.status = source.status

WHEN NOT MATCHED THEN
  INSERT *
```

Documenta: ¿cuántos registros actualizó esta versión vs la anterior? ¿Por qué es importante esa diferencia?

Commit esperado:
```bash
git commit -m "feat: implement conditional update in MERGE (SCD1 pattern)"
```

---

## Parte 5 — Comparativa: MERGE vs DLT apply_changes vs Overwrite

Crea una tabla en tu notebook con esta comparativa y complétala:

| Criterio | MERGE INTO | DLT apply_changes | Overwrite |
|----------|------------|-------------------|-----------|
| Soporte para upserts | ✓ | ✓ | ✗ |
| Maneja SCD2 nativo | ✗ | ✓ | ✗ |
| Requiere DLT pipeline | | | |
| Coste en tablas grandes | | | |
| Idempotente por defecto | | | |
| Cuándo usarlo | | | |

Documenta cuál usarías para cada uno de estos escenarios:
- Tabla de transacciones que recibe miles de updates diarios
- Dimensión de clientes con historial de cambios (SCD2)
- Tabla que se regenera completa cada noche desde un sistema externo

Commit esperado:
```bash
git commit -m "docs: add comparison MERGE vs apply_changes vs overwrite"
```

---

## Entrega en Git

```bash
git push origin feature/semana06-merge-incremental-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 06] MERGE INTO e Ingesta Incremental — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Batch simulado con los 3 tipos de registros | Nuevo, actualización, duplicado | 15% |
| [Requerido] MERGE básico funcionando | Inserción y actualización verificadas | 25% |
| [Requerido] Deduplicación implementada y verificada | Sin duplicados en el resultado | 25% |
| [Requerido] Comparativa MERGE vs alternativas completa | Tabla con los 3 escenarios resueltos | 25% |
| [Recomendado] Update condicional implementado | Solo actualiza cuando hay cambio real | 10% |
| **Total** | | **100%** |

---

## Referencias

- [Delta Lake MERGE INTO (Databricks docs)](https://docs.databricks.com/sql/language-manual/delta-merge-into.html)
- [Upsert into Delta Lake (Delta Lake OSS docs)](https://delta.io/blog/2023-02-14-delta-lake-merge/)
- [SCD Type 1 and 2 with Delta Lake](https://www.databricks.com/blog/2019/09/19/diving-into-delta-lake-schema-enforcement-evolution.html)
