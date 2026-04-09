# Proyecto Semana 04 — Pipeline end-to-end metadata-driven

**Semana:** 04  
**Tema:** Integrar todo: YAML maestro, entornos dev/prod, Job completo Bronze → Silver → Gold  
**Nivel:** Avanzado  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise  
**Prerequisito:** Actividades 01–04 completadas

---

## Contexto

En las actividades de esta semana construiste las piezas:
- **Act 01:** Widgets — parametrizar notebooks
- **Act 02:** Bronze notebook genérico — leer cualquier formato
- **Act 03:** YAML + orquestador — loop sin hardcode
- **Act 04:** Silver metadata-driven + Job de 2 tasks

Este proyecto las integra en un único sistema coherente: una pipeline de producción simulada con entornos dev y prod, tres capas (Bronze → Silver → Gold), y un Job de Databricks como orquestador central.

No hay nueva tecnología en este proyecto. El desafío es de **ingeniería** — diseñar algo que funcione de extremo a extremo, sea claro para quien lo mantenga en 6 meses, y soporte los dos entornos sin cambios de código.

---

## Dataset

Financial Transactions (Caixabank Tech, mismo dataset de semanas 02-03):

| Archivo | Formato | Tabla Bronze |
|---------|---------|-------------|
| `transactions_data.csv` | CSV | `bronze.transactions` |
| `users_data.csv` | CSV | `bronze.users` |
| `cards_data.csv` | CSV | `bronze.cards` |
| `mcc_codes.json` | JSON | `bronze.mcc_codes` |
| `train_fraud_labels.json` | JSON | `bronze.fraud_labels` |

Tabla Silver oficial:
- `silver.transactions` — tabla principal (datos limpios + features calculadas)
- `silver.users` — datos demográficos normalizados
- `silver.cards` — tarjetas con tipos codificados

Tabla Gold (nueva en este proyecto):
- `gold.reporte_fraude_diario` — agregado diario: transacciones por día, tasa de fraude, monto promedio

---

## Estructura del proyecto

```
semana_04/proyecto/<tu-nombre>/
├── pipeline_maestro.py        # Notebook principal — widget de entorno, orquesta todo
├── gold_notebook.py           # Notebook Gold — lee Silver, produce agregados
├── configs/
│   └── gold_config.yml        # (opcional) Configuración de la capa Gold
└── job_definition.json        # Job de Databricks exportado como JSON
```

Los configs de Bronze y Silver ya existen en `semana_04/configs/` y debes usarlos directamente (sin copiarlos).

---

## Parte 1 — Widget maestro de entorno

El pipeline maestro es el único lugar donde el entorno se define. Todos los notebooks que invoca heredan el entorno como parámetro.

```python
# pipeline_maestro.py

dbutils.widgets.removeAll()

dbutils.widgets.dropdown(
    name="environment",
    defaultValue="dev",
    choices=["dev", "prod"],
    label="Entorno de ejecución"
)

environment = dbutils.widgets.get("environment")

# Mapa de schemas por entorno y capa
SCHEMAS = {
    "dev": {
        "bronze": "bronze_dev",
        "silver": "silver_dev",
        "gold":   "gold_dev",
    },
    "prod": {
        "bronze": "bronze",
        "silver": "silver",
        "gold":   "gold",
    }
}

schemas = SCHEMAS[environment]
print(f"Entorno activo: {environment}")
print(f"Schemas: {schemas}")
```

> Regla: el mapa de schemas solo existe en este notebook. Los notebooks de Bronze, Silver y Gold reciben el schema como parámetro — nunca lo calculan solos.

---

## Parte 2 — Paso 1: Bronze via orquestador YAML

```python
import os

notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
repo_root = os.path.dirname(
             os.path.dirname(
             os.path.dirname(
             notebook_path)))

# Reutilizar el orquestador de Actividad 03
BRONZE_ORCHESTRATOR = f"/Workspace{repo_root}/semana_04/actividades/actividad_03/<tu-nombre>/orquestador_bronze"

print(f"Paso 1/3: Ejecutando Bronze ({schemas['bronze']})...")
resultado_bronze = dbutils.notebook.run(
    path=BRONZE_ORCHESTRATOR,
    timeout_seconds=1800,
    arguments={
        "environment": environment,
    }
)
print(f"Bronze completado: {resultado_bronze}")
```

---

## Parte 3 — Paso 2: Silver metadata-driven

```python
# Reutilizar el notebook Silver de Actividad 04
SILVER_NOTEBOOK = f"/Workspace{repo_root}/semana_04/actividades/actividad_04/<tu-nombre>/silver_generico"

print(f"\nPaso 2/3: Ejecutando Silver ({schemas['silver']})...")
resultado_silver = dbutils.notebook.run(
    path=SILVER_NOTEBOOK,
    timeout_seconds=1800,
    arguments={
        "environment": environment,
    }
)
print(f"Silver completado: {resultado_silver}")
```

> Nota: los notebooks de Act 03 y Act 04 deben aceptar `environment` como widget. Si no lo hacen, modifícalos para que sí lo hagan antes de continuar.

---

## Parte 4 — Paso 3: Gold — reporte de fraude diario

El notebook Gold lee de Silver y produce un agregado analítico. Es la primera transformación verdaderamente analítica del bootcamp.

Crea `gold_notebook.py`:

```python
# gold_notebook.py
# =======================================================================
# PROPÓSITO: Generar la tabla Gold de reporte de fraude diario
# FUENTES: silver.transactions (o silver_dev.transactions según entorno)
# DESTINO: gold.reporte_fraude_diario (o gold_dev.reporte_fraude_diario)
# PARÁMETROS: environment (dev | prod)
# =======================================================================

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "Entorno")

environment = dbutils.widgets.get("environment")

SCHEMAS = {
    "dev":  {"silver": "silver_dev", "gold": "gold_dev"},
    "prod": {"silver": "silver",     "gold": "gold"},
}
schemas = SCHEMAS[environment]

fuente  = f"{schemas['silver']}.transactions"
destino = f"{schemas['gold']}.reporte_fraude_diario"

print(f"Gold: {fuente} → {destino}")
```

```python
from pyspark.sql.functions import (
    col, to_date, count, sum as spark_sum, avg, round as spark_round
)

# Leer transactions de Silver
df_silver = spark.table(fuente)

# Calcular reporte diario de fraude
df_gold = (
    df_silver
    .withColumn("fecha", to_date(col("transaction_date")))
    .groupBy("fecha")
    .agg(
        count("*").alias("total_transacciones"),
        spark_sum(col("is_fraud")).alias("transacciones_fraude"),
        spark_round(
            (spark_sum(col("is_fraud")) / count("*")) * 100, 2
        ).alias("tasa_fraude_pct"),
        spark_round(avg(col("amount_abs")), 2).alias("monto_promedio"),
        spark_sum(col("amount_abs")).alias("monto_total"),
    )
    .orderBy("fecha")
)

print(f"Días en el reporte: {df_gold.count():,}")
df_gold.show(15)
```

```python
# Verificar que `is_fraud` existe en Silver (viene del join con fraud_labels)
# Si no existe, crear la columna con valor 0 para no romper el reporte
from pyspark.sql.functions import lit

if "is_fraud" not in df_silver.columns:
    print("ADVERTENCIA: is_fraud no encontrado en Silver. Usando 0 como valor por defecto.")
    df_silver = df_silver.withColumn("is_fraud", lit(0))
```

```python
# Escribir en Gold
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schemas['gold']}")

df_gold.write.format("delta").mode("overwrite").saveAsTable(destino)

n_filas = df_gold.count()
print(f"Tabla Gold: {destino} | {n_filas} días | escrito en {environment}")

dbutils.notebook.exit(f"OK: {destino} | {n_filas} días")
```

---

## Parte 5 — Verificación end-to-end

Crea una celda de verificación que confirma que las 3 capas tienen datos:

```python
# Verificación del pipeline completo
verificaciones = {
    f"{schemas['bronze']}.transactions": "Bronze transactions",
    f"{schemas['silver']}.transactions": "Silver transactions",
    f"{schemas['gold']}.reporte_fraude_diario": "Gold reporte fraude",
}

print("\nVERIFICACIÓN FINAL DE CAPAS")
print("=" * 60)
for tabla, descripcion in verificaciones.items():
    try:
        n = spark.table(tabla).count()
        print(f"  OK  {descripcion:30s} {n:>12,} filas")
    except Exception as e:
        print(f"  ERR {descripcion:30s} → {e}")
print("=" * 60)
```

---

## Parte 6 — Job de Databricks: 3 tasks encadenadas

Crea un nuevo Job con 3 tasks:

| Task | Nombre | Notebook | Depende de |
|------|--------|----------|------------|
| 1 | `bronze_ingestion` | orquestador_bronze (Act 03) | — |
| 2 | `silver_transform` | silver_generico (Act 04) | `bronze_ingestion` |
| 3 | `gold_report` | gold_notebook (este proyecto) | `silver_transform` |

Para cada task, configura el parámetro:
```
environment = dev
```

Ejecuta el Job en modo `dev`. Una vez validado, cámbialo a `prod` y vuelve a ejecutar.

---

## Parte 7 — Diferencias entre entornos

Corre el pipeline completo en `dev` y luego en `prod`. Documenta:

```python
# Comparar conteos entre dev y prod
for capa in ["bronze", "silver"]:
    tabla_dev  = f"{SCHEMAS['dev'][capa]}.transactions"
    tabla_prod = f"{SCHEMAS['prod'][capa]}.transactions"

    try:
        n_dev  = spark.table(tabla_dev).count()
        n_prod = spark.table(tabla_prod).count()
        match  = "OK" if n_dev == n_prod else "DIFERENCIA"
        print(f"  {capa}: dev={n_dev:,} | prod={n_prod:,} | {match}")
    except Exception as e:
        print(f"  {capa}: ERROR — {e}")
```

¿Son iguales los conteos en dev y prod? ¿Deberían serlo? ¿En qué escenario real tendrían conteos diferentes?

---

## Análisis final

Una vez que el pipeline completo corra en `prod`, responde estas preguntas con datos reales:

```sql
-- 1. ¿Cuál es el día con mayor número de transacciones fraudulentas?
SELECT fecha, transacciones_fraude, tasa_fraude_pct
FROM gold.reporte_fraude_diario
ORDER BY transacciones_fraude DESC
LIMIT 5;

-- 2. ¿Cuál es la tasa de fraude promedio del dataset?
SELECT AVG(tasa_fraude_pct) AS tasa_fraude_promedio
FROM gold.reporte_fraude_diario;

-- 3. ¿En qué mes hubo mayor fraude?
SELECT month(fecha) AS mes, SUM(transacciones_fraude) AS total_fraude_mes
FROM gold.reporte_fraude_diario
GROUP BY mes
ORDER BY total_fraude_mes DESC;
```

---

## Entrega en Git

```bash
git add semana_04/proyecto/<tu-nombre>/
git commit -m "feat: end-to-end metadata-driven pipeline dev/prod - semana 04 proyecto - <tu-nombre>"
git push origin feature/semana04-widgets-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 04 Proyecto] Pipeline metadata-driven end-to-end — <Tu Nombre>
```

El PR debe contener:
- Screenshot del Job de 3 tasks corriendo en `dev` (todo en verde)
- Screenshot del Job corriendo en `prod`
- El archivo `job_definition.json` exportado desde Databricks
- Las respuestas al análisis final (con datos reales de las queries)
- Una tabla que compara conteos dev vs prod por capa

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Pipeline maestro con widget de entorno | Un solo widget controla dev/prod | 15% |
| Bronze vía orquestador YAML reutilizado | Sin código duplicado de Act. 03 | 15% |
| Silver metadata-driven reutilizado | Sin código duplicado de Act. 04 | 15% |
| Notebook Gold funcionando | Tabla `reporte_fraude_diario` creada | 20% |
| Job con 3 tasks encadenadas | `depends_on` configurado correctamente | 15% |
| Pipeline corrido en dev Y prod | Screenshots de ambos entornos | 10% |
| Análisis final con datos reales | 3 queries respondidas | 10% |

---

## Reflexión de cierre

En el PR, incluye un párrafo (no un checklist) que responda:

> "Antes del bootcamp / antes de esta semana, ¿cómo habrías escrito este pipeline? ¿Qué cambia ahora en la forma en que piensas el código? ¿Qué parte del diseño metadata-driven te pareció más difícil de entender?"

No hay respuesta correcta o incorrecta. Queremos saber cómo evolucionó tu razonamiento.
