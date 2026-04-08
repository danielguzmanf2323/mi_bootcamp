# Actividad 04 — Arquitectura Medallón con el dataset Customers

**Semana:** 01  
**Tema:** Arquitectura Medallón (Bronze / Silver / Gold)  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Antes de empezar

Estudia el material de la semana sobre Arquitectura Medallón:

- Video y curso disponibles en el canal del bootcamp — revísalos antes de abrir Databricks.

Preguntas que debes poder responder después de estudiar el material:

- ¿Qué problema resuelve la arquitectura medallón?
- ¿Qué tipo de dato vive en cada capa?
- ¿Por qué Bronze no se toca?
- ¿Qué diferencia a Silver de Gold?

---

## Contexto

Ya leíste el dataset `customers`, lo perfilaste y le hiciste SQL. Ahora vas a organizarlo como lo haría un equipo de Data Engineering real: en tres capas bien definidas, con una razón clara para cada transformación.

Al final de esta actividad tendrás tres tablas en Databricks — `bronze_customers`, `silver_customers` y `gold_customers` — y un documento que explica cada decisión que tomaste.

---

## ¿Qué es la Arquitectura Medallón?

```
Fuente de datos
      │
      ▼
┌─────────────┐
│   BRONZE    │  Dato crudo. Se ingesta tal como llega. Sin transformar.
│  (Raw)      │  Es la fuente de verdad histórica. Nunca se modifica.
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Dato limpio. Se corrigen nulos, tipos, duplicados.
│  (Cleaned)  │  Confiable para análisis. Estructura estandarizada.
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Dato agregado y listo para consumo.
│ (Aggregated)│  Responde preguntas de negocio. Lo consume BI, reportes, ML.
└─────────────┘
```

---

## Paso 1 — Diseñar el diagrama antes de codear

Antes de abrir un notebook, crea un documento `diagrama_<tu-nombre>.md` dentro de tu carpeta de entrega con el diseño de las tres capas para el dataset `customers`.

El documento debe responder:

### Bronze
- ¿Qué archivo vas a ingestar? ¿En qué formato?
- ¿Qué columnas tiene la tabla tal como llega?
- ¿Qué NO vas a hacer en esta capa?

### Silver
- ¿Qué columnas tienen nulos? ¿Cómo los tratarás?
- ¿Hay nombres de columnas que estandarizar? (ejemplo: `ContactName` → `contact_name`)
- ¿Hay duplicados posibles? ¿Cómo los detectas?
- ¿Qué columnas no aportan valor y podrías descartar?

### Gold
- ¿Qué pregunta de negocio responde tu tabla Gold?
- ¿Qué columnas tendrá?
- ¿Qué agrupación o cálculo se aplica?

Ejemplo de estructura esperada en el documento:

```markdown
## Bronze — customers_raw
- Fuente: customers.csv
- Columnas: customer_id, company_name, contact_name, country, city, phone
- Sin transformaciones. Se guarda tal como llega.

## Silver — customers_clean
- Eliminar filas donde customer_id es nulo
- Renombrar columnas a snake_case
- Eliminar duplicados por customer_id
- Estandarizar valores de country (mayúsculas)

## Gold — customers_by_country
- Total de clientes por país
- Top 5 países con más clientes
- Columnas: country, total_customers, ranking
```

Commit esperado:
```bash
git commit -m "docs: add medallion architecture design for customers"
```

---

## Paso 2 — Implementar Bronze en Databricks

Crea el notebook `bronze_customers_<tu-nombre>.ipynb`.

**Objetivo:** leer el archivo raw y guardarlo como tabla Delta sin ninguna transformación.

```python
# Leer el archivo fuente (ajusta la ruta según donde subiste el archivo)
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/customers.csv")

# Verificar que llegó completo
print(f"Registros: {df_bronze.count()}")
df_bronze.printSchema()
df_bronze.show(5)

# Guardar como tabla Delta (Bronze)
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_customers")
```

> **Regla de Bronze:** no filtres nada, no cambies nada. Si el archivo tiene nulos, errores o columnas raras — Bronze los guarda igual. Eso es intencional.

Commit esperado:
```bash
git commit -m "feat: add bronze layer notebook for customers"
```

---

## Paso 3 — Implementar Silver en Databricks

Crea el notebook `silver_customers_<tu-nombre>.ipynb`.

**Objetivo:** leer desde Bronze y aplicar las transformaciones de limpieza que diseñaste en el diagrama.

```python
# Leer desde Bronze (siempre desde la capa anterior, nunca desde el archivo)
df_silver = spark.read.table("bronze_customers")

# --- Tus transformaciones van aquí ---

# Ejemplo: eliminar nulos en columna clave
df_silver = df_silver.dropna(subset=["customer_id"])

# Ejemplo: renombrar columnas a snake_case
df_silver = df_silver.withColumnRenamed("CustomerID", "customer_id") \
                     .withColumnRenamed("CompanyName", "company_name") \
                     .withColumnRenamed("ContactName", "contact_name") \
                     .withColumnRenamed("Country", "country") \
                     .withColumnRenamed("City", "city")

# Ejemplo: eliminar duplicados
df_silver = df_silver.dropDuplicates(["customer_id"])

# Verificar resultado
print(f"Registros después de limpieza: {df_silver.count()}")
df_silver.show(5)

# Guardar como tabla Delta (Silver)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")
```

> **Pista:** compara `df_bronze.count()` con `df_silver.count()`. Si son iguales, probablemente no hiciste ninguna limpieza real. Revisa el perfilamiento de la Act 03.

Commit esperado:
```bash
git commit -m "feat: add silver layer notebook for customers"
```

---

## Paso 4 — Implementar Gold en Databricks

Crea el notebook `gold_customers_<tu-nombre>.ipynb`.

**Objetivo:** leer desde Silver y construir una tabla agregada que responda una pregunta de negocio.

```python
# Leer desde Silver
df_gold = spark.read.table("silver_customers")

# Registrar como vista temporal para usar SQL
df_gold.createOrReplaceTempView("silver_customers_view")

# Construir la tabla Gold con SQL
df_gold_result = spark.sql("""
    SELECT
        country,
        COUNT(*) AS total_customers,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
    FROM silver_customers_view
    GROUP BY country
    ORDER BY total_customers DESC
""")

df_gold_result.show(10)

# Guardar como tabla Delta (Gold)
df_gold_result.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customers_by_country")
```

> **Reto adicional (opcional):** crea una segunda tabla Gold que responda otra pregunta, por ejemplo: `gold_customers_top_names` con los 10 nombres de contacto más frecuentes.

Commit esperado:
```bash
git commit -m "feat: add gold layer notebook for customers"
```

---

## Paso 5 — Documento de reflexión

En el mismo archivo `diagrama_<tu-nombre>.md`, agrega una sección final:

```markdown
## Reflexión

### ¿Qué cambió entre Bronze y Silver?
(cuántos registros se eliminaron, qué columnas se tocaron y por qué)

### ¿Qué pregunta de negocio responde tu tabla Gold?
(descríbela en una oración)

### ¿Qué pasaría si alguien modifica Bronze directamente?
(razona sobre por qué Bronze no se toca nunca)

### ¿Qué agregarías a Silver o Gold si tuvieras más tiempo?
```

Commit esperado:
```bash
git commit -m "docs: add medallion architecture reflection"
```

---

## Paso 6 — Entrega en Git

```bash
git push origin feature/semana01-medallon-<tu-nombre>
```

Abre un Pull Request hacia `develop` con el título:

```
[Semana 01] Arquitectura Medallón — <Tu Nombre>
```

En la descripción del PR incluye:
- ¿Cuántos registros tiene cada capa?
- ¿Qué fue lo más difícil de implementar?
- ¿Qué pregunta responde tu tabla Gold?

---

## Entregables esperados

```
semana_01/actividades/actividad_04/<tu-nombre>/
├── diagrama_<tu-nombre>.md
├── bronze_customers_<tu-nombre>.ipynb
├── silver_customers_<tu-nombre>.ipynb
└── gold_customers_<tu-nombre>.ipynb
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Diagrama completo antes del código | Las tres capas diseñadas con decisiones justificadas | 20% |
| Bronze correcto | Sin transformaciones, datos raw completos | 15% |
| Silver con limpieza real | Al menos 3 transformaciones aplicadas y justificadas | 25% |
| Gold con agregación funcional | Responde una pregunta de negocio concreta | 25% |
| Reflexión documentada | Sección de reflexión completa en el diagrama | 10% |
| Commits descriptivos (mínimo 4) | Un commit por entregable | 5% |

---

## Lo que NO se debe hacer

- No leer el archivo original directamente en Silver o Gold — cada capa lee de la capa anterior
- No modificar Bronze una vez creado
- No hacer las tres capas en un solo notebook
- No omitir el diagrama — el diseño va antes que el código

---

## Referencias

- [GITFLOW.md](../../GITFLOW.md) — flujo de trabajo Git del bootcamp
- [Actividad 03](../actividad_03/README.md) — perfilamiento del dataset customers
- [Documentación Delta Lake](https://docs.delta.io/latest/index.html)
- [Databricks — Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
