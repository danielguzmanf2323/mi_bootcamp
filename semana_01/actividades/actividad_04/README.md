# Actividad 04 — Arquitectura Medallón con el dataset Customers

**Semana:** 01  
**Tema:** Arquitectura Medallón (Bronze / Silver / Gold)  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Antes de empezar

Estudia el material de la semana sobre Arquitectura Medallón:

- Video y curso disponibles en las fuentes citadas al final del documento — revísalos antes de abrir Databricks.

Preguntas que debes poder responder después de estudiar el material:

- ¿Qué problema resuelve la arquitectura medallón?
- ¿Qué tipo de dato vive en cada capa?
- ¿Por qué Bronze no se toca?
- ¿Qué diferencia a Silver de Gold?

---

## Contexto

Ya leíste el dataset `customers`, lo perfilaste y le hiciste SQL. Ahora vas a organizarlo como lo haría un equipo de Data Engineering real: en tres capas bien definidas, con una razón clara para cada transformación.

En el sitio de Teams del bootcamp (SharePoint) encontrarás el mismo dataset customers, pero con 2 millones de registros:

**[inetum_data_engineer_bootcamp / semana_01 / customers-2000000](https://gfi1.sharepoint.com/sites/JUNIORDATAENGINEERSDEVTEAM/Documents%20partages/Forms/AllItems.aspx?id=%2Fsites%2FJUNIORDATAENGINEERSDEVTEAM%2FDocuments%20partages%2FGeneral%2Finetum%5Fdata%5Fengineer%5Fbootcamp&viewid=532715df%2D69df%2D4d0e%2D8785%2Daf7a4ccf2983)**

> Navega dentro del sitio a: `General / inetum_data_engineer_bootcamp / semana_01 / customers-2000000`

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
- Fuente: customers-2000000.csv  (disponible en SharePoint: semana_01/customers-2000000)
- Columnas: index, Customer_id, First_Name, Last_Name, Company, City, Country, Phone_1, Phone_2, Email, Subscription_Date, Website
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
MI_NOMBRE = "<tu_nombre>"  # ej: "maria", "carlos" — sin espacios, en minúsculas

# Leer el archivo fuente (ajusta la ruta según donde subiste el archivo)
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \  # Bronze: siempre inferSchema=false — preserva el dato exactamente como llega
    .load("/FileStore/customers-2000000.csv")

# Verificar que llegó completo
print(f"Registros: {df_bronze.count()}")
df_bronze.printSchema()
display(df_bronze)  # En Databricks: display() es preferible a show()

# Guardar como tabla Delta (Bronze) — sufija tu nombre para no pisar tablas de otros
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"bronze_customers_{MI_NOMBRE}")
```

> **Regla de Bronze:** no filtres nada, no cambies nada. Si el archivo tiene nulos, errores o columnas raras — Bronze los guarda igual. Eso es intencional.
>
> **¿Por qué `inferSchema=false` en Bronze?** Con `inferSchema=true`, Spark hace una pasada extra sobre el archivo para adivinar los tipos — en datasets de millones de filas esto es costoso y puede inferir tipos incorrectos (ej: IDs numéricos tratados como enteros). En Bronze queremos el dato crudo tal como llega: todo como `StringType`. Los tipos se aplican en Silver, donde tienes control total.

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
    .saveAsTable(f"silver_customers_{MI_NOMBRE}")
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
    .saveAsTable(f"gold_customers_by_country_{MI_NOMBRE}")
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
| Bronze correcto | Sin transformaciones, datos raw completos, `inferSchema=false` | 15% |
| Silver con limpieza real | Al menos 3 transformaciones aplicadas y justificadas | 25% |
| Gold con agregación funcional | Responde una pregunta de negocio concreta | 20% |
| Dataset subido al Volumen en la ruta correcta | `/default/<tu_nombre>/semana_01/customers-2000000/` | 5% |
| Reflexión documentada | Sección de reflexión completa en el diagrama | 10% |
| Commits descriptivos (mínimo 4) | Un commit por entregable | 5% |

---

## Instrucciones de entrega en Databricks Volumes

Carga el archivo `customers-2000000.csv` al Volumen del entorno Databricks en la siguiente ruta antes de hacer el PR:

```
/Volumes/main/default/<tu_nombre>/semana_01/customers-2000000/
```

El instructor verificará que el archivo esté en esa ruta para poder ejecutar tu notebook.

---

## Notas sobre la entrega

- **Outputs del notebook:** deja los resultados de `display()` y `.show()` visibles en el `.ipynb`. El instructor los revisará sin ejecutar el notebook.
- **Capturas de pantalla en `.md`:** puedes incluir imágenes en tu archivo de reflexión con `![descripción](imagen.png)` — útil para mostrar el Catalog, el Spark UI o resultados de Databricks.

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
- [youtube - databricks end to end project](https://www.youtube.com/watch?v=GjbyQO44af8)