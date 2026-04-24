# Proyecto Semana 01 — Maven Fuzzy Factory: Pipeline completo de E-Commerce

**Semana:** 01  
**Tipo:** Proyecto integrador  
**Nivel:** Junior  
**Modalidad:** Individual  
**Entorno:** Databricks Community Edition

---

## Contexto

Maven Fuzzy Factory es una tienda online que vende osos de peluche. Tiene datos reales de sesiones web, órdenes, productos, reembolsos y comportamiento de usuarios desde su lanzamiento.

Tu misión como Data Engineer: entender el modelo de datos, detectar problemas de calidad, construir el pipeline medallón completo y responder preguntas de negocio que el equipo de marketing y producto necesitan.

Los archivos están disponibles en el sitio de Teams del bootcamp (SharePoint):

**[Descargar dataset — inetum_data_engineer_bootcamp / semana_01 / Maven+Fuzzy+Factory](https://gfi1.sharepoint.com/sites/JUNIORDATAENGINEERSDEVTEAM/Documents%20partages/Forms/AllItems.aspx?id=%2Fsites%2FJUNIORDATAENGINEERSDEVTEAM%2FDocuments%20partages%2FGeneral%2Finetum%5Fdata%5Fengineer%5Fbootcamp&viewid=532715df%2D69df%2D4d0e%2D8785%2Daf7a4ccf2983)**

> Navega dentro del sitio a: `General / inetum_data_engineer_bootcamp / semana_01 / Maven+Fuzzy+Factory`

---

## El modelo de datos

El dataset tiene 6 tablas:

| Tabla | Descripción |
|-------|-------------|
| `website_sessions` | Cada visita al sitio web, con fuente de marketing y dispositivo |
| `website_pageviews` | Cada página vista dentro de una sesión |
| `orders` | Órdenes realizadas, ligadas a una sesión y usuario |
| `order_items` | Ítems individuales dentro de cada orden |
| `order_item_refunds` | Reembolsos aplicados a ítems de órdenes |
| `products` | Catálogo de productos disponibles |

---

## Parte 1 — Modelo Entidad-Relación

Antes de tocar los datos, debes entender cómo se relacionan las tablas.

Crea el archivo `modelo_er_<tu-nombre>.md` en tu carpeta de entrega con lo siguiente:

### 1.1 Diagrama en texto

Dibuja el diagrama usando texto o pseudocódigo. Ejemplo de formato:

```
website_sessions (website_session_id PK)
        │
        │ 1:N
        ▼
website_pageviews (website_session_id FK)

website_sessions (website_session_id PK)
        │
        │ 1:1
        ▼
orders (website_session_id FK)
```

Debes representar las 6 tablas con sus relaciones completas.

### 1.2 Tabla de relaciones

Completa esta tabla identificando cada relación:

| Tabla A | Tabla B | Llave de unión | Cardinalidad | Tipo de relación |
|---------|---------|----------------|-------------|------------------|
| `website_sessions` | `orders` | `website_session_id` | | |
| `orders` | `order_items` | `order_id` | | |
| `order_items` | `order_item_refunds` | `order_item_id` | | |
| `orders` | `order_item_refunds` | `order_id` | | |
| `products` | `order_items` | `product_id` | | |
| `website_sessions` | `website_pageviews` | `website_session_id` | | |

> Cardinalidad posible: `1:1`, `1:N`, `N:M`  
> Tipo de relación: identifica si es obligatoria u opcional (¿puede existir una orden sin sesión? ¿puede haber un reembolso sin orden?)

### 1.3 Llaves primarias y foráneas

| Tabla | Primary Key | Foreign Keys |
|-------|-------------|--------------|
| `website_sessions` | | |
| `website_pageviews` | | |
| `orders` | | |
| `order_items` | | |
| `order_item_refunds` | | |
| `products` | | |

Commit esperado:
```bash
git commit -m "docs: add entity relationship model for maven fuzzy factory"
```

---

## Parte 2 — Exploración y detección de problemas de calidad

Crea el notebook `exploracion_<tu-nombre>.ipynb`. Lee las 6 tablas en Bronze y documenta los problemas que encuentras.

### Qué buscar en cada tabla:

**`orders`**
- ¿El formato de `created_at` es consistente?
- ¿Hay `price_usd` negativos o nulos?
- ¿Todos los `website_session_id` existen en `website_sessions`?

**`order_items`**
- ¿Hay `order_id` que no existen en `orders`?
- ¿Los `product_id` corresponden a productos del catálogo?

**`order_item_refunds`**
- ¿Hay reembolsos sin orden asociada?
- ¿El `refund_amount_usd` supera el `price_usd` original?

**`products`**
- ¿Hay productos duplicados? (mismo nombre, distinto ID)
- ¿Hay nombres con espacios extra o mayúsculas inconsistentes?

**`website_sessions`**
- ¿Hay sesiones con `utm_source` nulo? ¿Qué significa eso?
- ¿El campo `is_repeat_session` tiene solo valores 0 y 1?

**`website_pageviews`**
- ¿Hay pageviews de sesiones que no existen?
- ¿Las URLs tienen formato consistente?

Documenta cada problema encontrado en una celda markdown con este formato:

```markdown
### Problema detectado: productos duplicados
- Tabla: products
- Descripción: existen registros con el mismo product_name pero diferente product_id
- Registros afectados: X
- Impacto: puede inflar métricas de ventas por producto
- Decisión: conservar el product_id más antiguo (menor ID)
```

Commit esperado:
```bash
git commit -m "feat: add data exploration and quality issues notebook"
```

---

## Parte 3 — Pipeline Medallón

### Bronze — Ingestar las 6 tablas sin transformar

Crea el notebook `bronze_<tu-nombre>.ipynb`.

Lee y guarda las 6 tablas como tablas Delta en su estado raw. Sin transformaciones.

```python
tablas = {
    "bronze_website_sessions": "/FileStore/website_sessions.csv",
    "bronze_website_pageviews": "/FileStore/website_pageviews.csv",
    "bronze_orders": "/FileStore/orders.csv",
    "bronze_order_items": "/FileStore/order_items.csv",
    "bronze_order_item_refunds": "/FileStore/order_item_refunds.csv",
    "bronze_products": "/FileStore/products.csv",
}

for nombre_tabla, ruta in tablas.items():
    # Bronze: inferSchema=false — preserva el dato crudo exactamente como llega
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(ruta)
    df.write.format("delta").mode("overwrite").saveAsTable(nombre_tabla)
    print(f"{nombre_tabla}: {df.count()} registros")
```

Commit esperado:
```bash
git commit -m "feat: add bronze layer for all 6 tables"
```

---

### Silver — Limpiar y estandarizar

Crea el notebook `silver_<tu-nombre>.ipynb`.

Aplica las correcciones identificadas en la Parte 2. Como mínimo:

- Estandarizar tipos de fecha en todas las tablas (`created_at` a `TimestampType`)
- Eliminar duplicados en `products`
- Resolver registros huérfanos (ítems sin orden, reembolsos sin ítem)
- Estandarizar `is_repeat_session` como booleano o entero limpio
- Normalizar URLs en `website_pageviews` (todo lowercase, sin espacios)
- Estandarizar `utm_source` y `utm_campaign` (lowercase)

> **Regla:** Silver lee de Bronze. Nunca del archivo original.

Cada transformación debe ir acompañada de un comentario que explique el porqué.

```python
# Eliminar productos duplicados conservando el product_id más bajo
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_name").orderBy("product_id")
df_products_silver = df_products_bronze \
    .withColumn("rn", F.row_number().over(w)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")
```

Commit esperado:
```bash
git commit -m "feat: add silver layer with cleaning transformations"
```

---

### Gold — Tablas listas para consumo de negocio

Crea el notebook `gold_<tu-nombre>.ipynb`.

Construye las siguientes tablas Gold usando JOINs entre Silver:

**Tabla 1: `gold_session_orders`** — base analítica plana

```sql
SELECT
    s.website_session_id,
    s.created_at AS session_date,
    s.utm_source,
    s.utm_campaign,
    s.device_type,
    s.is_repeat_session,
    o.order_id,
    o.price_usd AS order_revenue,
    o.cogs_usd,
    o.price_usd - o.cogs_usd AS margin_usd,
    CASE WHEN o.order_id IS NOT NULL THEN 1 ELSE 0 END AS converted
FROM silver_website_sessions s
LEFT JOIN silver_orders o ON s.website_session_id = o.website_session_id
```

**Tabla 2: `gold_channel_performance`** — rendimiento por canal de marketing

Responde: ¿qué canal genera más órdenes y mejor margen?

```sql
SELECT
    utm_source,
    utm_campaign,
    device_type,
    COUNT(DISTINCT website_session_id) AS total_sessions,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(COUNT(DISTINCT order_id) * 100.0 / COUNT(DISTINCT website_session_id), 2) AS conversion_rate_pct,
    ROUND(SUM(order_revenue), 2) AS total_revenue,
    ROUND(AVG(order_revenue), 2) AS avg_order_value
FROM gold_session_orders
GROUP BY utm_source, utm_campaign, device_type
ORDER BY total_revenue DESC
```

**Tabla 3: `gold_product_performance`** — rendimiento por producto

Responde: ¿cuál es el producto más vendido y más rentable?

```sql
SELECT
    p.product_name,
    COUNT(oi.order_item_id) AS total_units_sold,
    ROUND(SUM(oi.price_usd), 2) AS total_revenue,
    ROUND(SUM(oi.price_usd - oi.cogs_usd), 2) AS total_margin,
    COUNT(DISTINCT r.order_item_refund_id) AS total_refunds,
    ROUND(COUNT(DISTINCT r.order_item_refund_id) * 100.0 / COUNT(oi.order_item_id), 2) AS refund_rate_pct
FROM silver_order_items oi
LEFT JOIN silver_products p ON oi.product_id = p.product_id
LEFT JOIN silver_order_item_refunds r ON oi.order_item_id = r.order_item_id
GROUP BY p.product_name
ORDER BY total_revenue DESC
```

**Tabla 4: `gold_pageview_funnel`** — funnel de conversión por URL

Responde: ¿en qué página se pierden más usuarios?

```sql
SELECT
    pageview_url,
    COUNT(DISTINCT website_session_id) AS sessions_reached,
    ROUND(COUNT(DISTINCT website_session_id) * 100.0 / MAX(COUNT(DISTINCT website_session_id)) OVER (), 2) AS pct_of_total
FROM silver_website_pageviews
GROUP BY pageview_url
ORDER BY sessions_reached DESC
```

Commit esperado:
```bash
git commit -m "feat: add gold layer with 4 business analysis tables"
```

---

## Parte 4 — Tabla desnormalizada (Analytical Wide Table)

En el mismo notebook Gold o en uno separado `wide_table_<tu-nombre>.ipynb`, crea una única tabla plana que consolide los campos más relevantes para un analista que nunca quiere hacer JOINs.

Debe incluir al menos una columna de cada tabla original. Esta es la tabla que consumiría un dashboard de Power BI o Tableau.

Reflexiona en una celda markdown:
- ¿Qué se gana al desnormalizar?
- ¿Qué se pierde?
- ¿Cuándo tiene sentido tener esta tabla en producción?

Commit esperado:
```bash
git commit -m "feat: add denormalized wide table for analytics"
```

---

## Parte 5 — Responder las preguntas de negocio

En el archivo `analisis_<tu-nombre>.md`, responde con los resultados reales de tus tablas Gold:

1. ¿Cuál es la tasa de conversión general (sesiones → órdenes)?
2. ¿Qué canal de marketing tiene la tasa de conversión más alta?
3. ¿Qué canal genera más revenue total?
4. ¿Cuál es el producto más vendido? ¿Y el más rentable?
5. ¿Qué producto tiene la tasa de reembolso más alta?
6. ¿Qué URL del funnel pierde más usuarios?
7. ¿El dispositivo móvil convierte mejor o peor que desktop?
8. Una recomendación concreta para el equipo de marketing basada en tus datos.

Commit esperado:
```bash
git commit -m "docs: add business analysis with findings and recommendations"
```

---

## Estructura de entrega

```
semana_01/proyecto/<tu-nombre>/
├── modelo_er_<tu-nombre>.md
├── exploracion_<tu-nombre>.ipynb
├── bronze_<tu-nombre>.ipynb
├── silver_<tu-nombre>.ipynb
├── gold_<tu-nombre>.ipynb
├── wide_table_<tu-nombre>.ipynb     ← opcional pero valorado
└── analisis_<tu-nombre>.md
```

---

## Entrega en Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana01-proyecto-<tu-nombre>

# ... trabajar y commitear por partes ...

git push origin feature/semana01-proyecto-<tu-nombre>
```

PR hacia `develop` con título:

```
[Semana 01] Proyecto — Maven Fuzzy Factory — <Tu Nombre>
```

En la descripción del PR incluye:
- Tasa de conversión que encontraste
- Canal de marketing más efectivo según tus datos
- El mayor problema de calidad que encontraste y cómo lo resolviste
- Una decisión de diseño que tomaste y por qué

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Modelo ER completo | Diagrama + tabla de relaciones + llaves | 15% |
| Exploración documentada | Problemas detectados con impacto y decisión | 15% |
| Bronze correcto | 6 tablas ingestadas sin transformar | 10% |
| Silver con calidad | Transformaciones reales y justificadas | 20% |
| Gold funcional | 4 tablas que responden las preguntas | 25% |
| Análisis de negocio | Respuestas con datos reales, no inventados | 10% |
| Git (mínimo 6 commits, PR completo) | Commits atómicos y descripción del PR | 5% |

---

## Lo que NO se debe hacer

- No hacer todo en un solo notebook
- No inventar los números del análisis — deben salir de las tablas Gold
- No omitir el modelo ER — el diseño va antes que el código
- No hacer push directo a `develop`
- No subir los archivos de datos al repositorio

---

## Referencias

- [GITFLOW.md](../../GITFLOW.md)
- [Actividad 03](../actividades/actividad_03/README.md) — lectura de formatos
- [Actividad 04](../actividades/actividad_04/README.md) — arquitectura medallón
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
