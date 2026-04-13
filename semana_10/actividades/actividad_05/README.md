# Actividad 05 — Semana 10: Modelado Dimensional (Kimball)

**Semana:** 10  
**Tema:** Diseño de modelos dimensionales — star schema, hechos, dimensiones y grain  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Papel + Databricks (solo para validar el diseño, no para implementar)

> Esta actividad es mayoritariamente teórica y de diseño. No hay pipeline que construir.
> El entregable es un modelo dimensional bien diseñado sobre el dataset Olist —
> el mismo que implementarás en código en las semanas 11 y 12.

---

## Objetivo

Entender los principios del modelado dimensional de Kimball y aplicarlos
al diseño de un Data Warehouse real. Al terminar, tendrás el diseño del star schema
del proyecto final listo antes de escribir una sola línea de código — porque
en producción, diseñar antes de implementar no es opcional.

---

## Material de estudio previo

1. ¿Qué diferencia hay entre un modelo **normalizado (3FN)** y un modelo **dimensional (star schema)**? ¿Cuándo usarías cada uno?
2. ¿Qué es el **grain** de una tabla de hechos y por qué definirlo es lo primero que se hace?
3. ¿Qué diferencia hay entre una **medida aditiva**, **semiaditiva** y **no aditiva**? Pon un ejemplo de cada una.
4. ¿Qué es una **surrogate key** y por qué las dimensiones no deben usar la clave natural del sistema fuente como PK?
5. ¿Qué es **SCD Type 1** y **SCD Type 2**? ¿Cuándo usarías cada uno?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-modelado-dimensional-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_10/actividades/actividad_05/<tu-nombre>/
modelo_dimensional_<tu-nombre>.md
```

---

## Conceptos clave antes de diseñar

### Tabla de hechos

Registra eventos del negocio que ocurren en el tiempo. Tiene:
- **Grain:** el nivel de detalle de cada fila. Se define primero, antes de cualquier otra decisión.
- **Medidas:** columnas numéricas que se agregan (sumas, promedios, conteos)
- **Claves foráneas:** apuntan a cada dimensión

### Dimensiones

Describen el contexto de los hechos. Tienen:
- **Surrogate key:** PK generada (no la del sistema fuente)
- **Natural key:** la clave del sistema fuente (se guarda, pero no es la PK)
- **Atributos:** columnas descriptivas usadas para filtrar, agrupar y etiquetar

### Medidas según su comportamiento

| Tipo | Descripción | Ejemplo |
|------|-------------|---------|
| Aditiva | Se puede sumar en todas las dimensiones | `payment_value` |
| Semiaditiva | Se puede sumar en algunas dimensiones, no en todas | saldo de cuenta (no tiene sentido sumar por tiempo) |
| No aditiva | No se puede sumar en ninguna dimensión | porcentajes, ratios |

### dim_date: por qué no se extrae del dataset

La dimensión fecha se **genera programáticamente** con todos los días del rango del negocio.
No se extrae de las transacciones porque:
- Los datos pueden tener huecos (días sin pedidos)
- Necesitas poder filtrar por atributos como "es fin de semana" o "es festivo"
- El modelo debe poder responder preguntas sobre períodos sin ventas

---

## Parte 1 — Explorar el dataset Olist y definir el grain

Antes de diseñar, entiende qué representan los datos.
Lee el README de Kaggle del dataset y analiza las tablas:

```
olist_orders             → un pedido por fila
olist_order_items        → un producto por pedido por fila (un pedido puede tener varios)
olist_order_payments     → un pago por pedido por fila (un pedido puede tener varios métodos)
olist_order_reviews      → una reseña por pedido
olist_customers          → un cliente por fila
olist_sellers            → un vendedor por fila
olist_products           → un producto por fila
olist_geolocation        → coordenadas por código postal (puede tener múltiples filas por zip)
product_category_name_translation → traducción de categorías
```

Responde en tu documento:

**Pregunta 1 — Grain:**
¿Cuál es el grain más granular que puedes definir para la tabla de hechos principal?
Opciones a evaluar:
- a) Un pedido por fila
- b) Una línea de pedido (producto) por fila
- c) Un pago por fila

Para cada opción, documenta: ¿qué se pierde y qué se gana en granularidad?
¿Cuál elegirías y por qué?

**Pregunta 2 — Medidas:**
Una vez definido el grain, ¿qué medidas incluirías en la tabla de hechos?
Para cada medida, clasifícala como aditiva, semiaditiva o no aditiva:

| Medida | Origen | Tipo | ¿Por qué? |
|--------|--------|------|-----------|
| `price` | order_items | | |
| `freight_value` | order_items | | |
| `payment_value` | order_payments | | |
| `review_score` | order_reviews | | |
| `delivery_delay_days` (calculada) | orders | | |

Commit esperado:
```bash
git commit -m "docs: define grain and classify measures for Olist fact table"
```

---

## Parte 2 — Diseñar las dimensiones

Para cada dimensión, define:
- Qué tabla(s) del dataset la originan
- La surrogate key (nombre de la columna)
- La natural key (la clave del sistema fuente)
- Los atributos que incluirías

Completa esta plantilla para cada dimensión:

---

**dim_customers**

| Campo | Origen | Tipo | Notas |
|-------|--------|------|-------|
| `customer_key` | generada | surrogate key | `monotonically_increasing_id()` o MD5 de `customer_id` |
| `customer_id` | olist_customers | natural key | la del sistema Olist |
| `customer_city` | olist_customers | atributo | |
| `customer_state` | olist_customers | atributo | |
| `geolocation_lat` | olist_geolocation | atributo | ¿cómo resuelves los múltiples registros por zip? |
| `geolocation_lng` | olist_geolocation | atributo | |

---

Diseña de la misma forma:
- `dim_sellers`
- `dim_products` (incluyendo la traducción de categoría)
- `dim_date` (generada programáticamente — qué atributos incluirías)

Para `dim_date`, justifica cuántos años de rango generas y por qué.

Commit esperado:
```bash
git commit -m "docs: design all dimensions with attributes and surrogate keys"
```

---

## Parte 3 — Diagrama del star schema

Dibuja el diagrama completo en texto (ASCII art) en tu documento.
El diagrama debe mostrar:
- La tabla de hechos en el centro con todas sus medidas y FKs
- Cada dimensión alrededor con sus atributos principales

Ejemplo de formato:

```
                    dim_date
                   ──────────
                   date_key PK
                   date
                   year
                   quarter
                   month
                   week
                   day_of_week
                   is_weekend
                        │
                        │ date_key
                        │
dim_customers      fact_orders          dim_products
─────────────  ────────────────────  ─────────────────
customer_key──┤ customer_key    FK  │  product_key
customer_id   │ seller_key      FK  ├──product_key PK
customer_city │ product_key     FK  │  product_id
customer_state│ date_key        FK  │  category_en
              │ payment_value       │  ...
              │ freight_value       │
              │ delivery_delay_days │
              │ review_score        │
              └──────────┬──────────┘
                         │ seller_key
                    dim_sellers
                   ──────────────
                   seller_key PK
                   seller_id
                   seller_city
                   seller_state
```

Commit esperado:
```bash
git commit -m "docs: complete star schema diagram for Olist"
```

---

## Parte 4 — Decisiones de diseño

Responde estas preguntas en tu documento. No hay respuesta única correcta
— se evalúa la solidez del razonamiento:

**1. `olist_order_payments` tiene múltiples filas por pedido** (un pedido puede pagarse con tarjeta + vale). ¿Cómo lo tratas en el modelo? ¿Agrupas los pagos en la fact table o creas una fact table separada?

**2. `olist_geolocation` tiene múltiples coordenadas por código postal** (densidad de puntos). ¿Cómo sacas una sola coordenada por cliente/vendedor?

**3. ¿Aplicarías SCD Type 2 a alguna dimensión?** Si un cliente cambia de ciudad, ¿quieres conservar la ciudad original del pedido o actualizar? Justifica tu decisión para `dim_customers`.

**4. `review_score` es de 1 a 5.** ¿Es una medida de la fact table o un atributo de una dimensión? ¿Por qué?

Commit esperado:
```bash
git commit -m "docs: document dimensional model design decisions"
```

---

## Entrega en Git

```bash
git push origin feature/semana10-modelado-dimensional-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] Modelado Dimensional Olist — <Tu Nombre>
```

> **Este documento es el input para el proyecto de semanas 11-12.**
> Implementarás en código exactamente el modelo que diseñas aquí.
> Si el diseño está mal, el código lo estará también — invierte tiempo en esta actividad.

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Grain definido con justificación | No solo la elección — el razonamiento | 20% |
| [Requerido] Medidas clasificadas correctamente | Aditiva / semiaditiva / no aditiva con explicación | 15% |
| [Requerido] 4 dimensiones diseñadas con surrogate key | Atributos relevantes incluidos | 25% |
| [Requerido] Diagrama del star schema completo | Fact + 4 dimensiones con relaciones | 20% |
| [Requerido] 4 decisiones de diseño respondidas | Razonamiento técnico, no respuestas genéricas | 20% |
| **Total** | | **100%** |

---

## Referencias

- Kimball, R. & Ross, M. — *The Data Warehouse Toolkit* (la referencia canónica de modelado dimensional)
- [Dimensional Modeling — Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Star Schema vs Snowflake Schema](https://www.databricks.com/glossary/star-schema)
