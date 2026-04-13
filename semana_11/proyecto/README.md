# Proyecto Final — Olist E-Commerce: Pipeline End-to-End con Databricks y Fabric

**Semanas:** 11-12  
**Modalidad:** Individual o parejas  
**Stack:** Databricks Enterprise + ADLS Gen2 + Microsoft Fabric  
**Dataset:** Olist Brazilian E-Commerce (Kaggle `olistbr/brazilian-ecommerce`)

---

## Objetivo

Construir un pipeline de datos completo de producción:
desde la ingesta cruda de 9 archivos CSV hasta un modelo dimensional en Gold
expuesto en Microsoft Fabric para análisis y reporting.

No hay instrucciones paso a paso como en las actividades anteriores.
Las decisiones de diseño son tuyas — el instructor evalúa el criterio técnico,
no solo que el código funcione.

---

## Dataset

**Descarga:** [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Sube los 9 archivos CSV a ADLS Gen2:
```
landing/raw/olist/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_customers_dataset.csv
├── olist_sellers_dataset.csv
├── olist_products_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

---

## Arquitectura requerida

### Capa Bronze

- Una tabla Delta por cada archivo CSV: `bronze.olist_orders`, `bronze.olist_order_items`, etc.
- Schema **explícito** para todas las tablas (no `inferSchema`)
- Columna de auditoría `_ingestion_timestamp` en todas las tablas
- Organizado como Databricks Job: una task por tabla, o una task parametrizada con YAML

### Capa Silver

Tablas limpias y tipadas. Mínimo requerido:

| Tabla Silver | Transformaciones esperadas |
|-------------|---------------------------|
| `silver.orders` | Tipos correctos en fechas, columna `delivery_delay_days` calculada, nulos documentados |
| `silver.order_items` | Precio total por línea (`price + freight_value`), join con `silver.orders` para tener fecha |
| `silver.customers` | Estado y ciudad normalizados, join con geolocation para coordenadas |
| `silver.sellers` | Mismo tratamiento que customers |
| `silver.products` | Categoría traducida (join con translation), nulos en dimensiones documentados |
| `silver.reviews` | Score numérico, fechas tipadas, texto opcional |

### Capa Gold — Modelo Dimensional (Star Schema)

El alumno diseña el modelo. Orientación:

```
fact_orders (tabla de hechos central)
├── order_id               PK
├── customer_key           FK → dim_customers
├── seller_key             FK → dim_sellers
├── product_key            FK → dim_products
├── date_key               FK → dim_date
├── order_status
├── payment_value
├── freight_value
├── delivery_delay_days
└── review_score

dim_customers
dim_sellers
dim_products
dim_date                   ← generada programáticamente (no viene del dataset)
```

**Reglas del modelo dimensional:**
- `fact_orders` no tiene columnas descriptivas — solo métricas y claves foráneas
- Las dimensiones no tienen claves naturales como PK — usar `monotonically_increasing_id()` o un hash
- `dim_date` se genera con un rango de fechas, no se extrae de los pedidos

### Componente de streaming (semana 12)

Simula la llegada de nuevos pedidos como stream:
- Toma los últimos 1.000 pedidos del dataset ordenados por `order_purchase_timestamp`
- Escríbelos en archivos CSV en la landing zone uno por uno (script de simulación)
- El pipeline Bronze los ingiere con Auto Loader en modo streaming
- El MERGE INTO Silver actualiza el estado sin duplicados

### Exposición en Microsoft Fabric

- Crear un **OneLake Shortcut** apuntando a las tablas Gold del ADLS Gen2
- Fabric lee las tablas Delta directamente, sin mover datos
- Crear al menos **un report** con las métricas de negocio definidas abajo

---

## Métricas de negocio requeridas en el report de Fabric

El report debe responder al menos estas preguntas:

1. ¿Cuál es el tiempo medio de entrega por estado de Brasil?
2. ¿Qué categorías de producto tienen mayor tasa de reseñas negativas (score ≤ 2)?
3. ¿Cuál es la evolución mensual del volumen de pedidos y el ticket medio?
4. ¿Qué vendedores tienen mayor porcentaje de pedidos con retraso en la entrega?
5. (Libre) Una métrica adicional que el alumno considere relevante para el negocio

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/proyecto-final-<tu-nombre>
```

Carpeta de entrega:
```
semana_11/proyecto/<tu-nombre>/
├── notebooks/
│   ├── 01_bronze_ingesta.ipynb
│   ├── 02_silver_transformaciones.ipynb
│   └── 03_gold_modelo_dimensional.ipynb
├── configs/
│   └── pipeline_config.yml      ← si usas el patrón metadata-driven de semana 04
├── docs/
│   ├── arquitectura.md          ← diagrama ASCII + decisiones
│   ├── modelo_dimensional.md    ← esquema del star schema con descripción de cada tabla
│   └── calidad_datos.md         ← nulos, anomalías y cómo los trataste
└── README.md                    ← guía de ejecución del pipeline
```

---

## Criterios de evaluación (semanas 11-12 combinadas)

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Bronze completo | 9 tablas con schema explícito + columna de auditoría | 10% |
| [Requerido] Silver con transformaciones justificadas | Tipos correctos, nulos documentados, `delivery_delay_days` | 15% |
| [Requerido] Gold — Star Schema correcto | fact_orders + 4 dimensiones, reglas del modelo respetadas | 20% |
| [Requerido] Report en Fabric con 4 métricas | OneLake Shortcut configurado, report publicado | 15% |
| [Requerido] Documentación técnica completa | arquitectura.md + modelo_dimensional.md + calidad_datos.md | 15% |
| [Recomendado] Componente de streaming funcionando | Auto Loader + MERGE sin duplicados en Silver | 10% |
| [Recomendado] Pipeline organizado como Job | Tasks encadenadas, parametrizable dev/prod | 10% |
| [Opcional] dim_date generada programáticamente | Atributos: año, trimestre, mes, semana, día semana | 5% |
| **Total** | | **100%** |

---

## Dependencias de entorno

| Recurso | Estado |
|---------|--------|
| Databricks Enterprise | Requerido — confirmar acceso con Inetum |
| ADLS Gen2 (landing + capas) | Requerido — mismo storage de semana 10 |
| Microsoft Fabric (workspace) | Requerido — confirmar licencia con Inetum |
| Olist dataset descargado y subido a ADLS | El alumno lo gestiona en semana 11 |
