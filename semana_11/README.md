# Semana 11 — Proyecto Final: Diseño, Arquitectura y Desarrollo (Olist)

**Modalidad:** Individual o parejas (a criterio del instructor)  
**Dataset:** Olist Brazilian E-Commerce — Kaggle `olistbr/brazilian-ecommerce`  
**Stack:** Databricks Enterprise + ADLS Gen2 + Microsoft Fabric  
**Entorno requerido:** Databricks Enterprise + Microsoft Fabric (coordinar acceso con Inetum)

---

> **Convención obligatoria — entorno compartido**
>
> Todos los estudiantes del proyecto final trabajan sobre el mismo catálogo `default` en Databricks Enterprise.
> Para no sobrescribir las tablas de otras personas, **sufija siempre tu nombre en cada tabla que crees**:
>
> ```
> default.bronze.olist_orders_<tu_nombre>
> default.silver.olist_orders_<tu_nombre>
> default.gold.ventas_por_categoria_<tu_nombre>
> ```
>
> Esto aplica a las 9 tablas Bronze, todas las tablas Silver y todas las tablas Gold del proyecto.
> Si usas `mode("overwrite")` sin el sufijo, sobrescribirás el trabajo de otro estudiante.

---

## Contexto del proyecto final

El proyecto final integra todo el stack del bootcamp en un pipeline real end-to-end:

```
ADLS Gen2 (landing)
    │  Auto Loader (streaming o batch)
    ▼
Delta Lake Bronze (Databricks)
    │  PySpark + MERGE + calidad de datos
    ▼
Delta Lake Silver (Databricks)
    │  Modelado dimensional (star schema)
    ▼
Delta Lake Gold (Databricks)
    │  OneLake Shortcut
    ▼
Microsoft Fabric (análisis y reporting)
```

La diferencia con los proyectos anteriores: aquí el alumno toma **todas** las decisiones
de diseño — no hay arquitectura predefinida. El pipeline, el modelo dimensional,
las métricas de la capa Gold y el report de Fabric son decisiones propias.

---

## Dataset: Olist Brazilian E-Commerce

**Tamaño:** ~130 MB comprimido, ~450 MB descomprimido  
**Formato:** 9 archivos CSV

> ⚠️ **Nota:** la versión del dataset que se usa en el bootcamp es una copia con errores introducidos intencionalmente para que los debuguees durante el proyecto. No descargues el dataset directamente de Kaggle — usa la versión oficial del bootcamp en SharePoint.

**Descarga desde el sitio de Teams del bootcamp (SharePoint):**

**[inetum_data_engineer_bootcamp / semana_11 / Brazilian E-Commerce](https://gfi1.sharepoint.com/sites/JUNIORDATAENGINEERSDEVTEAM/Documents%20partages/Forms/AllItems.aspx?id=%2Fsites%2FJUNIORDATAENGINEERSDEVTEAM%2FDocuments%20partages%2FGeneral%2Finetum%5Fdata%5Fengineer%5Fbootcamp&viewid=532715df%2D69df%2D4d0e%2D8785%2Daf7a4ccf2983)**

> Navega dentro del sitio a: `General / inetum_data_engineer_bootcamp / semana_11 / Brazilian E-Commerce`

| Archivo | Descripción | Filas aprox. |
|---------|-------------|-------------|
| `olist_orders_dataset.csv` | Pedidos — tabla central | 99.441 |
| `olist_order_items_dataset.csv` | Líneas de pedido (producto, precio, flete) | 112.650 |
| `olist_order_payments_dataset.csv` | Pagos (puede haber varios por pedido) | 103.886 |
| `olist_order_reviews_dataset.csv` | Reseñas de clientes | 99.224 |
| `olist_customers_dataset.csv` | Clientes | 99.441 |
| `olist_sellers_dataset.csv` | Vendedores | 3.095 |
| `olist_products_dataset.csv` | Productos | 32.951 |
| `olist_geolocation_dataset.csv` | Coordenadas por código postal | 1.000.163 |
| `product_category_name_translation.csv` | Categorías PT → EN | 71 |

Una vez descargado, sube los CSV a:
```
ADLS Gen2: landing/raw/olist/
```

---

## Contenido de la semana

| Carpeta | Descripción |
|---------|-------------|
| [proyecto/](proyecto/) | Instrucciones completas del proyecto final |
| [documentos/](documentos/) | Material de referencia: modelado dimensional, OneLake Shortcuts |

---

## Objetivo de la semana 11

Al terminar la semana 11, debes tener:
- Los 9 archivos en ADLS Gen2
- Bronze completo (9 tablas Delta en `bronze.*`)
- Silver completo (tablas limpias, tipadas, con joins preparados)
- Diseño del modelo dimensional documentado (star schema en papel/diagrama antes de codificarlo)

→ [Ver instrucciones completas del proyecto](proyecto/README.md)
