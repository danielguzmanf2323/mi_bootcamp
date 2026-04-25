# Diseño Arquitectura Medallón - Customers

**Autor:** Daniel Guzmán  
**Actividad:** Actividad 04 — Arquitectura Medallón con el dataset Customers  
**Entorno:** Databricks Free Edition  
**Dataset:** customers con 2 millones de registros  

---

## Objetivo

Diseñar e implementar una arquitectura medallón para el dataset `customers`, separando el flujo de datos en tres capas:

- **Bronze:** datos crudos, guardados tal como llegan.
- **Silver:** datos limpios, estandarizados y confiables.
- **Gold:** datos agregados y listos para análisis de negocio.

---

## Bronze — bronze_customers

### Fuente

- Archivo fuente: `customers-2000000.csv`
- Formato: CSV
- Ubicación esperada: Volume de Databricks
- Ruta estimada: `/Volumes/workspace/default/customers_activity_04_daniel/customers-2000000.csv`

### Columnas esperadas tal como llegan

Según el dataset trabajado previamente, las columnas esperadas son:

- `Index`
- `Customer Id`
- `First Name`
- `Last Name`
- `Company`
- `City`
- `Country`
- `Phone 1`
- `Phone 2`
- `Email`
- `Subscription Date`
- `Website`

### Qué NO se hará en Bronze

En la capa Bronze no se realizará ninguna transformación.

No se hará:

- Renombramiento de columnas.
- Eliminación de nulos.
- Eliminación de duplicados.
- Cambio de tipos de datos.
- Estandarización de texto.
- Eliminación de columnas.

La razón es que Bronze debe conservar el dato crudo tal como llegó desde la fuente. Esta capa funciona como fuente histórica y punto de recuperación si una transformación posterior falla.

---

## Silver — silver_customers

### Objetivo

Crear una versión limpia y estandarizada del dataset, leyendo siempre desde `bronze_customers` y no desde el archivo original.

### Limpieza de columnas

Las columnas serán renombradas a formato `snake_case`:

- `Index` → `index`
- `Customer Id` → `customer_id`
- `First Name` → `first_name`
- `Last Name` → `last_name`
- `Company` → `company`
- `City` → `city`
- `Country` → `country`
- `Phone 1` → `phone_1`
- `Phone 2` → `phone_2`
- `Email` → `email`
- `Subscription Date` → `subscription_date`
- `Website` → `website`

### Tratamiento de nulos

Se revisarán columnas clave como:

- `customer_id`
- `country`
- `city`
- `email`

Decisiones:

- Si `customer_id` es nulo, se eliminará el registro porque no se puede identificar de forma confiable al cliente.
- Si `country` es nulo o vacío, se eliminará el registro porque la tabla Gold depende de agrupar clientes por país.
- Si `city` es nulo o vacío, se reemplazará por `unknown`, ya que la ciudad no es la clave principal del análisis.
- Si `email` es nulo, se conservará el registro porque no afecta directamente la tabla Gold.

### Duplicados

Los duplicados se detectarán usando la columna `customer_id`.

Decisión:

- Se eliminarán duplicados con `dropDuplicates(["customer_id"])`.

Esto evita contar varias veces al mismo cliente en la capa Gold.

### Estandarización de valores

Se aplicarán transformaciones de limpieza:

- Convertir nombres de columnas a `snake_case`.
- Aplicar `trim()` a columnas de texto.
- Estandarizar `country` en mayúsculas para evitar valores duplicados por diferencias de escritura.
- Convertir `subscription_date` a tipo fecha si es posible.

### Columnas que podrían no aportar valor

La columna `index` será descartada en Silver porque parece ser un índice técnico del archivo y no un atributo real del cliente.

Las columnas `phone_1`, `phone_2`, `website` y `subscription_date` se conservarán porque podrían servir para análisis futuros, aunque no se usen directamente en la tabla Gold principal.

---

## Gold — gold_customers_by_country

### Pregunta de negocio

¿Qué países tienen mayor cantidad de clientes registrados?

### Fuente

La tabla Gold se construirá leyendo desde:

- `silver_customers`

No se leerá directamente desde el archivo original ni desde Bronze.

### Transformación

Se agruparán los clientes por país y se calculará:

- Total de clientes por país.
- Ranking de países según cantidad de clientes.

### Columnas de la tabla Gold

La tabla `gold_customers_by_country` tendrá:

- `country`
- `total_customers`
- `ranking`

### Resultado esperado

Esta tabla permitirá identificar los países con mayor concentración de clientes y servirá como insumo para reportes, tableros o análisis de negocio.

---

## Flujo de la arquitectura

```text
customers-2000000.csv
        |
        v
bronze_customers
Dato crudo, sin transformar
        |
        v
silver_customers
Dato limpio, estandarizado y sin duplicados
        |
        v
gold_customers_by_country
Clientes agregados por país con ranking


## Decisiones principales

- Bronze se conserva intacto para mantener trazabilidad del dato original.
- Silver concentra la limpieza y estandarización.
- Gold responde una pregunta de negocio concreta.
- Cada capa lee desde la capa anterior.
- No se modifica Bronze después de haber sido creado.