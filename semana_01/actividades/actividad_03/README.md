# Actividad 03 — Formatos de archivos y primer contacto con Databricks

**Semana:** 01  
**Tema:** Formatos de datos + Databricks + SQL  
**Nivel:** Junior  
**Modalidad:** Individual  

---

## Objetivo

Leer cuatro archivos del mismo dataset en distintos formatos (CSV, JSON, Parquet, Avro), perfilar los datos, comparar rendimiento entre formatos y practicar SQL — todo desde un entorno cloud (Databricks Community Edition o Google Colab), conectado al repositorio del bootcamp.

---

## Dataset

Los archivos están disponibles en Google Drive:

**[Descargar archivos — data_engineering_files/semana_01_actividad_03/customers_files](https://drive.google.com/drive/folders/1NPcvkwEyU5t9euXay3Uzxo02LqY_Ptb9)**

Encontrarás 4 archivos, todos con el mismo dataset de clientes:

| Archivo | Formato |
|---------|---------|
| `customers.csv` | Texto plano separado por comas |
| `customers.json` | JavaScript Object Notation |
| `customers.parquet` | Columnar binario (Apache Parquet) |
| `customers.avro` | Binario con schema embebido (Apache Avro) |

---

## Paso 0 — Configurar el entorno

### Opción A: Databricks Community Edition (recomendado)

1. Crea una cuenta gratuita en [https://community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Una vez dentro, ve a **Workspace** y verifica que puedes usar un cluster serverless
3. Para conectar el repositorio del bootcamp:
   - Ve a **Workspace** en el panel lateral
   - Clic en **create** y **git folder**
   - Ingresa la URL: `https://github.com/jobrrerac/inetum_data_engineer_bootcamp.git`
   - Databricks clonará el repositorio directamente

   > **Pista:** En Databricks puedes hacer `git pull` y cambiar de rama desde la interfaz de Repos, sin necesidad de terminal.

4. Sube los 4 archivos de datos a un **Volume** en Databricks:
   - Ve a **Catalog** → **Create Volume**
   - Sube los 4 archivos desde tu máquina
   - La ruta para leerlos será algo como: `/dbfs/FileStore/customers.csv`

### Opción B: Google Colab

1. Ve a [https://colab.research.google.com](https://colab.research.google.com)
2. Sube los archivos directamente al entorno de Colab o léelos desde Google Drive montando la unidad:
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```
3. Para subir tu trabajo al repo, usa la terminal integrada de Colab:
   ```bash
   !git clone https://github.com/jobrrerac/inetum_data_engineer_bootcamp.git
   ```

---

## Paso 1 — Crear tu rama y carpeta de trabajo

En tu entorno local (o desde la terminal de Colab/Databricks):

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana01-formatos-<tu-nombre>
```

Dentro de `semana_01/actividades/actividad_03/`, crea una **carpeta con tu nombre**:

```
semana_01/actividades/actividad_03/<tu-nombre>/
```

Ahí crearás tus 4 notebooks, uno por formato:

```
customers_csv_<tu-nombre>.ipynb
customers_json_<tu-nombre>.ipynb
customers_parquet_<tu-nombre>.ipynb
customers_avro_<tu-nombre>.ipynb
```

---

## Paso 2 — Estructura de cada notebook

Cada notebook debe seguir esta estructura de celdas. A continuación se describe lo que debe contener cada sección.

---

### Celda 1 — Título y contexto

```markdown
# Análisis de customers.<extensión>
**Autor:** <Tu Nombre>
**Fecha:** <fecha>
**Entorno:** Databricks / Google Colab
```

---

### Celda 2 — Librerías y configuración

Importa las librerías necesarias. Investiga qué librería se usa para cada formato.

> **Pistas:**
> - CSV y JSON: `pandas` lo maneja nativamente
> - Parquet: `pandas` con `pyarrow` o `fastparquet`
> - Avro: investiga la librería `fastavro`
> - PySpark puede leer los 4 formatos con `spark.read`

```python
import pandas as pd
import time
# ... más imports según el formato
```

---

### Celda 3 — Lectura del archivo y tiempo de carga

Mide el tiempo que tarda en leer el archivo. Esto es importante para comparar formatos.

```python
import time

inicio = time.time()
df = pd.read_csv("/ruta/al/archivo/customers.csv")  # cambia según formato
fin = time.time()

print(f"Tiempo de lectura: {fin - inicio:.4f} segundos")
print(f"Registros: {len(df)}")
print(f"Columnas: {list(df.columns)}")
```

> **Pista para Avro:** `fastavro` devuelve un iterador de registros, necesitarás convertirlo a lista antes de crear el DataFrame.

---

### Celda 4 — Vista previa e inspección inicial

```python
df.head(5)
df.dtypes
df.shape
df.info()
```

---

### Celda 5 — Perfilamiento de datos

Analiza la calidad y distribución de los datos:

```python
# Valores nulos por columna
df.isnull().sum()

# Valores únicos por columna
df.nunique()

# Estadísticas descriptivas
df.describe(include='all')
```

---

### Celda 6 — Análisis y agrupaciones

Responde las siguientes preguntas usando código. Puedes usar pandas o SQL (ver Paso 3):

**Preguntas a responder:**

1. ¿Cuántos clientes hay por país? Muestra el resultado ordenado de mayor a menor.
2. ¿Cuál es el **Top 5** de países con más clientes?
3. ¿Cuántas empresas (`company`) distintas aparecen en el dataset?
4. ¿Cuál es el nombre (`contact_name` o similar) más frecuente?
5. ¿Hay clientes sin ciudad registrada? ¿Cuántos?

```python
# Ejemplo de agrupación
clientes_por_pais = df.groupby("country")["customer_id"].count().reset_index()
clientes_por_pais.columns = ["country", "total_clientes"]
clientes_por_pais.sort_values("total_clientes", ascending=False).head(5)
```

---

### Celda 7 — Reflexión sobre el formato

Al final de cada notebook, agrega una celda de texto (markdown) respondiendo:

```markdown
## Reflexión sobre el formato .<extensión>

- Tiempo de lectura registrado: X.XX segundos
- Tamaño del archivo: X MB
- ¿Fue fácil o difícil de leer con Python?
- ¿Qué ventajas o desventajas noté?
- ¿En qué situación usaría este formato en un pipeline real?
```

---

## Paso 3 — Investigación: JOINs en PySpark

Antes de escribir el notebook de SQL, investiga cómo funcionan los JOINs en PySpark. Los necesitarás en el proyecto de la semana. Documenta en una celda markdown de tu notebook SQL lo que encontraste:

- ¿Qué tipos de JOIN existen en PySpark? (`inner`, `left`, `right`, `full`)
- ¿Cuál es la sintaxis básica?
- ¿En qué se diferencia de SQL tradicional?

Ejemplo de referencia:
```python
# JOIN entre dos DataFrames en PySpark
df_resultado = df_a.join(df_b, df_a["id"] == df_b["id"], how="left")
```

> Esta investigación no se evalúa con código — solo documenta lo que aprendiste. Lo pondrás en práctica en el proyecto.

---

## Paso 4 — Práctica SQL

En un quinto notebook llamado `sql_customers_<tu-nombre>.ipynb`, practica SQL sobre el dataset de clientes usando **PySpark SQL**.

Registra el DataFrame como tabla temporal y usa `spark.sql()` directamente:

```python
# Leer el archivo (usa el formato que prefieras)
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/customers.csv")

# Registrar como vista temporal para usar SQL
df.createOrReplaceTempView("customers")

# Ejemplo de consulta
result = spark.sql("""
    SELECT country, COUNT(*) as total
    FROM customers
    GROUP BY country
    ORDER BY total DESC
    LIMIT 5
""")
result.show()
```

> **Nota:** En Databricks, `spark` ya está disponible sin necesidad de crear una sesión. Si usas Google Colab, necesitas crear la sesión: `from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate()`

### Consultas a desarrollar

Escribe una query SQL para cada uno de estos ejercicios:

| # | Ejercicio | Cláusula principal |
|---|-----------|-------------------|
| 1 | Listar todos los clientes de México | `WHERE` |
| 2 | Contar clientes por país | `GROUP BY` |
| 3 | Mostrar solo países con más de 5 clientes | `HAVING` |
| 4 | Top 5 países con más clientes | `ORDER BY` + `LIMIT` |
| 5 | Nombre más frecuente en el dataset | `GROUP BY` + `ORDER BY` |
| 6 | Ranking de clientes por país usando ventana | `WINDOW` / `ROW_NUMBER()` |
| 7 | Actualizar el país de un cliente específico | `UPDATE` en Delta: `spark.sql("UPDATE customers SET country = ... WHERE ...")` |
| 8 | Eliminar registros donde la ciudad es nula | `DELETE` en Delta: `spark.sql("DELETE FROM customers WHERE city IS NULL")` |

> **Pista para window functions:**
> ```sql
> SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY customer_id) as ranking
> FROM customers
> ```

---

## Paso 4 — Commits y entrega

Haz commits progresivos, **uno por notebook completado**:

```bash
git add semana_01/actividades/actividad_03/<tu-nombre>/
git commit -m "feat: add customers CSV notebook - <tu-nombre>"
git commit -m "feat: add customers JSON notebook - <tu-nombre>"
git commit -m "feat: add customers Parquet notebook - <tu-nombre>"
git commit -m "feat: add customers Avro notebook - <tu-nombre>"
git commit -m "feat: add SQL practice notebook - <tu-nombre>"

git push origin feature/semana01-formatos-<tu-nombre>
```

Abre un Pull Request hacia `develop` con el título:

```
[Semana 01] Formatos y SQL — <Tu Nombre>
```

En la descripción del PR incluye:
- ¿Qué formato fue más rápido de leer?
- ¿Cuál fue el mayor desafío?
- Una conclusión sobre cuándo usarías cada formato

---

## Entregables esperados

```
semana_01/actividades/actividad_03/<tu-nombre>/
├── customers_csv_<tu-nombre>.ipynb
├── customers_json_<tu-nombre>.ipynb
├── customers_parquet_<tu-nombre>.ipynb
├── customers_avro_<tu-nombre>.ipynb
└── sql_customers_<tu-nombre>.ipynb
```

Cada notebook debe poder ejecutarse de principio a fin sin errores.

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Rama y carpeta con formato correcto | `feature/semana01-formatos-<nombre>` + carpeta `<nombre>/` | 10% |
| 4 notebooks de formatos completos | Lectura, perfil, agrupaciones y reflexión | 40% |
| Comparación de tiempos de lectura | Presente en cada notebook | 10% |
| Notebook de SQL | Las 8 consultas resueltas y ejecutadas | 30% |
| Commits descriptivos (mínimo 5) | Un commit por notebook | 10% |

---

## Lo que NO se debe hacer

- No subir los archivos de datos al repositorio (están en el `.gitignore`)
- No hacer un solo commit con los 5 notebooks
- No copiar código sin entenderlo — se evaluará en clase
- No omitir la celda de reflexión en cada notebook

---

## Referencias

- [GITFLOW.md](../../GITFLOW.md) — flujo de trabajo Git del bootcamp
- [Actividad 01](../actividad_01/README.md) — repaso de Git
- [Documentación de pandas](https://pandas.pydata.org/docs/)
- [fastavro — lectura de Avro](https://fastavro.readthedocs.io/)
- [PySpark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Databricks Community Edition](https://community.cloud.databricks.com)
