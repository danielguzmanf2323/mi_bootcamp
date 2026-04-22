# Conceptos de Data Engineering — Daniel

## 1. ETL vs ELT
 ETL — Extract, Transform, Load
**¿Qué significa cada letra?**  
- **E**xtract: Extraer los datos de las fuentes de datos.
- **T**ransform: Transformar los datos, limpiarlos, enriquecelos, etc.
- **L**oad: Cargar los datos transformados en el sistema de almacenamiento final.

**¿En qué orden ocurren los pasos?**  
Los pasos ocurren en el siguiente orden: Extraer → Transformar → Cargar.

**¿Dónde se hace la transformación?**  
La transformación ocurre **fuera** del sistema de almacenamiento (es decir, antes de cargar los datos).

**Ejemplo:**  
Una empresa quiere mover datos de ventas desde un sistema Oracle hacia un Data Warehouse. El proceso sería:

1. **Extraer** los datos de Oracle.
2. **Transformar** los datos: limpieza de datos, formato de fecha, etc.
3. **Cargar** los datos transformados al Data Warehouse.

---

### 2. ELT — Extract, Load, Transform
**¿En qué se diferencia del ETL?**  
En ELT, el orden de los pasos es diferente. Primero, extraemos los datos, luego los cargamos y, finalmente, los transformamos en el sistema de almacenamiento.

**¿Cuándo conviene usarlo?**  
Se usa cuando se quiere aprovechar la capacidad de procesamiento del Data Warehouse o Data Lake, transformando los datos **después** de cargarlos.

**¿Por qué herramientas como dbt encajan en este modelo?**  
Herramientas como dbt (Data Build Tool) permiten transformar datos dentro del Data Warehouse después de haber sido cargados, facilitando la transformación.

**Ejemplo:**  
En un Data Lakehouse moderno, es común usar ELT, ya que primero cargamos todos los datos crudos y luego los transformamos de forma más eficiente en el mismo sistema.




 2. Batch vs Streaming

 Qué significa procesar datos en batch?

El procesamiento por lotes (Batch) implica procesar grandes volúmenes de datos a intervalos programados. En lugar de procesar los datos de manera continua, los datos se recogen y almacenan durante un período de tiempo y luego se procesan todos juntos en un solo "lote".

¿Cada cuánto tiempo se ejecuta un proceso batch típico? 

Los procesos batch suelen ejecutarse a intervalos regulares, como una vez al día, semanalmente o mensualmente. La frecuencia depende de la necesidad de los datos y los procesos de negocio involucrados.

Ventajas y desventajas del procesamiento por lotes:

- Ventajas:

  - Eficiente para grandes volúmenes de datos.
  - Procesos sin interferencias, ya que los datos no se procesan continuamente.
  - Suele ser más sencillo de implementar en entornos donde no se necesita inmediatez.

- Desventajas:
  - No es adecuado para situaciones que requieren datos en tiempo real.
  - Puede haber una latencia considerable entre la recolección de datos y su procesamiento.

Ejemplo: ¿cuándo usarías batch en un pipeline de reportes financieros?

El procesamiento por lotes es ideal para reportes financieros, ya que las transacciones pueden acumularse durante todo el día o la semana, y luego ser procesadas todas juntas en la noche para generar reportes semanales o mensuales. De esta manera, los sistemas no necesitan estar activos todo el tiempo y pueden generar los informes necesarios al final del ciclo de trabajo...



 3. Data Warehouse
¿Qué es y para qué sirve?

Un Data Warehouse (DW) es una base de datos especializada diseñada para almacenar y analizar grandes volúmenes de datos de forma eficiente. Se utiliza para consolidar datos provenientes de diferentes fuentes para realizar análisis y reportes. Los datos son organizados y almacenados de tal manera que facilitan consultas rápidas y eficientes.

**¿Qué tipo de datos almacena?**  
Almacena datos estructurados y procesados provenientes de diferentes sistemas, como bases de datos operacionales, archivos log, o incluso datos de otras aplicaciones. Los datos suelen ser históricos y no se modifican una vez almacenados, lo que permite hacer análisis de tendencias a largo plazo.

**Ejemplos de tecnologías:**
- **Snowflake**: Una plataforma moderna de almacenamiento de datos en la nube.
- **BigQuery**: El sistema de almacenamiento de datos de Google Cloud, ideal para grandes volúmenes de datos.
- **Redshift**: La solución de almacenamiento de datos de Amazon Web Services (AWS).
- **Synapse**: La plataforma de datos de Microsoft Azure, ideal para análisis y procesamiento de grandes cantidades de datos.

¿Qué es el esquema estrella (star schema)?  
El esquema estrella es un tipo de modelado de datos utilizado en un Data Warehouse, donde se centraliza la tabla de hechos (que contiene los datos principales, como ventas, ingresos, etc.) y se conecta con diversas tablas de dimensiones (que describen los atributos relacionados, como cliente, productos, tiempo, etc.).



## 4. Data Lake

**¿Qué lo diferencia de un Data Warehouse?**  
A diferencia de un **Data Warehouse**, que solo almacena datos estructurados y transformados, un **Data Lake** permite almacenar **datos crudos y sin procesar**, tanto estructurados como no estructurados. Los datos pueden ser almacenados en su formato original, como archivos CSV, JSON, Avro, imágenes, logs, etc. Esto lo convierte en una solución más flexible para grandes volúmenes de datos, ya que no es necesario transformar los datos antes de almacenarlos.

**¿Qué tipo de archivos se almacenan?**  
- **Estructurados**: CSV, Parquet, Avro.
- **No estructurados**: JSON, imágenes, videos, archivos de log, datos de sensores, etc.
  
**¿Cuál es el riesgo de un Data Lake mal gestionado?**  
El mayor riesgo es convertirlo en un **"data swamp"** (pantano de datos). Esto ocurre cuando los datos no se gestionan de manera adecuada, no se etiquetan correctamente y se almacenan sin ningún orden ni estructura, lo que dificulta el acceso y análisis de la información.

**Ejemplos de tecnologías:**
- **Amazon S3**: Servicio de almacenamiento en la nube de Amazon, que se utiliza comúnmente como un Data Lake.
- **Azure Data Lake Storage**: Plataforma de almacenamiento de datos no estructurados de Microsoft Azure.
- **Google Cloud Storage**: El sistema de almacenamiento de Google que permite almacenar grandes volúmenes de datos no estructurados.


## 5. Data Lakehouse
## 6. Pipeline de datos
## 7. Otros conceptos
## 8. Caso práctico imaginario
## 9. Reflexión personal

