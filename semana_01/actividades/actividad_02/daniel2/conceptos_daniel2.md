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

**¿Qué problema viene a resolver?**  
El **Data Lakehouse** es una arquitectura moderna que combina lo mejor de los **Data Lakes** y los **Data Warehouses**. El problema que resuelve es la falta de consistencia y la alta latencia que suelen tener los Data Lakes cuando se trata de análisis de datos estructurados. A diferencia de los Data Lakes, que almacenan datos crudos, el Data Lakehouse permite tener un sistema único que puede manejar tanto datos no estructurados como estructurados, optimizando el rendimiento y la consistencia para el análisis de datos.

**¿Qué combina del Data Lake y del Data Warehouse?**  
- **De los Data Lakes**, toma la capacidad de almacenar grandes volúmenes de datos no estructurados a bajo costo.
- **De los Data Warehouses**, toma la capacidad de análisis de datos estructurados y el uso de esquemas y herramientas que optimizan el rendimiento de las consultas.

**Tecnologías:**
- **Delta Lake**: Plataforma que combina un Data Lake con características de un Data Warehouse, agregando soporte para transacciones ACID y control de versiones.
- **Apache Iceberg**: Un formato de almacenamiento para Data Lakes y Data Warehouses que ofrece transacciones ACID y escalabilidad.
- **Apache Hudi**: Plataforma similar a Delta Lake que agrega capacidades de actualización y eliminación en Data Lakes.

**¿Por qué se dice que es la arquitectura moderna de referencia?**  
El Data Lakehouse es considerado la arquitectura moderna porque combina lo mejor de ambos mundos: la escalabilidad y flexibilidad del Data Lake con el rendimiento y la consistencia del Data Warehouse. Esto lo convierte en una solución ideal para organizaciones que necesitan almacenar grandes volúmenes de datos y realizar análisis rápidos sin tener que mover los datos entre diferentes sistemas.

## 6. Pipeline de datos

**¿Qué es un pipeline de datos?**  
Un **pipeline de datos** es un conjunto de procesos automatizados que mueve los datos desde su origen hasta su destino, pasando por varias etapas de transformación y procesamiento. Los pipelines de datos suelen ser orquestados para que los datos se muevan y transformen de manera eficiente y sin intervención manual.

**¿Qué etapas puede tener?**
Un pipeline de datos típico puede incluir las siguientes etapas:
- **Ingesta**: La recopilación de datos desde las fuentes, que pueden ser bases de datos, archivos, APIs, etc.
- **Transformación**: Limpieza, validación, agregación o cualquier otro proceso que cambie el formato o la calidad de los datos.
- **Almacenamiento**: El destino donde se guardan los datos procesados (Data Warehouse, Data Lake, etc.).
- **Consumo**: El uso de los datos, como análisis, informes, visualizaciones o la creación de modelos predictivos.

**¿Qué herramienta se usa para orquestar pipelines?**  
- **Apache Airflow**: Una herramienta de orquestación de flujos de trabajo que permite programar y supervisar pipelines de datos.

**Ejemplo de flujo de un pipeline simple:**
- **Fuente**: Base de datos de ventas.
- **Transformación**: Limpiar los datos, convertir los valores de la fecha al formato adecuado, agregar columnas adicionales.
- **Destino**: Data Warehouse para almacenar los datos procesados.
  
Este flujo puede ser orquestado con herramientas como **Apache Airflow**.


## 7. Otros conceptos

**Data Mart**  
Un **Data Mart** es una versión más pequeña y específica de un **Data Warehouse**. Se centra en un área particular de un negocio, como ventas o marketing. Los Data Marts se utilizan para proporcionar a los usuarios acceso rápido a los datos relevantes para su departamento o función.

**Particionado de datos**  
El particionado de datos es una técnica utilizada en bases de datos y sistemas de almacenamiento para dividir grandes volúmenes de datos en fragmentos más pequeños y manejables. Esto ayuda a mejorar el rendimiento de las consultas y facilita el manejo de grandes cantidades de información.

**Formato columnar vs fila (Parquet vs CSV)**  
- **Formato columnar**: En un archivo **columnar** (como Parquet), los datos se almacenan por columna, lo que mejora el rendimiento de las consultas que acceden a un pequeño subconjunto de las columnas.  
- **Formato de fila**: En un archivo **de fila** (como CSV), los datos se almacenan por fila, lo que es útil para consultas que necesitan acceder a todos los campos de una sola fila.

**Data Catalog**  
Un **Data Catalog** es una herramienta que permite a las organizaciones gestionar, buscar y entender los datos disponibles dentro de su infraestructura. Es útil para realizar un seguimiento de las fuentes de datos, definir metadatos y garantizar la gobernanza de los datos.

**Linaje de datos (Data Lineage)**  
El **linaje de datos** describe el recorrido de los datos desde su origen hasta su destino. Permite a los usuarios ver cómo se transforman y manipulan los datos a lo largo de su ciclo de vida.

**Schema-on-read vs Schema-on-write**  
- **Schema-on-read**: La estructura de los datos se aplica cuando se leen. Esto es común en los **Data Lakes**, donde los datos no necesitan una estructura predefinida antes de ser almacenados.  
- **Schema-on-write**: Los datos deben ajustarse a un esquema cuando se escriben en el sistema, como en los **Data Warehouses**.

**SLA de datos**  
El **SLA (Service Level Agreement)** de datos establece los acuerdos sobre la calidad, disponibilidad y tiempo de respuesta de los datos. Es fundamental para asegurar que los datos estén disponibles cuando se necesitan y que cumplan con los estándares de calidad.

**Idempotencia en pipelines**  
La **idempotencia** en los pipelines de datos significa que un proceso puede ejecutarse varias veces sin cambiar el resultado más de una vez. Es un principio importante en los pipelines, ya que asegura que los datos no se dupliquen o se pierdan si un proceso se ejecuta varias veces.

**CDC — Change Data Capture**  
**CDC** es una técnica utilizada para capturar y registrar los cambios que ocurren en los datos de una fuente en tiempo real, lo que permite replicar esos cambios a otros sistemas sin necesidad de recargar toda la base de datos.

**Data Mesh**  
El **Data Mesh** es una nueva arquitectura que propone tratar los datos como un producto y distribuir la responsabilidad del manejo de datos entre equipos de negocio. Cada equipo es responsable de los datos que producen, promoviendo la descentralización.



## 8. Caso práctico imaginario


**Pregunta**:  
"¿Cuáles fueron los 10 productos más vendidos en los últimos 7 días, por región?"

**¿De dónde vienen los datos?**  
Los datos provienen de la plataforma de e-commerce de la empresa. La fuente incluye registros de ventas, que contienen información sobre el producto, la cantidad vendida, la fecha de la transacción y la región de venta.

**¿Batch o streaming?**  
Este caso podría beneficiarse de un **procesamiento por lotes (batch)**, ya que no se requiere tiempo real para obtener los 10 productos más vendidos, y los datos pueden ser procesados a intervalos regulares, como una vez al día.

**¿Cómo los transformas?**  
Se pueden realizar las siguientes transformaciones:
- **Filtrar** las transacciones para que solo incluyan las ventas de los últimos 7 días.
- **Agrupar** por producto y región, sumando las cantidades vendidas.
- **Ordenar** los productos por cantidad vendida, limitando a los 10 productos más vendidos por cada región.

**¿Dónde los almacenas?**  
El resultado podría ser almacenado en un **Data Warehouse** (como Redshift o BigQuery), ya que este sistema permite realizar consultas rápidas y eficientes sobre grandes volúmenes de datos estructurados.

**¿Quién los consume?**  
El equipo de **análisis de negocios** o **gerentes de ventas** pueden usar esta información para entender qué productos son más populares y tomar decisiones estratégicas sobre inventarios o promociones.


## 9. Reflexión personal

A lo largo de esta actividad, pude aprender y profundizar en varios conceptos fundamentales del mundo de los datos. Primero, entendí la diferencia entre **ETL** y **ELT**, lo cual es crucial para elegir el enfoque correcto según las necesidades del negocio y la infraestructura disponible. 

Además, el procesamiento por **Batch** y **Streaming** me permitió comprender cómo y cuándo utilizar cada uno en función de los requisitos de tiempo y volumen de datos. Fue especialmente útil conocer las diferencias y cómo se pueden aplicar en diferentes casos de uso.

La actividad también me ayudó a familiarizarme con conceptos como **Data Warehouse**, **Data Lake** y **Data Lakehouse**, y cómo cada uno se adapta a distintos escenarios de almacenamiento y análisis de datos.

En resumen, esta actividad me permitió ver cómo los conceptos teóricos se aplican en situaciones reales y cómo un Data Engineer debe tomar decisiones basadas en la arquitectura de datos adecuada para cada tipo de negocio.