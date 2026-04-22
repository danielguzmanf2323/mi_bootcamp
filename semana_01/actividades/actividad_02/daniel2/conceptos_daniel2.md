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

El procesamiento por lotes es ideal para reportes financieros, ya que las transacciones pueden acumularse durante todo el día o la semana, y luego ser procesadas todas juntas en la noche para generar reportes semanales o mensuales. De esta manera, los sistemas no necesitan estar activos todo el tiempo y pueden generar los informes necesarios al final del ciclo de trabajo.



## 3. Data Warehouse
## 4. Data Lake
## 5. Data Lakehouse
## 6. Pipeline de datos
## 7. Otros conceptos
## 8. Caso práctico imaginario
## 9. Reflexión personal

