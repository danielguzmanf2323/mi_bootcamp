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

---

Cuando hayas terminado con la explicación, realiza el commit:

```bash
git add .
git commit -m "docs: add ETL vs ELT explanation"

## 2. Batch vs Streaming
## 3. Data Warehouse
## 4. Data Lake
## 5. Data Lakehouse
## 6. Pipeline de datos
## 7. Otros conceptos
## 8. Caso práctico imaginario
## 9. Reflexión personal

