# Actividad 01 — Semana 06: Delta Lake en Producción

**Semana:** 06  
**Tema:** Optimización, ciclo de vida y time travel en tablas Delta  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise

---

## Objetivo

Entender y aplicar las operaciones de mantenimiento y optimización de tablas Delta Lake
que diferencian un entorno de desarrollo de uno en producción real:
cuándo y por qué optimizar, cómo gestionar el ciclo de vida de los datos,
y cómo usar time travel para auditoría y recuperación.

---

## Dataset

Las tablas Delta construidas en semanas 02-05, ya disponibles en tu workspace:

| Tabla | Schema | Descripción |
|-------|--------|-------------|
| `bronze.transactions` | bronze | Ingesta cruda del dataset financiero |
| `silver.transactions` | silver | Transacciones limpias y tipadas |
| `silver.customers` | silver | Clientes enriquecidos |
| `gold.fraude_por_categoria` | gold | Agregaciones de fraude |

No necesitas descargar nada nuevo.

---

## Material de estudio previo

Antes de empezar, investiga y responde estas preguntas en tu documento de entrega:

1. ¿Qué es el **layout de archivos** de una tabla Delta y por qué afecta al rendimiento de las queries?
2. ¿Qué diferencia hay entre `ZORDER BY` y **Liquid Clustering**? ¿Cuándo elegirías uno sobre el otro?
3. ¿Qué datos borra exactamente `VACUUM`? ¿Qué riesgo tiene si reduces el periodo de retención por debajo de 7 días?
4. ¿Qué es el **Delta transaction log** y cómo lo usa el time travel?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana06-delta-produccion-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_06/actividades/actividad_01/<tu-nombre>/
```

El entregable es un notebook `.ipynb` con tu código y un archivo `notas_<tu-nombre>.md`
con tus observaciones sobre cada operación.

---

## Parte 1 — Diagnóstico: estado actual de tus tablas

Antes de optimizar, entiende en qué estado están las tablas que construiste.

Ejecuta sobre `silver.transactions`:

```python
# Historial de la tabla
spark.sql("DESCRIBE HISTORY silver.transactions").show(10, truncate=False)

# Detalle de archivos actuales
spark.sql("DESCRIBE DETAIL silver.transactions").show(truncate=False)

# Estadísticas de archivos
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "silver.transactions")
dt.detail().select("numFiles", "sizeInBytes", "partitionColumns").show()
```

Documenta: ¿cuántos archivos tiene la tabla? ¿Cuántas versiones en el historial?

Commit esperado:
```bash
git commit -m "docs: add table diagnostics for silver.transactions"
```

---

## Parte 2 — OPTIMIZE y ZORDER

`OPTIMIZE` consolida los pequeños archivos Parquet que se van generando.
`ZORDER BY` reordena los datos dentro de esos archivos para acelerar filtros frecuentes.

```sql
-- Optimizar sin ordenar (solo compactar)
OPTIMIZE silver.transactions

-- Optimizar con colocación por columnas de filtro frecuente
OPTIMIZE silver.transactions ZORDER BY (transaction_date, category)
```

Después de cada operación:
1. Compara el número de archivos antes y después
2. Ejecuta la misma query de filtro (`WHERE category = 'X' AND transaction_date > '...'`) y compara el tiempo
3. Anota la diferencia en tu documento

Commit esperado:
```bash
git commit -m "feat: apply OPTIMIZE and ZORDER to silver.transactions"
```

---

## Parte 3 — Liquid Clustering (alternativa moderna a ZORDER)

Liquid Clustering es la evolución de ZORDER: clustering incremental que no requiere
reescribir toda la tabla cada vez.

```sql
-- Convertir una tabla existente a Liquid Clustering
ALTER TABLE silver.customers
CLUSTER BY (customer_state, customer_city)

-- Ejecutar clustering
OPTIMIZE silver.customers
```

Investiga y documenta:
- ¿Por qué Databricks recomienda Liquid Clustering sobre `ZORDER` para tablas nuevas?
- ¿Puedes aplicar Liquid Clustering a `silver.transactions`? ¿Qué pasa si lo intentas?

Commit esperado:
```bash
git commit -m "feat: apply Liquid Clustering to silver.customers"
```

---

## Parte 4 — VACUUM: gestión del ciclo de vida

`VACUUM` elimina los archivos físicos que ya no son necesarios para el historial de la tabla.

```sql
-- Ver qué archivos borraría (dry run — sin borrar nada)
VACUUM silver.transactions RETAIN 168 HOURS DRY RUN

-- Ejecutar el vacuum (retención de 7 días = 168 horas, valor por defecto)
VACUUM silver.transactions RETAIN 168 HOURS
```

Importante:
- Después de `VACUUM`, intenta acceder a una versión anterior al periodo de retención. ¿Qué error recibes?
- Documenta por qué nunca deberías bajar la retención por debajo de la duración de tus jobs más largos

Commit esperado:
```bash
git commit -m "docs: document VACUUM behavior and retention tradeoffs"
```

---

## Parte 5 — Time Travel y recuperación

```sql
-- Ver el historial completo
DESCRIBE HISTORY silver.transactions

-- Leer una versión anterior
SELECT * FROM silver.transactions VERSION AS OF 1 LIMIT 10

-- Leer por timestamp
SELECT * FROM silver.transactions
TIMESTAMP AS OF '2024-01-01 00:00:00'
LIMIT 10

-- Restaurar la tabla a una versión anterior (úsalo con cuidado)
RESTORE TABLE silver.transactions TO VERSION AS OF 2
```

Simula un escenario de recuperación:
1. Introduce intencionalmente un error (sobreescribe un campo con un valor incorrecto)
2. Verifica el error con una query
3. Usa time travel para identificar la versión correcta
4. Restaura la tabla
5. Documenta los pasos como si fuera un runbook de incidencias real

Commit esperado:
```bash
git commit -m "docs: add time travel recovery runbook"
```

---

## Parte 6 — Zero-Copy Clone

Clone crea una copia de la tabla que comparte el almacenamiento físico con el original.
Ideal para crear entornos de prueba sin duplicar storage.

```sql
-- Crear un entorno de desarrollo sin copiar datos
CREATE TABLE bronze.transactions_dev
CLONE bronze.transactions

-- Verificar: el clone tiene los mismos datos pero es independiente
SELECT COUNT(*) FROM bronze.transactions_dev

-- Modificar el clone no afecta al original
DELETE FROM bronze.transactions_dev WHERE amount < 0
```

Documenta:
- ¿Cuánto espacio ocupa el clone inicialmente?
- ¿Qué pasa con el espacio cuando modificas el clone?

Commit esperado:
```bash
git commit -m "feat: create dev clone and document zero-copy behavior"
```

---

## Entrega en Git

```bash
git push origin feature/semana06-delta-produccion-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 06] Delta Lake en Producción — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Diagnóstico inicial documentado | Número de archivos y versiones antes de cualquier operación | 10% |
| [Requerido] OPTIMIZE + ZORDER aplicados y comparados | Query antes/después con tiempos medidos | 25% |
| [Requerido] VACUUM ejecutado con dry run previo | Documentado el efecto sobre el time travel | 20% |
| [Requerido] Runbook de recuperación con time travel | Los 5 pasos documentados como si fuera producción real | 25% |
| [Recomendado] Liquid Clustering investigado y aplicado | Comparativa con ZORDER incluida | 15% |
| [Opcional] Zero-copy clone con análisis de storage | Comportamiento del storage antes y después de modificar el clone | 5% |
| **Total** | | **100%** |

---

## Referencias

- [Delta Lake: Table Utility Commands (Databricks docs)](https://docs.databricks.com/delta/optimize.html)
- [Liquid Clustering (Databricks docs)](https://docs.databricks.com/delta/clustering.html)
- [Delta Lake Time Travel (Databricks docs)](https://docs.databricks.com/delta/history.html)
