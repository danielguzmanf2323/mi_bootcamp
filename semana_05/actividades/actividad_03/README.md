# Actividad 03 — Semana 05: APPLY CHANGES INTO — CDC, SCD1 y SCD2 automáticos

**Semana:** 05  
**Tema:** CDC y dimensiones de cambio lento con `dlt.apply_changes()`  
**Nivel:** Avanzado  
**Modalidad:** Individual  
**Entorno:** Databricks Enterprise (DLT Advanced edition para SCD2)  
**Prerequisito:** Actividades 01 y 02 completadas

---

## Contexto

En semana 04 implementaste SCD1 con MERGE INTO:

```sql
MERGE INTO silver.users_scd1 AS destino
USING usuarios_source AS fuente
ON destino.user_id = fuente.user_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

32 líneas de SQL + lógica Python para detectar qué cambió + orquestación con el Job. Correcto, funcional, pero frágil: si cambia el esquema, editas el MERGE. Si necesitas SCD2 (guardar historial), reescribes todo.

`dlt.apply_changes()` hace CDC (Change Data Capture) de forma declarativa. Le dices: "esta es la clave, este campo indica el orden de los cambios, quiero SCD1 o SCD2." DLT hace el resto, incluyendo el historial con fechas de vigencia en SCD2.

---

## Material de estudio previo

- ¿Qué es CDC (Change Data Capture)? ¿Cómo difiere de un full load?
- ¿Qué es SCD1? ¿Qué información se pierde con SCD1?
- ¿Qué es SCD2? ¿Qué columnas agrega SCD2 a la tabla (_start, _end, _current)?
- ¿Qué tipos de operaciones CDC existen? (INSERT, UPDATE, DELETE)
- ¿Por qué `dlt.apply_changes()` requiere *primero* `dlt.create_streaming_table()`?

---

## Instrucciones Git

```bash
git checkout feature/semana05-dlt-<tu-nombre>
```

Crea tus archivos en:
```
semana_05/actividades/actividad_03/<tu-nombre>/
```

---

## Parte 1 — Simular un feed de cambios (CDC source)

Para el ejemplo de CDC necesitamos datos que lleguen como un stream de cambios — filas con operaciones INSERT/UPDATE/DELETE. Vamos a simular eso con los datos de usuarios.

Crea un notebook SEPARADO (no DLT) `simular_cdc_feed_<tu-nombre>.py`:

```python
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType

# Cargar usuarios existentes como baseline
df_usuarios = spark.table("bronze.users")
print(f"Usuarios base: {df_usuarios.count():,}")

# Simular batch de cambios:
# - 300 usuarios existentes cambiaron de ciudad (UPDATE)
# - 100 usuarios nuevos se registraron (INSERT)
# - 50 usuarios fueron dados de baja (DELETE)

# Batch de cambios — columna 'operacion' indica el tipo de cambio
df_updates = (
    df_usuarios
    .limit(300)
    .withColumn("city", lit("Barcelona"))          # cambiaron de ciudad
    .withColumn("operacion", lit("UPDATE"))
    .withColumn("updated_at", current_timestamp())
)

df_inserts = (
    df_usuarios
    .orderBy("user_id", ascending=False)           # tomar usuarios con IDs más altos
    .limit(100)
    .withColumn("user_id", (df_usuarios["user_id"] + 999999).cast("long"))  # IDs nuevos
    .withColumn("operacion", lit("INSERT"))
    .withColumn("updated_at", current_timestamp())
)

df_deletes = (
    df_usuarios
    .limit(50)
    .orderBy("user_id", ascending=False)
    .withColumn("operacion", lit("DELETE"))
    .withColumn("updated_at", current_timestamp())
)

# Combinar y escribir como archivo de feed CDC
df_cdc_feed = df_updates.union(df_inserts).union(df_deletes)
print(f"Registros en el feed CDC: {df_cdc_feed.count()}")
print(f"  INSERT: {df_cdc_feed.filter('operacion = \"INSERT\"').count()}")
print(f"  UPDATE: {df_cdc_feed.filter('operacion = \"UPDATE\"').count()}")
print(f"  DELETE: {df_cdc_feed.filter('operacion = \"DELETE\"').count()}")

# Guardar en la zona de landing para que DLT lo consuma
(
    df_cdc_feed
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze.users_cdc_feed")
)
print("Feed CDC guardado en bronze.users_cdc_feed")
```

---

## Parte 2 — SCD1 con apply_changes (sobrescribir, sin historial)

Crea un notebook DLT `cdc_scd_<tu-nombre>.py`:

```python
import dlt
from pyspark.sql.functions import col

# Paso 1: declarar la tabla *antes* de apply_changes (OBLIGATORIO)
# DLT necesita saber que esta tabla existe antes de definir cómo se llena
dlt.create_streaming_table(
    name="silver_users_scd1",
    comment="Dimensión de usuarios — SCD1: solo el valor más reciente",
    table_properties={"quality": "silver"},
)

# Paso 2: aplicar los cambios CDC sobre esa tabla
dlt.apply_changes(
    target="silver_users_scd1",                # tabla destino (declarada arriba)
    source="bronze.users_cdc_feed",            # fuente de cambios CDC
    keys=["user_id"],                          # columna(s) que identifican únicamente la fila
    sequence_by=col("updated_at"),             # campo que ordena los cambios (el más reciente gana)
    apply_as_deletes=col("operacion") == "DELETE",  # filas con DELETE se eliminan del destino
    except_column_list=["operacion", "updated_at"], # estas columnas no van a la tabla final
    stored_as_scd_type="1",                    # SCD1: sobrescribir (no guardar historial)
)
```

> `sequence_by` es crítico: si llegan dos cambios para el mismo `user_id`, DLT aplica el que tiene el `updated_at` más reciente — en el orden correcto, sin importar en qué orden llegaron al stream.

Ejecuta el pipeline y verifica:

```python
# En un notebook normal (no DLT)
df_scd1 = spark.table("<schema_pipeline>.silver_users_scd1")
print(f"Total usuarios: {df_scd1.count():,}")

# Los 300 usuarios actualizados deben mostrar city = Barcelona
barcelona = df_scd1.filter("city = 'Barcelona'").count()
print(f"Usuarios con city=Barcelona: {barcelona}")

# Los 50 usuarios eliminados NO deben aparecer
print(f"SCD1 aplicado correctamente: {True if barcelona == 300 else False}")
```

---

## Parte 3 — SCD2 con apply_changes (guardar historial completo)

La diferencia con SCD1 es una sola línea: `stored_as_scd_type="2"`. DLT agrega automáticamente las columnas `__START_AT`, `__END_AT` y marca cuál es la fila más reciente con `__CURRENT`.

```python
# Declarar la tabla SCD2
dlt.create_streaming_table(
    name="silver_users_scd2",
    comment="Dimensión de usuarios — SCD2: historial completo de cambios",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_users_scd2",
    source="bronze.users_cdc_feed",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=col("operacion") == "DELETE",
    except_column_list=["operacion", "updated_at"],
    stored_as_scd_type="2",               # SCD2: cada cambio crea una fila nueva con fechas
    # track_history_column_list — opcional: solo rastrear cambios en estas columnas
    # Si no se especifica, cualquier cambio en cualquier columna genera una nueva versión
)
```

Verificar el historial:

```python
df_scd2 = spark.table("<schema_pipeline>.silver_users_scd2")
print(f"Filas totales (incluyendo historial): {df_scd2.count():,}")

# Ver las columnas que agrega SCD2
print("Columnas SCD2:", [c for c in df_scd2.columns if c.startswith("__")])
# Esperado: ['__START_AT', '__END_AT', '__CURRENT']

# Ver el historial de un usuario específico que cambió de ciudad
usuario_ejemplo = df_scd2.first()["user_id"]
(
    df_scd2
    .filter(col("user_id") == usuario_ejemplo)
    .orderBy("__START_AT")
    .select("user_id", "city", "__START_AT", "__END_AT", "__CURRENT")
    .show(truncate=False)
)
```

Documenta qué ves:
- ¿Cuántas filas tiene el usuario que cambió de ciudad? (debería tener 2: la anterior y la nueva)
- ¿Qué valor tiene `__END_AT` en la fila más reciente?
- ¿Qué valor tiene `__CURRENT` en la fila histórica vs la actual?

---

## Parte 4 — Consultar solo el estado actual (SCD2 best practice)

Una tabla SCD2 contiene historial. Para analítica, normalmente quieres solo el estado actual. La vista estándar:

```python
@dlt.view(name="v_usuarios_actuales")
def v_usuarios_actuales():
    """Vista que filtra solo la versión vigente de cada usuario en SCD2."""
    return (
        dlt.read("silver_users_scd2")
        .filter(col("__CURRENT") == True)
        .drop("__START_AT", "__END_AT", "__CURRENT")
    )
```

```python
# Verificar que solo hay un registro por user_id
df_actuales = spark.table("<schema>.v_usuarios_actuales")
n_usuarios   = df_actuales.count()
n_distintos  = df_actuales.select("user_id").distinct().count()
print(f"Registros: {n_usuarios} | IDs únicos: {n_distintos}")
print(f"Un registro por usuario: {n_usuarios == n_distintos}")
```

---

## Parte 5 — Comparación: MERGE manual (semana 04) vs apply_changes

Completa esta tabla en una celda markdown:

```markdown
| Aspecto | MERGE INTO (semana 04) | apply_changes (DLT) |
|---------|------------------------|---------------------|
| Líneas de código para SCD1 | ~15 SQL + Python | ~10 Python |
| Líneas de código para SCD2 | ~40+ SQL | ~12 Python |
| Columnas de historial (__START, __END, __CURRENT) | Manual — tú las agregas | Automático |
| Manejo de DELETE | Manual — necesitas lógica | `apply_as_deletes=` |
| Desduplicación de cambios en el mismo timestamp | Manual | Automático (sequence_by) |
| Linaje visible en UI | No | Sí |
| Restart si falla a mitad | Reprocesa todo | DLT retoma desde donde quedó |
```

---

## Entrega en Git

```bash
git add semana_05/actividades/actividad_03/<tu-nombre>/
git commit -m "feat: CDC simulation + SCD1 + SCD2 with apply_changes - <tu-nombre>"
git push origin feature/semana05-dlt-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 05] APPLY CHANGES INTO — SCD1 y SCD2 — <Tu Nombre>
```

Incluye en el PR:
- Screenshot de la tabla SCD2 mostrando filas con `__START_AT`, `__END_AT`, `__CURRENT`
- El historial de un usuario que cambió de ciudad (2 filas)
- La tabla comparativa MERGE vs apply_changes completada con tus observaciones

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Feed CDC simulado correctamente | INSERT + UPDATE + DELETE presentes | 20% |
| SCD1 con `apply_changes` funcionando | City=Barcelona en 300 usuarios, 50 deletados | 25% |
| SCD2 con historial visible | Columnas `__START_AT`, `__END_AT`, `__CURRENT` presentes | 30% |
| Vista de estado actual (`__CURRENT = True`) | 1 registro por user_id sin columnas SCD2 | 15% |
| Tabla comparativa MERGE vs apply_changes | Completada con observaciones propias | 10% |

---

## Referencias

- [DLT — apply_changes() Python reference](https://docs.databricks.com/en/delta-live-tables/python-ref.html#apply_changes)
- [SCD Type 1 and Type 2 in DLT](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [CDC con Auto Loader + apply_changes](https://docs.databricks.com/en/delta-live-tables/tutorial-cdc.html)
