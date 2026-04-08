# Proyecto Integrador — Semana 02: Sistema de Detección de Patrones de Fraude

**Semana:** 02  
**Tipo:** Proyecto integrador  
**Modalidad:** Individual (presentación en sesión sincrónica)  
**Entorno:** Databricks Community Edition

---

## Contexto

Durante esta semana construiste herramientas progresivamente:

- Actividad 01: leer y explorar el dataset de transacciones
- Actividad 02: conectar las 5 tablas con JOINs
- Actividad 03: aplicar funciones de fecha y window functions
- Actividad 04: construir la arquitectura Medallón completa

El proyecto integrador tiene un objetivo diferente: **usar todo eso para responder preguntas de negocio reales**.

Imagina que trabajas en el equipo de Data Engineering de un banco. El equipo de Fraude te pide un conjunto de tablas Gold listas para que sus analistas puedan trabajar sin tocar los datos crudos. Tu entrega son esas tablas, más una investigación sobre los patrones de fraude que encontraste.

---

## Dataset

**[data_engineering_files/semana_02_proyecto](https://drive.google.com/drive/folders/1NPcvkwEyU5t9euXay3Uzxo02LqY_Ptb9)**

Los mismos 5 archivos del dataset financiero.

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana02-proyecto-<tu-nombre>
```

Estructura esperada:

```
semana_02/proyecto/<tu-nombre>/
    01_bronze_<tu-nombre>.py
    02_silver_<tu-nombre>.py
    03_gold_analisis_fraude_<tu-nombre>.py
    04_gold_perfil_usuario_<tu-nombre>.py
    05_informe_<tu-nombre>.md
```

---

## Parte 1 — Pipeline Bronze y Silver

Reutiliza y mejora lo que construiste en la Actividad 04. Requisitos adicionales:

### Bronze
- Los 5 archivos ingestados como tablas Delta sin transformaciones.
- Agrega una columna `_ingested_at` con el timestamp de ingesta usando `F.current_timestamp()`.
- Documenta el conteo de filas de cada tabla.

### Silver
- Reads desde Bronze exclusivamente.
- Limpieza completa: tipos, nombres estandarizados, nulls documentados.
- JOIN de las 5 tablas → `silver_transactions` con todas las dimensiones.
- Columnas derivadas de fecha: `hora`, `dia_semana`, `es_fin_de_semana`, `mes`, `anio`.
- Columna `amount_abs` para manejar montos negativos.
- Documenta: ¿cuántos registros se pierden en cada JOIN? ¿Por qué?

Commit esperado:
```bash
git commit -m "feat: bronze and silver layers for fraud project"
```

---

## Parte 2 — Gold: Tablas de Análisis de Fraude

Construye las siguientes tablas Gold. Cada una responde una pregunta de negocio específica.

### Tabla 1: `gold_resumen_fraude`

Resumen general del dataset:

```python
df_resumen = df_silver.agg(
    F.count("transaction_id").alias("total_transacciones"),
    F.sum("is_fraud").alias("total_fraudes"),
    F.round(F.sum("is_fraud") / F.count("transaction_id") * 100, 4).alias("tasa_fraude_global_pct"),
    F.sum(F.when(F.col("is_fraud") == 1, F.col("amount_abs"))).alias("monto_total_fraudulento"),
    F.sum("amount_abs").alias("monto_total"),
    F.round(
        F.sum(F.when(F.col("is_fraud") == 1, F.col("amount_abs"))) / F.sum("amount_abs") * 100, 4
    ).alias("pct_monto_fraudulento")
)
df_resumen.write.format("delta").mode("overwrite").saveAsTable("gold_resumen_fraude")
```

---

### Tabla 2: `gold_fraude_por_dimension`

Una tabla que consolida la tasa de fraude por múltiples dimensiones analíticas:

| Dimensión | Descripción |
|-----------|-------------|
| `card_type` | Tipo de tarjeta (crédito, débito, prepago) |
| `merchant_category` | Categoría del comercio (MCC) |
| `hora_dia` | Hora del día (0-23) |
| `dia_semana` | Día de la semana |
| `es_fin_de_semana` | Si es sábado o domingo |

Para cada dimensión, calcula: total de transacciones, total de fraudes, tasa de fraude %, monto promedio.

Puedes hacer 5 tablas separadas o una tabla unificada con una columna `dimension` y `valor`.

---

### Tabla 3: `gold_fraude_por_monto`

¿El fraude se concentra en montos altos o bajos?

```python
df_silver_buckets = df_silver.withColumn(
    "rango_monto",
    F.when(F.col("amount_abs") < 10, "< $10")
     .when(F.col("amount_abs") < 50, "$10 - $50")
     .when(F.col("amount_abs") < 100, "$50 - $100")
     .when(F.col("amount_abs") < 500, "$100 - $500")
     .when(F.col("amount_abs") < 1000, "$500 - $1000")
     .otherwise("> $1000")
)

df_gold_monto = df_silver_buckets.groupBy("rango_monto") \
    .agg(
        F.count("transaction_id").alias("total_transacciones"),
        F.sum("is_fraud").alias("total_fraudes"),
        F.round(F.sum("is_fraud") / F.count("transaction_id") * 100, 2).alias("tasa_fraude_pct"),
        F.avg("amount_abs").alias("monto_promedio")
    )

df_gold_monto.write.format("delta").mode("overwrite").saveAsTable("gold_fraude_por_monto")
```

---

## Parte 3 — Gold: Perfil de Usuario en Riesgo

Construye una tabla Gold que permita identificar usuarios con comportamiento atípico.

```
gold_perfil_usuario
```

Columnas esperadas:

| Columna | Descripción |
|---------|-------------|
| `user_id` | Identificador del usuario |
| `total_transacciones` | Total de transacciones históricas |
| `total_fraudes` | Total de transacciones fraudulentas |
| `tasa_fraude_pct` | Porcentaje de fraude personal |
| `monto_total` | Monto total transaccionado |
| `monto_fraudulento` | Monto total en transacciones fraudulentas |
| `num_tarjetas` | Número de tarjetas distintas usadas |
| `num_categorias_mcc` | Número de categorías de comercio distintas |
| `primer_transaccion` | Fecha de la primera transacción |
| `ultima_transaccion` | Fecha de la última transacción |
| `dias_activo` | Diferencia en días entre primera y última transacción |
| `categoria_riesgo` | `[Requerido]` Alta / Media / Baja, según tu criterio |

Para `categoria_riesgo`, define tu propio criterio de segmentación. Documenta el criterio en markdown.

Commit esperado:
```bash
git commit -m "feat: gold user risk profile table with risk category"
```

---

## Parte 4 — Informe de Hallazgos

Crea el archivo `05_informe_<tu-nombre>.md` con tus conclusiones. El informe debe responder:

### Preguntas obligatorias [Requerido]

1. **Tasa de fraude global:** ¿Qué porcentaje de transacciones son fraudulentas? ¿Y del monto total?
2. **Categoría MCC de mayor riesgo:** ¿Qué tipo de comercio concentra más fraude? ¿Tiene sentido desde la perspectiva de negocio?
3. **Tipo de tarjeta más vulnerable:** ¿Hay diferencia entre débito, crédito y prepago?
4. **Patrón temporal:** ¿A qué hora y en qué días es más frecuente el fraude?
5. **Monto y fraude:** ¿El fraude ocurre más en transacciones de montos bajos o altos?

### Preguntas recomendadas [Recomendado]

6. ¿Existen usuarios con tasa de fraude muy superior a la media? ¿Cuántos?
7. ¿El fraude aumentó o disminuyó a lo largo del período observado?
8. ¿Cambiarías el diseño de alguna tabla Gold basándote en lo que descubriste?

### Reflexión sobre la pipeline [Requerido]

- ¿Qué decisiones tomaste en Silver que afectaron los resultados en Gold?
- ¿Qué información del dataset NO pudiste usar efectivamente? ¿Por qué?
- Si tuvieras que entregar estas tablas a un equipo de analistas, ¿qué documentación adicional agregarías?

---

## Entrega en Git

```bash
git add semana_02/proyecto/<tu-nombre>/
git commit -m "feat: fraud detection project - full medallion pipeline - <tu-nombre>"
git push origin feature/semana02-proyecto-<tu-nombre>
```

PR hacia `develop`:
```
[Semana 02 - Proyecto] Detección de Fraude — <Tu Nombre>
```

En el PR incluye:
- La tasa de fraude global que encontraste
- El hallazgo más sorprendente
- Una dificultad técnica que enfrentaste y cómo la resolviste

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Bronze: 5 tablas con `_ingested_at` | Delta, sin transformaciones | 10% |
| Silver: JOIN completo + limpieza | Tipos correctos, pérdidas documentadas | 20% |
| Gold análisis de fraude | 3 tablas orientadas a negocio | 25% |
| Gold perfil de usuario con `categoria_riesgo` | Criterio documentado | 20% |
| Informe: preguntas obligatorias respondidas | Con evidencia de código | 20% |
| Git: commits descriptivos, PR bien descrito | Mínimo 4 commits | 5% |

---

## Consideraciones técnicas

- Si Databricks Community Edition te da problemas de memoria con el dataset completo, trabaja con una muestra representativa y documenta que lo estás haciendo:

```python
# Muestra del 10% para exploración
df_sample = df_transactions.sample(fraction=0.10, seed=42)
```

- Si encuentras que `is_fraud` tiene NULLs (transacciones sin etiqueta), documenta qué hiciste con ellas. No hay una respuesta única correcta.

- Los montos negativos son válidos en datos financieros (reversas, devoluciones). Usa `amount_abs` para análisis de volumen pero no elimines las filas negativas.

---

## Referencias

- [Actividad 04 semana 02](../actividades/actividad_04/README.md) — Medallón completo
- [Actividad 03 semana 02](../actividades/actividad_03/README.md) — Window functions y fechas
- [Proyecto semana 01](../../semana_01/proyecto/README.md) — Referencia de pipeline anterior
