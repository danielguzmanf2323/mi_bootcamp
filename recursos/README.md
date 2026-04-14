# Recursos del Bootcamp

Materiales transversales que se usan a lo largo de todo el programa.

---

## Estructura

```
recursos/
├── datasets/
│   └── ejemplos/       ← archivos pequeños de muestra versionados en git
├── plantillas/         ← plantillas de actividad, proyecto y laboratorio
└── referencias/        ← cheatsheets y guías de referencia rápida
```

---

## datasets/

Los datasets reales **no se versionan en este repo** — son demasiado grandes y
van en Google Drive bajo la estructura:

```
data_engineering_files/
├── semana_01/
│   ├── customers.csv
│   └── maven_fuzzy_factory/
├── semana_02/
│   └── financial_transactions/
└── ...
```

La carpeta `datasets/ejemplos/` sí está versionada y contiene archivos pequeños
(< 1 MB) para pruebas rápidas sin necesidad de descargar el dataset completo.

---

## plantillas/

Plantillas base para crear nuevas actividades, proyectos o laboratorios.
Seguir el formato de estas plantillas garantiza consistencia entre semanas.

Ver también: [CONTRIBUTING.md](../CONTRIBUTING.md) para las reglas completas
de cómo añadir contenido nuevo.

---

## referencias/

Cheatsheets y guías de referencia rápida:
- Comandos Git más usados en el bootcamp
- Sintaxis PySpark vs SQL equivalente
- Comandos dbutils de Databricks
- Patrones de la arquitectura Medallón
