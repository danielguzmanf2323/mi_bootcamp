# Guía de Gitflow — Inetum Data Engineer Team

Este documento define el flujo de trabajo con Git para proyectos de Data Engineering (ETL, pipelines, modelos, etc.).
El objetivo es mantener el repositorio ordenado, trazable y colaborativo.

---

## Estructura de ramas principales

| Rama | Propósito |
|------|-----------|
| `main` | Código en producción. Nunca se trabaja directamente aquí. |
| `develop` | Integración de todos los cambios antes de subir a producción. |

---

## Tipos de ramas de trabajo

### `feature/*` — Nuevas funcionalidades

Se usan para desarrollar nuevas funcionalidades o cambios planificados.

**Origen:** `develop` → **Destino:** `develop`

Ejemplos:
- `feature/add-user-model`
- `feature/airflow-dag-orders`
- `feature/dbt-sales-metrics`

```bash
git checkout develop
git pull origin develop
git checkout -b feature/airflow-dag-orders
# ... trabajar, commitear ...
git push origin feature/airflow-dag-orders
```

---

### `bugfix/*` — Corrección de errores en desarrollo

Para errores detectados durante el desarrollo, antes de llegar a producción.

**Origen:** `develop` → **Destino:** `develop`

Ejemplos:
- `bugfix/null-values-customers`
- `bugfix/dag-failure-timeout`

```bash
git checkout develop
git pull origin develop
git checkout -b bugfix/null-values-customers
# ... corregir y commitear ...
git push origin bugfix/null-values-customers
```

---

### `release/*` — Preparación de versión productiva

Se usa para estabilizar y preparar una versión antes de subir a `main`.

**Origen:** `develop` → **Destino:** `main` y `develop`

Ejemplos:
- `release/v1.2.0`

```bash
git checkout develop
git pull origin develop
git checkout -b release/v1.2.0
# ... ajustes finales, bump de versión ...
git push origin release/v1.2.0
# Abrir PR hacia main y hacia develop
```

---

### `hotfix/*` — Errores críticos en producción

Para corregir problemas urgentes directamente desde `main`.

**Origen:** `main` → **Destino:** `main` y `develop`

Ejemplos:
- `hotfix/fix-duplication-prod`
- `hotfix/critical-pipeline-error`

```bash
git checkout main
git pull origin main
git checkout -b hotfix/fix-duplication-prod
# ... aplicar corrección ...
git push origin hotfix/fix-duplication-prod
# Abrir PR hacia main y hacia develop
```

---

### `docs/*` — Documentación

Para cambios exclusivos de documentación: guías, READMEs, diccionarios de datos, etc.

**Origen:** `develop` → **Destino:** `develop`

Ejemplos:
- `docs/pipeline-readme`
- `docs/data-dictionary-sales`
- `docs/update-onboarding-guide`

```bash
git checkout develop
git pull origin develop
git checkout -b docs/data-dictionary-sales
# ... editar archivos de documentación ...
git push origin docs/data-dictionary-sales
```

---

## Flujo de trabajo general

```
develop
  └── git checkout -b feature/mi-funcionalidad
        └── git add .
        └── git commit -m "feat: descripción clara"
        └── git push origin feature/mi-funcionalidad
              └── Pull Request → develop
                    └── Code review + validaciones
                          └── Merge
```

### Paso a paso

1. Actualizar `develop` antes de empezar:
   ```bash
   git checkout develop
   git pull origin develop
   ```

2. Crear la rama para el trabajo:
   ```bash
   git checkout -b feature/nombre-descriptivo
   ```

3. Hacer los cambios y commits:
   ```bash
   git add .
   git commit -m "feat: add sales aggregation model"
   ```

4. Subir la rama al repositorio remoto:
   ```bash
   git push origin feature/nombre-descriptivo
   ```

5. Abrir un Pull Request (PR) hacia `develop` en la plataforma (GitHub / Azure DevOps).

6. Esperar code review y aprobación.

7. Merge realizado por el revisor o el responsable del repo.

---

## Convención de commits

Formato basado en [Conventional Commits](https://www.conventionalcommits.org/):

```
<tipo>: <descripción corta en imperativo>
```

| Tipo | Uso |
|------|-----|
| `feat` | Nueva funcionalidad |
| `fix` | Corrección de error |
| `refactor` | Mejora de código sin cambiar lógica |
| `docs` | Documentación |
| `test` | Tests o validaciones |
| `chore` | Tareas menores (configuración, dependencias) |

### Ejemplos correctos

```
feat: add sales aggregation model
fix: handle null values in customer_id
refactor: optimize join in orders pipeline
docs: update data dictionary for customers table
test: add dbt test for null check on order_id
```

### Evitar

```
cambios varios
fix stuff
update
arreglé cosas
```

---

## Buenas prácticas para Data Engineering

### Commits pequeños y atómicos

Un commit = un cambio lógico. Evitar commits gigantes que mezclen múltiples propósitos.

### Nombres descriptivos

Tanto el nombre de la rama como los commits deben dejar claro qué se hizo y por qué.

### PRs claros

Un buen PR incluye:
- Qué cambia y por qué
- Impacto en los datos o pipelines
- Resultado esperado

### Validaciones antes del merge

Especialmente en proyectos con dbt o similares, verificar:

```bash
# Ejemplo con dbt
dbt run --select mi_modelo
dbt test --select mi_modelo
```

Checks mínimos:
- Nulos en columnas críticas
- Duplicados en claves primarias
- Volumen de registros dentro de lo esperado

### Validar DAGs antes de hacer push

Si se trabaja con Apache Airflow:

```bash
airflow dags list
airflow tasks test mi_dag mi_task 2024-01-01
```

### Nunca hacer push directo a `main`

```bash
# Esto está prohibido
git push origin main
```

---

## Errores comunes a evitar

- Commitear sin mensaje claro o con mensajes genéricos
- No actualizar `develop` antes de crear una rama nueva
- PRs demasiado grandes (difíciles de revisar)
- No validar datos antes del merge
- Mezclar múltiples cambios no relacionados en una sola rama
- Trabajar directamente en `main` o `develop`

---

## Referencia rápida de comandos

```bash
# Actualizar rama local
git pull origin develop

# Ver ramas locales y remotas
git branch -a

# Cambiar de rama
git checkout nombre-de-rama

# Crear y cambiar a nueva rama
git checkout -b feature/nueva-funcionalidad

# Ver estado de cambios
git status

# Agregar cambios al staging
git add .
git add ruta/al/archivo.py

# Commitear
git commit -m "feat: descripción del cambio"

# Subir rama al remoto
git push origin feature/nueva-funcionalidad

# Ver historial de commits
git log --oneline --graph

# Traer cambios del remoto sin hacer merge
git fetch origin
```

---

## Resumen

| Acción | Regla |
|--------|-------|
| Empezar a trabajar | Siempre desde `develop` actualizado |
| Tipo de rama | `feature/`, `bugfix/`, `hotfix/`, `release/`, `docs/` |
| Commits | Claros, pequeños, en formato Conventional Commits |
| Integración | Siempre mediante Pull Request, nunca push directo |
| Antes del merge | Validar datos, tests y dependencias |

---

> **Regla de oro:** Si alguien revisa tu PR, debe entender qué hiciste y por qué sin necesidad de preguntarte.
