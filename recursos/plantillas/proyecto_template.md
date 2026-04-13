# Proyecto Semana XX — <Título>

**Semana:** XX
**Modalidad:** Individual
**Entorno:** Databricks CE | Databricks Enterprise | Microsoft Fabric

---

## Objetivo

<!-- Qué construirá el estudiante al terminar este proyecto.
     Debe conectar directamente con las actividades de la semana. -->

---

## Dataset

<!-- Mismo dataset que las actividades de la semana.
     Link a Google Drive: data_engineering_files/semana_XX/ -->

---

## Contexto del negocio

<!-- Narrativa que da sentido al trabajo técnico.
     El estudiante debe saber por qué importa lo que está construyendo. -->

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semanaXX-proyecto-<tu-nombre>
```

Carpeta de entrega:
```
semana_XX/proyecto/<tu-nombre>/
```

---

## Requisitos técnicos

### Parte 1 — <Nombre>

<!-- Descripción detallada. -->

### Parte 2 — <Nombre>

<!-- Descripción detallada. -->

### Parte 3 — Análisis e informe

Documenta tus hallazgos en un archivo `informe_<tu-nombre>.md` dentro de tu carpeta. Incluye:
- Descripción del pipeline construido
- Al menos 3 hallazgos relevantes del análisis
- Decisiones técnicas que tomaste y por qué

---

## Entrega

```bash
git push origin feature/semanaXX-proyecto-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana XX] Proyecto — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Pipeline completo y funcional | Bronze → Silver → Gold sin errores | % |
| [Requerido] Schemas explícitos | `bronze.*`, `silver.*`, `gold.*` | % |
| [Requerido] Informe con hallazgos | Al menos 3 hallazgos documentados | % |
| [Recomendado] Calidad de datos documentada | Nulos, duplicados, anomalías | % |
| [Opcional] Gold table adicional | Más allá de los requisitos mínimos | % |
| **Total** | | **100%** |
