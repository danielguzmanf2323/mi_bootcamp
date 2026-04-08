# Actividad 01 — Mi primer flujo con Git

**Semana:** 01  
**Tema:** Control de versiones con Git  
**Nivel:** Junior  
**Modalidad:** Individual

---

## Objetivo

Practicar el flujo de trabajo con Git: crear una rama, realizar commits progresivos y abrir un Pull Request. No se requiere programar nada — el entregable es un documento de texto que irás construyendo a lo largo de la actividad.

---

## Contexto

Eres nuevo en el equipo de Data Engineering. Tu primer tarea es documentar, en tus propias palabras, lo que aprendiste sobre Git esta semana. El equipo evalúa que sepas trabajar con el repositorio correctamente, no solo que escribas buen contenido.

---

## Instrucciones

antes de iniciar, ver los siguientes tutoriales en youtube:
https://www.youtube.com/watch?v=ppiARvOnP6M
https://www.youtube.com/watch?v=XpulbHt5zZ0

tener instalado visual studio code en la laptop
tener instalado GIT

### Paso 1 — Clonar y preparar el entorno

Si aún no tienes el repositorio en tu máquina:

```bash
git clone https://github.com/jobrrerac/inetum_data_engineer_bootcamp.git
cd inetum_data_engineer_bootcamp
```

Asegúrate de estar en `develop` y tenerlo actualizado:

```bash
git checkout develop
git pull origin develop
```

---

### Paso 2 — Crear tu rama de trabajo

Crea una rama con el siguiente formato:

```
feature/semana01-git-<tu-nombre>
```

Ejemplo:

```bash
git checkout -b feature/semana01-git-maria
```

---

### Paso 3 — Crear tu carpeta y archivo de entrega

Dentro de la carpeta `semana_01/actividades/actividad_01/` crea **una carpeta con tu nombre** y dentro de ella tu archivo de entrega:

```
semana_01/actividades/actividad_01/<tu-nombre>/aprendizajes_<tu-nombre>.md
```

Ejemplo: `semana_01/actividades/actividad_01/maria/aprendizajes_maria.md`

> Esta convención se usará en todas las actividades del bootcamp: cada Data Engineer entrega su trabajo dentro de su propia carpeta.

El archivo debe tener al menos esta estructura inicial:

```markdown
# Aprendizajes Git — <Tu Nombre>

## ¿Qué es Git y para qué sirve?

(Escribe aquí con tus propias palabras)

## Comandos que aprendí

(Lista los comandos vistos en clase)

## ¿Qué es una rama y por qué se usa?

(Explica con tus palabras)

## Dudas o dificultades

(Opcional pero valorado)
```

---

### Paso 4 — Primer commit

Una vez creado el archivo con la estructura básica (aunque todavía vacío o incompleto):

```bash
git add semana_01/actividades/actividad_01/aprendizajes_<tu-nombre>.md
git commit -m "feat: create learning log for week 01 - <tu-nombre>"
```

---

### Paso 5 — Completar el documento y hacer commits progresivos

Completa cada sección del documento. Haz un commit **por cada sección que termines**. No hagas un solo commit con todo.

Ejemplos de commits esperados:

```bash
git commit -m "docs: add explanation of what Git is"
git commit -m "docs: add list of learned commands"
git commit -m "docs: add branch explanation"
git commit -m "docs: add doubts and difficulties section"
```

Se espera un mínimo de **3 commits** en la rama.

---

### Paso 6 — Subir la rama al repositorio remoto

```bash
git push origin feature/semana01-git-<tu-nombre>
```

---

### Paso 7 — Abrir un Pull Request

1. Ve al repositorio en GitHub.
2. Abre un Pull Request desde tu rama `feature/semana01-git-<tu-nombre>` hacia `develop`.
3. Usa este formato para el título del PR:

   ```
   [Semana 01] Aprendizajes Git — <Tu Nombre>
   ```

4. En la descripción del PR incluye:
   - Qué hiciste en esta actividad
   - Qué fue lo más difícil
   - Un comando de Git que te pareció importante

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Rama creada con el formato correcto | `feature/semana01-git-<nombre>` | 20% |
| Mínimo 3 commits con mensajes claros | Formato Conventional Commits | 30% |
| Documento completo y en sus propias palabras | Contenido del archivo `.md` | 30% |
| PR abierto correctamente hacia `develop` | Título y descripción completos | 20% |

---

## Lo que NO se debe hacer

- No hacer un solo commit con todo el trabajo
- No trabajar directamente en `main` o `develop`
- No copiar y pegar el documento GITFLOW.md del repositorio
- No abrir el PR hacia `main`

---

## Referencia

Puedes apoyarte en el documento [GITFLOW.md](../../GITFLOW.md) en la raíz del repositorio para repasar los comandos y el flujo de trabajo.
