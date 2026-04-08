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
https://m.youtube.com/watch?v=jGehuhFhtnE&pp=0gcJCdoKAYcqIYzv
https://m.youtube.com/watch?v=AYbgqmyg7dk&pp=0gcJCdoKAYcqIYzv


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
git add semana_01/actividades/actividad_01/<tu-nombre>/aprendizajes_<tu-nombre>.md
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

### Paso 6 — Investigar y documentar comandos adicionales

Agrega una sección en tu archivo `aprendizajes_<tu-nombre>.md` llamada **"Comandos que investigué"** y documenta los siguientes cuatro comandos: qué hacen, cuándo usarlos y un ejemplo concreto de uso.

| Comando | ¿Para qué sirve? |
|---------|------------------|
| `git stash` | Guardar cambios temporalmente sin commitear |
| `git diff` | Ver qué cambió antes de hacer commit |
| `git log --oneline --graph` | Ver el historial de commits de forma visual |
| `git revert <commit>` | Deshacer un commit sin borrar el historial |

Para cada uno:
1. Lee la documentación o busca un tutorial corto
2. Pruébalo en tu rama
3. Escribe en el documento: qué hiciste y qué pasó

Commit esperado:
```bash
git commit -m "docs: add additional git commands research"
```

---

### Paso 7 — Deshacer cambios de forma segura

Una de las habilidades más importantes en Git es saber deshacer sin romper nada. Investiga y documenta en tu archivo los siguientes tres escenarios. Para cada uno: explica qué hace el comando, pruébalo y describe el resultado.

**Escenario A — Descartar cambios que aún no están en staging:**
```bash
git restore nombre_del_archivo.md
```
> Pista: Modifica una línea de tu archivo, verifica con `git status`, luego ejecútalo y observa qué pasa.

**Escenario B — Deshacer el último commit pero conservar los cambios:**
```bash
git reset --soft HEAD~1
```
> Pista: Haz un commit de prueba con contenido temporal, luego ejecuta este comando. ¿Qué quedó en staging?

**Escenario C — Corregir el mensaje del último commit antes de hacer push:**
```bash
git commit --amend -m "mensaje corregido"
```
> Pista: Haz un commit con un mensaje malo a propósito, luego corrígelo con este comando.

Documenta en tu archivo qué aprendiste de cada escenario. Commit esperado:
```bash
git commit -m "docs: add undo strategies in git"
```

---

### Paso 8 — Subir la rama al repositorio remoto

```bash
git push origin feature/semana01-git-<tu-nombre>
```

---

### Paso 9 — Abrir un Pull Request

1. Ve al repositorio en GitHub.
2. Abre un Pull Request desde tu rama `feature/semana01-git-<tu-nombre>` hacia `develop`.
3. Usa este formato para el título del PR:

   ```
   [Semana 01] Aprendizajes Git — <Tu Nombre>
   ```

4. En la descripción del PR incluye:
   - Qué hiciste en esta actividad
   - Qué fue lo más difícil
   - Un comando de Git que te pareció más útil y por qué

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| Rama y carpeta con formato correcto | `feature/semana01-git-<nombre>` + carpeta `<nombre>/` | 15% |
| Mínimo 5 commits con mensajes claros | Formato Conventional Commits | 25% |
| Secciones base del documento completas | En sus propias palabras | 20% |
| Sección de comandos adicionales documentada | Con ejemplo y resultado real | 20% |
| Sección de estrategias para deshacer | Los 3 escenarios documentados | 20% |

---

## Lo que NO se debe hacer

- No hacer un solo commit con todo el trabajo
- No trabajar directamente en `main` o `develop`
- No copiar y pegar el documento GITFLOW.md del repositorio
- No abrir el PR hacia `main`

---

## Referencia

Puedes apoyarte en el documento [GITFLOW.md](../../GITFLOW.md) en la raíz del repositorio para repasar los comandos y el flujo de trabajo.
