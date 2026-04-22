Aprendizajes Git — Daniel
¿Qué es Git y para qué sirve?
Git es un sistema de control de ve>rsiones que permite guardar cambios en el código a lo largo del tiempo. Sirve para trabajar en equipo, mantener historial de cambios y poder volver a versiones anteriores si algo falla.

Comandos que aprendí
Comandos que aprendí
git clone: sirve para copiar un repositorio remoto a tu computadora
git checkout: sirve para cambiar de rama
git checkout -b: crea una nueva rama y te cambia a ella
git add: agrega archivos al staging area
git commit: guarda los cambios con un mensaje
git push: sube los cambios al repositorio remoto
git pull: trae cambios del repositorio remoto
¿Qué es una rama y por qué se usa?
Una rama en Git es una copia del código en la que puedes trabajar sin afectar la versión principal. Se utiliza para desarrollar nuevas funcionalidades, hacer pruebas o corregir errores de forma aislada. Una vez validados los cambios, estos se integran nuevamente a la rama principal mediante un merge o pull request.

Dudas o dificultades
# Aprendizajes Git — Daniel

## ¿Qué es Git y para qué sirve?

Git es un sistema de control de versiones que permite guardar cambios en el código a lo largo del tiempo. Sirve para trabajar en equipo, mantener historial de cambios y poder volver a versiones anteriores si algo falla.

## Comandos que aprendí
## Comandos que aprendí

- git clone: sirve para copiar un repositorio remoto a tu computadora
- git checkout: sirve para cambiar de rama
- git checkout -b: crea una nueva rama y te cambia a ella
- git add: agrega archivos al staging area
- git commit: guarda los cambios con un mensaje
- git push: sube los cambios al repositorio remoto
- git pull: trae cambios del repositorio remoto


## ¿Qué es una rama y por qué se usa?

Una rama en Git es una copia del código en la que puedes trabajar sin afectar la versión principal. Se utiliza para desarrollar nuevas funcionalidades, hacer pruebas o corregir errores de forma aislada. Una vez validados los cambios, estos se integran nuevamente a la rama principal mediante un merge o pull request.

## Dudas o dificultades

Durante la actividad surgieron algunos inconvenientes relacionados con el manejo de ramas, específicamente cuando una referencia local se corrompió y fue necesario eliminar la rama y recrearla desde develop.

Esto permitió entender mejor cómo Git maneja internamente las referencias y la importancia de mantener un flujo de trabajo limpio. También fue útil para reforzar el uso de comandos como git checkout, git branch y git reset para recuperar el estado del repositorio sin perder avances.

En general, más que dificultades, fueron situaciones prácticas que ayudaron a consolidar el manejo de Git en escenarios reales.

Comandos que investigué
git stash
¿Para qué sirve?
Guarda los cambios temporalmente sin necesidad de hacer un commit. Ideal cuando se está trabajando en algo pero se necesita cambiar de contexto rápidamente.

Ejemplo de uso:


## Comandos que investigué

### git stash
**¿Para qué sirve?**  
Guarda los cambios temporalmente sin necesidad de hacer un commit. Ideal cuando se está trabajando en algo pero se necesita cambiar de contexto rápidamente.

**Ejemplo de uso:**  
```bash
git stash



Estrategias para deshacer cambios:

1. Restaurar cambios antes de hacer commit:

Comando: git restore <archivo>
Descripción: Permite restaurar un archivo a su última versión comprometida, descartando cualquier cambio no guardado.
Ejemplo de uso:

git restore semana_01/actividades/actividad_01/README.md


2. Deshacer el último commit sin perder los cambios:

Comando: git reset --soft HEAD~1
Descripción: Deshace el último commit pero mantiene los cambios en el área de staging para ser editados antes de hacer un nuevo commit.

Ejemplo de uso:

git reset --soft HEAD~1



3. Corregir el último commit:

Comando: git commit --amend
Descripción: Permite modificar el último  commit, ya sea para añadir cambios adicionales o corregir el mensaje de commit.

Ejemplo de uso:

git commit --amend
git commit --amend


