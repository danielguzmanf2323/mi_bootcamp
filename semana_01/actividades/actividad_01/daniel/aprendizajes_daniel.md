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


## Comandos que investigué

### git stash
**¿Para qué sirve?**  
Guarda los cambios temporalmente sin necesidad de hacer un commit. Ideal cuando se está trabajando en algo pero se necesita cambiar de contexto rápidamente.

**Ejemplo de uso:**  
```bash
git stash




esta es una pruebra para git diff 