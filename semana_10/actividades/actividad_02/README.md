# Actividad 02 — Semana 10: Azure IAM para Datos

**Semana:** 10  
**Tema:** Service Principals, Managed Identities, Key Vault y RBAC en pipelines de datos  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Azure Portal + Databricks Enterprise

---

## Objetivo

Entender cómo se gestiona la identidad y el acceso en un entorno de datos Azure real:
la diferencia entre un Service Principal y una Managed Identity, cómo Key Vault
centraliza los secretos, y cómo RBAC aplica de forma consistente a través de
storage, Unity Catalog y Fabric. La seguridad no es opcional en producción.

---

## Material de estudio previo

1. ¿Qué es un **Service Principal** en Azure AD? ¿En qué se parece a una cuenta de servicio en Linux?
2. ¿Qué ventaja tiene una **Managed Identity** sobre un Service Principal?  ¿Por qué no necesita secretos?
3. ¿Qué es el **principio de mínimo privilegio** y cómo se aplica al acceso a un Data Lake?
4. ¿Qué diferencia hay entre un **Secret Scope** de Databricks backed by Azure Key Vault vs uno backed by Databricks?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-iam-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_10/actividades/actividad_02/<tu-nombre>/
notas_<tu-nombre>.md
```

---

## Parte 1 — Explorar el Service Principal del entorno

El instructor habrá creado un Service Principal para el entorno de datos.
En Azure Portal → Azure Active Directory → App registrations, localiza la aplicación.

Documenta:
- ¿Qué es el `Application (client) ID`? ¿Y el `Directory (tenant) ID`?
- ¿Dónde se generan los **Client Secrets**? ¿Cuánto tiempo de vida tienen?
- ¿A qué recursos tiene acceso este Service Principal? (ver Certificates & secrets y los role assignments en el storage)

Responde: ¿qué pasaría si el Client Secret expira en producción sin haberlo renovado?

Commit esperado:
```bash
git commit -m "docs: explore service principal configuration and risks"
```

---

## Parte 2 — Azure Key Vault: gestión centralizada de secretos

Key Vault es el lugar correcto para guardar credenciales. Nunca en notebooks, nunca en repos.

En Azure Portal → Key Vault, explora el recurso del entorno:

```bash
# Ver los secretos disponibles (desde Azure CLI)
az keyvault secret list --vault-name <keyvault-name> --query "[].name" -o tsv

# Ver el valor de un secreto (con los permisos adecuados)
az keyvault secret show --vault-name <keyvault-name> --name sp-client-id --query value -o tsv
```

Desde Databricks, accede a los secretos vía Secret Scope:

```python
# Listar los scopes disponibles
dbutils.secrets.listScopes()

# Listar los secretos en el scope del Key Vault
dbutils.secrets.list(scope="azure-kv")

# Acceder a un secreto — nota que el valor no se muestra en el output
client_id = dbutils.secrets.get(scope="azure-kv", key="sp-client-id")
print(client_id)  # output: [REDACTED] — nunca se muestra en logs
```

Documenta:
- ¿Por qué Databricks muestra `[REDACTED]` en lugar del valor real?
- ¿Qué diferencia hay entre un secret scope backed by Key Vault vs uno Databricks-native?
- ¿Qué rol necesita el Service Principal para leer secretos del Key Vault?

Commit esperado:
```bash
git commit -m "docs: Key Vault secret scopes and access from Databricks"
```

---

## Parte 3 — RBAC: mínimo privilegio en la práctica

El principio de mínimo privilegio dice: dar exactamente el acceso necesario, ni más.

Completa esta tabla para el entorno de datos que estás usando:

| Componente | Identidad | Rol asignado | ¿Es el mínimo necesario? |
|------------|-----------|-------------|--------------------------|
| Storage Account (landing) | Service Principal | Storage Blob Data Reader | ? |
| Storage Account (bronze/silver/gold) | Service Principal | Storage Blob Data Contributor | ? |
| Unity Catalog (schema bronze) | Tu usuario | ? | ? |
| Unity Catalog (schema gold) | Analista (read-only) | ? | ? |
| Key Vault | Service Principal | Key Vault Secrets User | ? |

Para cada fila, justifica si el rol es correcto o si lo reducirías/ampliarías.

Ejemplo de cómo verificar los role assignments desde CLI:
```bash
az role assignment list \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<name> \
  --output table
```

Commit esperado:
```bash
git commit -m "docs: RBAC audit - minimum privilege analysis for data environment"
```

---

## Parte 4 — Managed Identity vs Service Principal

Lee la documentación sobre Managed Identities y responde en tu documento:

| Criterio | Service Principal | Managed Identity |
|----------|------------------|-----------------|
| Necesita gestionar secretos | Sí | No |
| Puede usarse en Databricks (cluster) | Sí | Sí (con configuración) |
| Puede usarse desde local/laptop | Sí | No |
| Riesgo si las credenciales se filtran | Alto | Ninguno (no hay credenciales) |
| Cuándo usarlo | | |

Investiga: ¿cómo configura Databricks la Managed Identity del cluster para acceder a ADLS sin Service Principal?

Commit esperado:
```bash
git commit -m "docs: service principal vs managed identity comparison"
```

---

## Entrega en Git

```bash
git push origin feature/semana10-iam-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] Azure IAM para Datos — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Service Principal explorado y documentado | Client ID, riesgos de expiración documentados | 20% |
| [Requerido] Key Vault + Secret Scope funcionando desde Databricks | `[REDACTED]` explicado correctamente | 25% |
| [Requerido] Tabla RBAC completada con justificación | Mínimo privilegio analizado por componente | 30% |
| [Recomendado] Comparativa SP vs Managed Identity | Tabla completa con recomendación de uso | 25% |
| **Total** | | **100%** |

---

## Referencias

- [Azure AD Service Principals (Microsoft docs)](https://learn.microsoft.com/en-us/azure/active-directory/develop/app-objects-and-service-principals)
- [Managed Identities for Azure resources](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview)
- [Databricks Secret Scopes backed by Azure Key Vault](https://docs.databricks.com/security/secrets/secret-scopes.html)
