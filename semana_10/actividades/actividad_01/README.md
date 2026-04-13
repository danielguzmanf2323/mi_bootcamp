# Actividad 01 — Semana 10: ADLS Gen2, el Sistema de Archivos de Datos en Azure

**Semana:** 10  
**Tema:** Azure Data Lake Storage Gen2 — estructura, acceso y montaje en Databricks  
**Nivel:** Intermedio  
**Modalidad:** Individual  
**Entorno:** Azure Portal + Databricks Enterprise

---

## Objetivo

Entender ADLS Gen2 como la capa de almacenamiento sobre la que corren Databricks y Fabric
en Azure: cómo se organiza, cómo se controla el acceso, y cómo se conecta a Databricks
de forma segura. Al terminar, sabrás montar un storage externo en Databricks
y estructurar un Data Lake siguiendo convenciones de producción.

---

## Dataset

Los archivos CSV del dataset Financial Transactions.  
En esta actividad los subes tú al storage — eso es parte del ejercicio.

---

## Material de estudio previo

1. ¿Qué diferencia hay entre **Azure Blob Storage** y **ADLS Gen2**? ¿Qué añade el Hierarchical Namespace?
2. ¿Qué es un **contenedor** en ADLS Gen2? ¿Cómo se relaciona con el concepto de bucket en S3?
3. ¿Qué son las **ACLs** en ADLS Gen2 y en qué se diferencian del RBAC de Azure?
4. ¿Qué es el protocolo **ABFS** (`abfss://`) y por qué Databricks lo usa en lugar de `wasbs://`?

---

## Instrucciones Git

```bash
git checkout develop
git pull origin develop
git checkout -b feature/semana10-adls-<tu-nombre>
```

Crea tu carpeta de entrega:
```
semana_10/actividades/actividad_01/<tu-nombre>/
```

El entregable es un archivo `notas_<tu-nombre>.md` con tus observaciones y capturas.
No hay notebook de código en esta actividad — el trabajo es en el Portal de Azure y en Databricks.

---

## Parte 1 — Explorar el storage account existente

El instructor habrá creado un Storage Account en Azure para esta semana.
Accede al Azure Portal y localiza el recurso.

En la sección **Containers**, identifica:
- ¿Qué contenedores existen? (`landing`, `bronze`, `silver`, `gold`)
- ¿Qué tipo de acceso tiene cada uno? (Private, Blob, Container)
- ¿Está habilitado el **Hierarchical Namespace**? ¿Cómo lo verificas?

En la sección **Access Control (IAM)**:
- ¿Qué roles están asignados al storage?
- ¿Qué rol necesita una aplicación para leer datos? ¿Y para escribir?

Documenta con capturas en tu `notas_<tu-nombre>.md`.

Commit esperado:
```bash
git commit -m "docs: explore storage account structure and IAM in Azure Portal"
```

---

## Parte 2 — Estructura del Data Lake

Un Data Lake bien organizado sigue convenciones consistentes de naming.
Crea esta estructura dentro del contenedor `landing/`:

```
landing/
└── raw/
    └── financial_transactions/
        ├── transactions/
        │   └── transactions_data.csv
        ├── customers/
        │   └── customers.json
        └── ...
```

Sube los archivos del dataset Financial Transactions usando el Portal de Azure
o el CLI de Azure (`az storage blob upload`):

```bash
az storage blob upload \
  --account-name <storage_account_name> \
  --container-name landing \
  --name raw/financial_transactions/transactions/transactions_data.csv \
  --file /local/path/transactions_data.csv \
  --auth-mode login
```

Justifica en tu documento:
- ¿Por qué separar por fuente (`financial_transactions/`) y no mezclar todos los archivos en `raw/`?
- ¿Qué convención de naming seguirías para versiones del mismo archivo? (ej. llegadas diarias)

Commit esperado:
```bash
git commit -m "docs: document data lake folder structure and naming conventions"
```

---

## Parte 3 — Montar ADLS en Databricks con Service Principal

Conectar Databricks a ADLS de forma segura requiere un Service Principal
con acceso al storage. El instructor habrá creado el SP y guardado sus credenciales en Key Vault.

```python
# Acceder a las credenciales desde el Secret Scope de Databricks
# (configurado por el instructor para apuntar al Key Vault)
client_id     = dbutils.secrets.get(scope="azure-kv", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="azure-kv", key="sp-client-secret")
tenant_id     = dbutils.secrets.get(scope="azure-kv", key="tenant-id")
storage_name  = dbutils.secrets.get(scope="azure-kv", key="storage-account-name")

# Configurar el acceso OAuth en Spark
spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Verificar acceso
files = dbutils.fs.ls(f"abfss://landing@{storage_name}.dfs.core.windows.net/raw/")
display(files)
```

Documenta:
- ¿Qué pasaría si el Service Principal no tiene el rol `Storage Blob Data Reader`?
- ¿Por qué se usa `abfss://` (con doble s) y no `abfs://`?

Commit esperado:
```bash
git commit -m "docs: ADLS mount via service principal and OAuth configuration"
```

---

## Parte 4 — Leer desde ADLS y comparar con Volumes

Con el storage montado, lee los archivos del dataset:

```python
storage_name = dbutils.secrets.get(scope="azure-kv", key="storage-account-name")
base_path = f"abfss://landing@{storage_name}.dfs.core.windows.net/raw/financial_transactions"

df_transactions = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(transactions_schema)
    .load(f"{base_path}/transactions/")
)

print(f"Registros leídos desde ADLS: {df_transactions.count()}")
```

Compara con leer el mismo archivo desde un Volumen de Unity Catalog:

```python
df_from_volume = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(transactions_schema)
    .load("/Volumes/main/landing/raw/transactions/")
)
print(f"Registros leídos desde Volume: {df_from_volume.count()}")
```

Documenta la diferencia entre acceder a ADLS directamente vs a través de Volumes de Unity Catalog.
¿Cuál recomendarías en un entorno Enterprise con Unity Catalog habilitado?

Commit esperado:
```bash
git commit -m "docs: compare direct ADLS access vs Unity Catalog Volumes"
```

---

## Entrega en Git

```bash
git push origin feature/semana10-adls-<tu-nombre>
```

PR hacia `develop` con título:
```
[Semana 10] ADLS Gen2 — <Tu Nombre>
```

---

## Criterios de evaluación

| Criterio | Descripción | Puntaje |
|----------|-------------|---------|
| [Requerido] Estructura del Data Lake documentada con justificación | Convenciones de naming explicadas | 25% |
| [Requerido] Montaje con Service Principal funcionando | Lectura de archivos verificada | 30% |
| [Requerido] Comparativa ADLS directo vs Volumes documentada | Con recomendación justificada | 25% |
| [Recomendado] Análisis de roles IAM en el Portal | Qué rol mínimo necesita cada operación | 20% |
| **Total** | | **100%** |

---

## Referencias

- [Azure Data Lake Storage Gen2 Introduction (Microsoft docs)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Access Azure Data Lake Storage from Databricks](https://docs.databricks.com/storage/azure-storage.html)
- [Azure Storage access control](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control)
