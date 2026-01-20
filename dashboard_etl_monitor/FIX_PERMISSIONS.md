# Solución de Permisos para Sync Job

## Error: Service Account no existe o Permission 'iam.serviceaccounts.actAs' denied

### Paso 0: Verificar Service Accounts Existentes

Primero, verifica qué service accounts existen en el proyecto:

```bash
gcloud iam service-accounts list --project=pph-central
```

### Opción 1: Crear Nueva Service Account (Recomendado)

Si la service account `etl-servicetitan@pph-central.iam.gserviceaccount.com` no existe, créala:

```bash
# Crear la service account
gcloud iam service-accounts create etl-servicetitan \
  --display-name="ETL ServiceTitan Sync Job" \
  --description="Service account para actualizar companies_consolidated" \
  --project=pph-central

# Darle permisos de BigQuery
gcloud projects add-iam-policy-binding pph-central \
  --member="serviceAccount:etl-servicetitan@pph-central.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# Darle permisos para leer tablas en otros proyectos (necesario para leer bronze)
# Esto debe hacerse en cada proyecto de compañía (dev, qua, pro)
gcloud projects add-iam-policy-binding platform-partners-des \
  --member="serviceAccount:etl-servicetitan@pph-central.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding platform-partners-qua \
  --member="serviceAccount:etl-servicetitan@pph-central.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding constant-height-455614-i0 \
  --member="serviceAccount:etl-servicetitan@pph-central.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# Darle permisos a tu usuario para usar esta service account
gcloud iam service-accounts add-iam-policy-binding \
  etl-servicetitan@pph-central.iam.gserviceaccount.com \
  --member="user:gcloud@peachcfo.com" \
  --role="roles/iam.serviceAccountUser" \
  --project=pph-central
```

**Nota:** Necesitas tener permisos de `iam.serviceAccounts.create` y `iam.serviceAccounts.setIamPolicy` o que un administrador ejecute estos comandos por ti.

### Opción 2: Usar Service Account Existente

Si ya existe una service account con los permisos necesarios, úsala:

```bash
# Listar service accounts disponibles
gcloud iam service-accounts list --project=pph-central

# Usar una service account existente (reemplaza con el email real)
export SYNC_JOB_SERVICE_ACCOUNT="tu-service-account-existente@pph-central.iam.gserviceaccount.com"
./deploy_sync_job.sh
```

### Opción 3: Usar Service Account Default del Proyecto

Si no especificas service account, Cloud Run usará la default del proyecto (Compute Engine default):

```bash
# No usar service account específica (usará la default)
export SYNC_JOB_SERVICE_ACCOUNT=""
./deploy_sync_job.sh
```

**Nota:** Asegúrate de que la service account default tenga permisos de BigQuery Data Editor en `pph-central` y BigQuery Data Viewer en los proyectos de compañías.

## Verificar Permisos

Para verificar qué service accounts puedes usar:

```bash
gcloud iam service-accounts list --project=pph-central
```

Para ver los permisos de una service account específica:

```bash
gcloud projects get-iam-policy pph-central \
  --flatten="bindings[].members" \
  --filter="bindings.members:etl-servicetitan@pph-central.iam.gserviceaccount.com"
```
