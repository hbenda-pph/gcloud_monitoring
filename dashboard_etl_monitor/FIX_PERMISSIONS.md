# Solución de Permisos para Sync Job

## Error: Permission 'iam.serviceaccounts.actAs' denied

Si ves este error, tienes dos opciones:

### Opción 1: Agregar Permisos a tu Usuario (Recomendado)

Ejecuta este comando para darle permisos a tu usuario para usar la service account:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  etl-servicetitan@pph-central.iam.gserviceaccount.com \
  --member="user:gcloud@peachcfo.com" \
  --role="roles/iam.serviceAccountUser" \
  --project=pph-central
```

**Nota:** Necesitas tener permisos de `iam.serviceAccounts.setIamPolicy` o que un administrador ejecute este comando por ti.

### Opción 2: Usar Service Account Diferente

Si no puedes obtener los permisos, puedes usar una service account diferente o la default del proyecto:

```bash
# Opción A: Usar service account diferente (reemplaza con una que tengas permisos)
export SYNC_JOB_SERVICE_ACCOUNT="tu-service-account@pph-central.iam.gserviceaccount.com"
./deploy_sync_job.sh

# Opción B: No usar service account (usará la default del proyecto)
export SYNC_JOB_SERVICE_ACCOUNT=""
./deploy_sync_job.sh
```

### Opción 3: Usar Compute Engine Default Service Account

Si no especificas service account, Cloud Run usará la default del proyecto:

```bash
# Modificar deploy_sync_job.sh y cambiar:
SERVICE_ACCOUNT=""

# O ejecutar:
SYNC_JOB_SERVICE_ACCOUNT="" ./deploy_sync_job.sh
```

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
