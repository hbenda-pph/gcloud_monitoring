# 🚀 Despliegue en Cloud Run - IAM Access Monitor

## Opciones de Despliegue

### Opción 1: Cloud Run (Recomendado) - Despliegue Manual

```bash
# Variables
export PROJECT_ID="pph-central"
export SERVICE_NAME="iam-access-monitor"
export REGION="us-central1"
export REGISTRY="cloud-run-services"

# Paso 1: Crear repositorio en Artifact Registry (si no existe)
gcloud artifacts repositories create ${REGISTRY} \
  --project=${PROJECT_ID} \
  --repository-format=docker \
  --location=${REGION} \
  --description="Cloud Run Services Repository"

# Paso 2: Construir y subir imagen
gcloud builds submit \
  --project=${PROJECT_ID} \
  --config=cloudbuild.iam_monitor.yaml \
  --substitutions=_SERVICE_NAME=${SERVICE_NAME},_REGION=${REGION}

# Paso 3: Verificar despliegue
gcloud run services describe ${SERVICE_NAME} \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --platform managed

# Paso 4: Obtener URL del servicio
gcloud run services describe ${SERVICE_NAME} \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --platform managed \
  --format='value(status.url)'
```

### Opción 2: Cloud Run - Despliegue Directo

```bash
# Desplegar sin Cloud Build (más rápido para desarrollo)
gcloud run deploy iam-access-monitor \
  --source . \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --cpu 1 \
  --timeout 3600 \
  --allow-unauthenticated \
  --set-env-vars ENVIRONMENT=production \
  --update-env-vars PYTHONUNBUFFERED=1
```

### Opción 3: Cloud Scheduler + Cloud Run (Sincronización Automática)

```bash
# Crear Cloud Scheduler job para sincronización diaria
gcloud scheduler jobs create http iam-sync-daily \
  --project=${PROJECT_ID} \
  --schedule="0 2 * * *" \
  --time-zone="America/New_York" \
  --http-method=GET \
  --uri="https://iam-access-monitor-xxxxx.run.app/sync-snapshot" \
  --oidc-service-account-email=iam-sync@${PROJECT_ID}.iam.gserviceaccount.com
```

## Configuración de Producción

### 1. Seguridad

```bash
# Crear service account específico
gcloud iam service-accounts create iam-monitor-sa \
  --display-name="IAM Access Monitor Service Account"

# Asignar roles necesarios
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member=serviceAccount:iam-monitor-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/bigquery.dataViewer

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member=serviceAccount:iam-monitor-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/logging.viewer

# Usar service account en Cloud Run
gcloud run services update iam-access-monitor \
  --project=${PROJECT_ID} \
  --service-account=iam-monitor-sa@${PROJECT_ID}.iam.gserviceaccount.com
```

### 2. Autenticación

```bash
# Restringir acceso por IAM (solo usuarios autorizados)
gcloud run services add-iam-policy-binding iam-access-monitor \
  --project=${PROJECT_ID} \
  --member=group:data-engineers@company.com \
  --role=roles/run.invoker

# Verificar acceso
gcloud run services describe iam-access-monitor \
  --show-bindings \
  --project=${PROJECT_ID}
```

### 3. Monitoreo

```bash
# Ver logs en tiempo real
gcloud run services logs read iam-access-monitor \
  --follow \
  --project=${PROJECT_ID} \
  --region=us-central1

# Crear alertas en Cloud Monitoring
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="IAM Monitor Error Rate" \
  --condition-display-name="High Error Rate" \
  --condition-threshold-value=10 \
  --condition-threshold-duration=300s
```

### 4. Custom Domain

```bash
# Mapear dominio personalizado
gcloud beta run domain-mappings create \
  --service=iam-access-monitor \
  --domain=iam-monitor.company.com \
  --region=us-central1 \
  --project=${PROJECT_ID}

# Configurar DNS (CNAME)
# iam-monitor.company.com -> ghs.googleusercontent.com
```

## Variables de Entorno - Producción

```yaml
# .env.production
ENVIRONMENT=production
STREAMLIT_SERVER_PORT=8080
STREAMLIT_SERVER_ADDRESS=0.0.0.0
STREAMLIT_CLIENT_SHOWSTDERR=false
STREAMLIT_LOGGER_LEVEL=info
STREAMLIT_THEME_BASE=light

# BigQuery
GCP_PROJECT=pph-central
GOOGLE_CLOUD_PROJECT=pph-central
AUDIT_DATASET=management

# Logging
LOG_LEVEL=INFO
```

## Monitoreo y Alertas

### Métricas Importantes

```bash
# 1. Disponibilidad del servicio
gcloud monitoring metrics-descriptors list \
  --filter="metric.type:run.googleapis.com/*"

# 2. Latencia
gcloud monitoring time-series list \
  --filter='metric.type="run.googleapis.com/request_latencies"'

# 3. Errores
gcloud monitoring time-series list \
  --filter='resource.type="cloud_run_revision" AND metric.type="run.googleapis.com/request_count" AND metric.response_code_class="5xx"'
```

### Dashboard en Cloud Monitoring

```bash
# Ver dashboard
gcloud monitoring dashboards list

# Crear dashboard personalizado
gcloud monitoring dashboards create --config='{
  "displayName": "IAM Monitor Dashboard",
  "dashboardFilters": [],
  "gridLayout": {
    "widgets": [
      {
        "title": "Request Rate",
        "xyChart": {
          "dataSets": [{
            "timeSeriesQuery": {
              "timeSeriesFilter": {
                "filter": "resource.type=\"cloud_run_revision\" resource.label.service_name=\"iam-access-monitor\""
              }
            }
          }]
        }
      }
    ]
  }
}'
```

## Troubleshooting en Producción

### Error: Service Unavailable

```bash
# Verificar estado del servicio
gcloud run services describe iam-access-monitor \
  --format='value(status.conditions[0].message)'

# Ver logs
gcloud run logs read iam-access-monitor --limit=50
```

### Error: Permission Denied (BigQuery)

```bash
# Verificar permisos del service account
gcloud projects get-iam-policy ${PROJECT_ID} \
  --flatten="bindings[].members" \
  --filter="bindings.members:iam-monitor-sa*"

# Agregar permisos si falta
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member=serviceAccount:iam-monitor-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/bigquery.admin
```

### Error: Timeout

```bash
# Aumentar timeout en Cloud Run
gcloud run services update iam-access-monitor \
  --timeout 3600 \
  --region us-central1

# Optimizar consultas SQL si es lenta la sincronización
```

## Sincronización Automática

### Cloud Scheduler + Cloud Functions

Alternativa a scheduler simple - trigger personalizado:

```bash
# Crear función para sincroniar
gcloud functions deploy sync-iam \
  --runtime python39 \
  --trigger-topic sync-iam-topic \
  --entry-point sync_main \
  --source . \
  --service-account iam-monitor-sa@${PROJECT_ID}.iam.gserviceaccount.com

# Publicar mensaje para triggear sync
gcloud pubsub topics publish sync-iam-topic --message "sync"
```

## Costos Estimados (USD/mes)

```
Cloud Run Compute:
- 2GB memory, 1 CPU
- ~100 invocaciones/día
- Promedio 200ms por invocación
- Costo: ~$5-10 USD/mes

Cloud Logging:
- ~100 MB logs/mes
- Costo: Incluido primeros 50GB

BigQuery Queries:
- ~1 TB escaneo/día (análisis)
- ~100 MB insertados/día (snapshots)
- Costo: ~$100-200 USD/mes

Total Estimado: $105-210 USD/mes
```

## Post-Despliegue

### Verificación

```bash
# 1. Dashboard accesible
curl -I https://iam-monitor-xxxxx.run.app

# 2. Permisos de lectura correctos
# Ir a dashboard y seleccionar ambiente

# 3. Base de datos de auditoría
bq query "SELECT COUNT(*) FROM pph-central.management.iam_access_snapshot"

# 4. Logs sin errores
gcloud run logs read --limit=10 --filter="severity=ERROR"
```

### Documentación para Usuarios

```markdown
# 🔐 IAM Monitor - Acceso en Producción

**URL**: https://iam-monitor.company.com

**Requisitos**:
- Ser miembro de grupo `data-engineers@company.com`

**Funcionalidades**:
- Ver quién tiene acceso a cada dataset
- Filtrar por ambiente (dev/qua/pro)
- Descargar reportes

**Soporte**: platform-engineering@company.com
```

---

**Última actualización**: Marzo 2026
