#!/bin/bash

# =============================================================================
# SCRIPT DE DEPLOY PARA SYNC JOB (Cloud Run Job + Cloud Scheduler)
# Actualiza last_etl_synced y row_count en companies_consolidated
# =============================================================================

set -e

# Configuración
PROJECT_ID="pph-central"
REGION="us-east1"
JOB_NAME="update-companies-consolidated-sync"
SERVICE_ACCOUNT="etl-servicetitan@pph-central.iam.gserviceaccount.com"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${JOB_NAME}"

echo "🚀 DEPLOY SYNC JOB"
echo "=================="
echo "Proyecto: ${PROJECT_ID}"
echo "Job: ${JOB_NAME}"
echo ""

# Paso 1: Build de imagen
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "========================================="
gcloud builds submit --config=cloudbuild.sync.yaml --project=${PROJECT_ID}

if [ $? -eq 0 ]; then
    echo "✅ Build exitoso!"
else
    echo "❌ Error en el build"
    exit 1
fi

echo ""
echo "🚀 PASO 2: CREATE/UPDATE CLOUD RUN JOB"
echo "======================================="

# Verificar si el job ya existe
if gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} &> /dev/null; then
    echo "📝 Job existe, actualizando..."
    gcloud run jobs update ${JOB_NAME} \
        --image ${IMAGE_NAME} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --service-account ${SERVICE_ACCOUNT} \
        --max-retries 3 \
        --task-timeout 600s \
        --memory 2Gi \
        --cpu 2
else
    echo "🆕 Job no existe, creando..."
    gcloud run jobs create ${JOB_NAME} \
        --image ${IMAGE_NAME} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --service-account ${SERVICE_ACCOUNT} \
        --max-retries 3 \
        --task-timeout 600s \
        --memory 2Gi \
        --cpu 2
fi

if [ $? -eq 0 ]; then
    echo "✅ Job creado/actualizado exitosamente!"
else
    echo "❌ Error creando/actualizando job"
    exit 1
fi

echo ""
echo "⏰ PASO 3: CONFIGURAR CLOUD SCHEDULER"
echo "======================================"
echo ""
echo "Ejecuta estos comandos para crear los 4 schedulers (7am, 1pm, 7pm, 1am):"
echo ""

# Crear schedulers para cada horario
SCHEDULER_NAMES=(
    "sync-companies-consolidated-7am"
    "sync-companies-consolidated-1pm"
    "sync-companies-consolidated-7pm"
    "sync-companies-consolidated-1am"
)

CRON_SCHEDULES=(
    "0 7 * * *"   # 7am
    "0 13 * * *"  # 1pm
    "0 19 * * *"  # 7pm
    "0 1 * * *"   # 1am
)

for i in "${!SCHEDULER_NAMES[@]}"; do
    SCHEDULER_NAME=${SCHEDULER_NAMES[$i]}
    CRON_SCHEDULE=${CRON_SCHEDULES[$i]}
    
    echo "# Scheduler para ${SCHEDULER_NAME}"
    
    # Verificar si existe
    if gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION} --project=${PROJECT_ID} &> /dev/null; then
        echo "gcloud scheduler jobs update ${SCHEDULER_NAME} \\"
        echo "    --location=${REGION} \\"
        echo "    --project=${PROJECT_ID} \\"
        echo "    --schedule=\"${CRON_SCHEDULE}\" \\"
        echo "    --time-zone=\"America/New_York\" \\"
        echo "    --http-method=POST \\"
        echo "    --uri=\"https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run\" \\"
        echo "    --oauth-service-account-email=${SERVICE_ACCOUNT}"
    else
        echo "gcloud scheduler jobs create http ${SCHEDULER_NAME} \\"
        echo "    --location=${REGION} \\"
        echo "    --project=${PROJECT_ID} \\"
        echo "    --schedule=\"${CRON_SCHEDULE}\" \\"
        echo "    --time-zone=\"America/New_York\" \\"
        echo "    --http-method=POST \\"
        echo "    --uri=\"https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run\" \\"
        echo "    --oauth-service-account-email=${SERVICE_ACCOUNT}"
    fi
    
    echo ""
done

echo ""
echo "🎉 ¡DEPLOY COMPLETADO!"
echo "======================"
echo ""
echo "📋 Próximos pasos:"
echo "1. Ejecuta los comandos de Cloud Scheduler mostrados arriba"
echo "2. O ejecuta este script para crear los schedulers automáticamente:"
echo "   ./create_schedulers.sh"
echo ""
