#!/bin/bash

# =============================================================================
# CREAR CLOUD SCHEDULERS PARA SYNC JOB
# =============================================================================

set -e

# Configuración
PROJECT_ID="pph-central"
REGION="us-east1"
JOB_NAME="update-companies-consolidated-sync"
SERVICE_ACCOUNT="etl-servicetitan@pph-central.iam.gserviceaccount.com"

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

echo "⏰ CREANDO CLOUD SCHEDULERS"
echo "==========================="
echo ""

for i in "${!SCHEDULER_NAMES[@]}"; do
    SCHEDULER_NAME=${SCHEDULER_NAMES[$i]}
    CRON_SCHEDULE=${CRON_SCHEDULES[$i]}
    
    echo "📅 Creando scheduler: ${SCHEDULER_NAME} (${CRON_SCHEDULE})"
    
    # Verificar si existe
    if gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION} --project=${PROJECT_ID} &> /dev/null; then
        echo "   📝 Actualizando scheduler existente..."
        gcloud scheduler jobs update http ${SCHEDULER_NAME} \
            --location=${REGION} \
            --project=${PROJECT_ID} \
            --schedule="${CRON_SCHEDULE}" \
            --time-zone="America/New_York" \
            --http-method=POST \
            --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
            --oauth-service-account-email=${SERVICE_ACCOUNT} \
            --quiet
    else
        echo "   🆕 Creando nuevo scheduler..."
        gcloud scheduler jobs create http ${SCHEDULER_NAME} \
            --location=${REGION} \
            --project=${PROJECT_ID} \
            --schedule="${CRON_SCHEDULE}" \
            --time-zone="America/New_York" \
            --http-method=POST \
            --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
            --oauth-service-account-email=${SERVICE_ACCOUNT} \
            --quiet
    fi
    
    if [ $? -eq 0 ]; then
        echo "   ✅ ${SCHEDULER_NAME} configurado correctamente"
    else
        echo "   ❌ Error configurando ${SCHEDULER_NAME}"
    fi
    
    echo ""
done

echo "🎉 ¡TODOS LOS SCHEDULERS CONFIGURADOS!"
echo "======================================"
echo ""
echo "📋 Schedulers creados:"
for name in "${SCHEDULER_NAMES[@]}"; do
    echo "   - ${name}"
done
echo ""
echo "🔍 Para verificar:"
echo "   gcloud scheduler jobs list --location=${REGION} --project=${PROJECT_ID}"
echo ""
