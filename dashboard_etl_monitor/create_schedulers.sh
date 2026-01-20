#!/bin/bash

# =============================================================================
# CREAR CLOUD SCHEDULERS PARA SYNC JOB
# Multi-Environment: DEV, QUA, PRO
# =============================================================================

set -e  # Salir si hay algún error

# =============================================================================
# CONFIGURACIÓN DE AMBIENTES
# =============================================================================

# Detectar proyecto activo de gcloud
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

# Si se proporciona parámetro, usarlo; si no, detectar automáticamente
if [ -n "$1" ]; then
    # Parámetro proporcionado explícitamente
    ENVIRONMENT="$1"
    ENVIRONMENT=$(echo "$ENVIRONMENT" | tr '[:upper:]' '[:lower:]')  # Convertir a minúsculas
    
    # Validar ambiente (aceptar "des" como alias de "dev")
    if [[ "$ENVIRONMENT" == "des" ]]; then
        ENVIRONMENT="dev"
        echo "ℹ️  'des' interpretado como 'dev'"
    fi
    
    # Validar ambiente
    if [[ ! "$ENVIRONMENT" =~ ^(dev|qua|pro)$ ]]; then
        echo "❌ Error: Ambiente inválido '$ENVIRONMENT'"
        echo "Uso: ./create_schedulers.sh [dev|qua|pro]"
        exit 1
    fi
else
    # Detectar automáticamente según el proyecto activo
    echo "🔍 Detectando ambiente desde proyecto activo de gcloud..."
    
    case "$CURRENT_PROJECT" in
        platform-partners-des)
            ENVIRONMENT="dev"
            echo "✅ Detectado: DEV (platform-partners-des)"
            ;;
        platform-partners-qua)
            ENVIRONMENT="qua"
            echo "✅ Detectado: QUA (platform-partners-qua)"
            ;;
        constant-height-455614-i0)
            ENVIRONMENT="pro"
            echo "✅ Detectado: PRO (platform-partners-pro)"
            ;;
        *)
            echo "⚠️  Proyecto activo: ${CURRENT_PROJECT}"
            echo "⚠️  No se reconoce el proyecto. Usando QUA por defecto."
            ENVIRONMENT="qua"
            ;;
    esac
fi

# Configuración según ambiente
case "$ENVIRONMENT" in
    dev)
        PROJECT_ID="platform-partners-des"
        JOB_NAME="update-companies-consolidated-sync-dev"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        JOB_NAME="update-companies-consolidated-sync-qua"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="constant-height-455614-i0"
        JOB_NAME="update-companies-consolidated-sync"
        SERVICE_ACCOUNT="etl-servicetitan@constant-height-455614-i0.iam.gserviceaccount.com"
        ;;
esac

REGION="us-east1"

# Agregar sufijo de ambiente a los nombres de schedulers
SCHEDULER_NAMES=(
    "sync-companies-consolidated-7am-${ENVIRONMENT}"
    "sync-companies-consolidated-1pm-${ENVIRONMENT}"
    "sync-companies-consolidated-7pm-${ENVIRONMENT}"
    "sync-companies-consolidated-1am-${ENVIRONMENT}"
)

CRON_SCHEDULES=(
    "0 7 * * *"   # 7am
    "0 13 * * *"  # 1pm
    "0 19 * * *"  # 7pm
    "0 1 * * *"   # 1am
)

echo "⏰ CREANDO CLOUD SCHEDULERS"
echo "==========================="
echo "🌍 Ambiente: ${ENVIRONMENT^^}"
echo "📊 Proyecto: ${PROJECT_ID}"
echo "📦 Job: ${JOB_NAME}"
echo "🔐 Service Account: ${SERVICE_ACCOUNT}"
echo ""

for i in "${!SCHEDULER_NAMES[@]}"; do
    SCHEDULER_NAME=${SCHEDULER_NAMES[$i]}
    CRON_SCHEDULE=${CRON_SCHEDULES[$i]}
    
    echo "📅 Creando scheduler: ${SCHEDULER_NAME} (${CRON_SCHEDULE})"
    
    # Verificar si existe
    if gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION} --project=${PROJECT_ID} &> /dev/null; then
        echo "   📝 Actualizando scheduler existente..."
        if [ -n "${SERVICE_ACCOUNT}" ]; then
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
            echo "   ⚠️  No se especificó SERVICE_ACCOUNT, usando la default del proyecto"
            gcloud scheduler jobs update http ${SCHEDULER_NAME} \
                --location=${REGION} \
                --project=${PROJECT_ID} \
                --schedule="${CRON_SCHEDULE}" \
                --time-zone="America/New_York" \
                --http-method=POST \
                --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
                --quiet
        fi
    else
        echo "   🆕 Creando nuevo scheduler..."
        if [ -n "${SERVICE_ACCOUNT}" ]; then
            gcloud scheduler jobs create http ${SCHEDULER_NAME} \
                --location=${REGION} \
                --project=${PROJECT_ID} \
                --schedule="${CRON_SCHEDULE}" \
                --time-zone="America/New_York" \
                --http-method=POST \
                --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
                --oauth-service-account-email=${SERVICE_ACCOUNT} \
                --quiet
        else
            echo "   ⚠️  No se especificó SERVICE_ACCOUNT, usando la default del proyecto"
            gcloud scheduler jobs create http ${SCHEDULER_NAME} \
                --location=${REGION} \
                --project=${PROJECT_ID} \
                --schedule="${CRON_SCHEDULE}" \
                --time-zone="America/New_York" \
                --http-method=POST \
                --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
                --quiet
        fi
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
