#!/bin/bash

# =============================================================================
# SCRIPT DE DEPLOY PARA SYNC JOB (Cloud Run Job + Cloud Scheduler)
# Multi-Environment: DEV, QUA, PRO
# Actualiza last_etl_synced y row_count en companies_consolidated
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
        echo "Uso: ./deploy_sync_job.sh [dev|qua|pro]"
        echo ""
        echo "Ejemplos:"
        echo "  ./deploy_sync_job.sh dev    # Deploy en DEV (platform-partners-des)"
        echo "  ./deploy_sync_job.sh des    # Deploy en DEV (alias de 'dev')"
        echo "  ./deploy_sync_job.sh qua    # Deploy en QUA (platform-partners-qua)"
        echo "  ./deploy_sync_job.sh pro    # Deploy en PRO (platform-partners-pro)"
        echo ""
        echo "O ejecuta sin parámetros para usar el proyecto activo de gcloud"
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
# Determinar sufijo para el nombre de la imagen
case "$ENVIRONMENT" in
    dev)
        ENV_SUFFIX="-dev"
        ;;
    qua)
        ENV_SUFFIX="-qua"
        ;;
    pro)
        ENV_SUFFIX=""
        ;;
esac
IMAGE_NAME="gcr.io/${PROJECT_ID}/update-companies-consolidated-sync${ENV_SUFFIX}"
CENTRAL_PROJECT="pph-central"  # Proyecto donde están los datos

echo "🚀 DEPLOY SYNC JOB"
echo "=================="
echo "🌍 Ambiente: ${ENVIRONMENT^^}"
echo "📊 Proyecto: ${PROJECT_ID}"
echo "📦 Job: ${JOB_NAME}"
echo "🔐 Service Account: ${SERVICE_ACCOUNT}"
echo "💾 Datos en: ${CENTRAL_PROJECT}"
echo ""

# Paso 1: Build de imagen
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "========================================="
# Determinar sufijo para el nombre de la imagen
case "$ENVIRONMENT" in
    dev)
        ENV_SUFFIX="-dev"
        ;;
    qua)
        ENV_SUFFIX="-qua"
        ;;
    pro)
        ENV_SUFFIX=""
        ;;
esac

gcloud builds submit --config=cloudbuild.sync.yaml \
  --project=${PROJECT_ID} \
  --substitutions=_PROJECT_ID=${PROJECT_ID},_ENV_SUFFIX=${ENV_SUFFIX}

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
    if [ -n "${SERVICE_ACCOUNT}" ]; then
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
        echo "⚠️  No se especificó SERVICE_ACCOUNT, usando la default del proyecto"
        gcloud run jobs update ${JOB_NAME} \
            --image ${IMAGE_NAME} \
            --region ${REGION} \
            --project ${PROJECT_ID} \
            --max-retries 3 \
            --task-timeout 600s \
            --memory 2Gi \
            --cpu 2
    fi
else
    echo "🆕 Job no existe, creando..."
    if [ -n "${SERVICE_ACCOUNT}" ]; then
        gcloud run jobs create ${JOB_NAME} \
            --image ${IMAGE_NAME} \
            --region ${REGION} \
            --project ${PROJECT_ID} \
            --service-account ${SERVICE_ACCOUNT} \
            --max-retries 3 \
            --task-timeout 600s \
            --memory 2Gi \
            --cpu 2
    else
        echo "⚠️  No se especificó SERVICE_ACCOUNT, usando la default del proyecto"
        gcloud run jobs create ${JOB_NAME} \
            --image ${IMAGE_NAME} \
            --region ${REGION} \
            --project ${PROJECT_ID} \
            --max-retries 3 \
            --task-timeout 600s \
            --memory 2Gi \
            --cpu 2
    fi
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
