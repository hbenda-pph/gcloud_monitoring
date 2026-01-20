#!/bin/bash

# =============================================================================
# SCRIPT PARA CONFIGURAR PERMISOS DE SERVICE ACCOUNTS
# Da permisos a las service accounts de dev/qua/pro para escribir en pph-central
# =============================================================================

set -e

CENTRAL_PROJECT="pph-central"

echo "🔐 CONFIGURANDO PERMISOS PARA SERVICE ACCOUNTS"
echo "=============================================="
echo ""

# Service accounts de cada proyecto
SERVICE_ACCOUNTS=(
    "etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
    "etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
    "etl-servicetitan@constant-height-455614-i0.iam.gserviceaccount.com"
)

ENVIRONMENTS=("DEV" "QUA" "PRO")

for i in "${!SERVICE_ACCOUNTS[@]}"; do
    SA="${SERVICE_ACCOUNTS[$i]}"
    ENV="${ENVIRONMENTS[$i]}"
    
    echo "📋 Configurando permisos para ${ENV}: ${SA}"
    
    # BigQuery Data Editor en pph-central (para escribir en companies_consolidated)
    echo "   ➕ Agregando BigQuery Data Editor en pph-central..."
    gcloud projects add-iam-policy-binding ${CENTRAL_PROJECT} \
        --member="serviceAccount:${SA}" \
        --role="roles/bigquery.dataEditor" \
        --condition=None \
        --quiet || echo "   ⚠️  Error o permiso ya existe"
    
    # BigQuery Job User en pph-central (para crear jobs de BigQuery)
    echo "   ➕ Agregando BigQuery Job User en pph-central..."
    gcloud projects add-iam-policy-binding ${CENTRAL_PROJECT} \
        --member="serviceAccount:${SA}" \
        --role="roles/bigquery.jobUser" \
        --condition=None \
        --quiet || echo "   ⚠️  Error o permiso ya existe"
    
    echo "   ✅ Permisos configurados para ${ENV}"
    echo ""
done

echo "🎉 ¡PERMISOS CONFIGURADOS!"
echo "=========================="
echo ""
echo "Las service accounts ahora pueden:"
echo "  ✅ Leer/escribir en pph-central.settings.companies_consolidated"
echo "  ✅ Crear jobs de BigQuery en pph-central"
echo "  ✅ Leer tablas bronze en proyectos de compañías"
echo ""
