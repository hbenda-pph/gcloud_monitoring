# Script de Actualización de Sync Status

Este script actualiza automáticamente los campos `last_etl_synced` y `row_count` en la tabla `companies_consolidated` ejecutándose 4 veces al día (7am, 1pm, 7pm, 1am).

## 🚀 Deploy Rápido

### Paso 1: Deploy del Cloud Run Job

El script detecta automáticamente el ambiente desde el proyecto activo de gcloud, o puedes especificarlo:

```bash
cd dashboard_etl_monitor
chmod +x deploy_sync_job.sh

# Opción A: Detectar automáticamente desde gcloud
./deploy_sync_job.sh

# Opción B: Especificar ambiente
./deploy_sync_job.sh dev    # Deploy en DEV
./deploy_sync_job.sh qua    # Deploy en QUA
./deploy_sync_job.sh pro    # Deploy en PRO
```

Este script:
- Detecta el ambiente (dev/qua/pro)
- Construye la imagen Docker en el proyecto correspondiente
- Crea/actualiza el Cloud Run Job en el proyecto correspondiente
- Muestra los comandos para crear los schedulers

### Paso 2: Crear los Cloud Schedulers

```bash
chmod +x create_schedulers.sh

# Opción A: Detectar automáticamente
./create_schedulers.sh

# Opción B: Especificar ambiente
./create_schedulers.sh dev
./create_schedulers.sh qua
./create_schedulers.sh pro
```

Esto crea 4 schedulers que ejecutan el job en:
- **7:00 AM** (1 hora después del ETL de 6am)
- **1:00 PM** (1 hora después del ETL de 12pm)
- **7:00 PM** (1 hora después del ETL de 6pm)
- **1:00 AM** (1 hora después del ETL de 12am)

## 📋 Configuración

### Arquitectura
- **Proyectos de Código:** DEV (`platform-partners-des`), QUA (`platform-partners-qua`), PRO (`constant-height-455614-i0`)
- **Proyecto de Datos:** `pph-central` (donde está `companies_consolidated`)
- **Región:** `us-east1`
- **Service Accounts:** 
  - DEV: `etl-servicetitan@platform-partners-des.iam.gserviceaccount.com`
  - QUA: `etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com`
  - PRO: `etl-servicetitan@constant-height-455614-i0.iam.gserviceaccount.com`

### Permisos Necesarios

La service account del proyecto de código (dev/qua/pro) debe tener:
- `BigQuery Job User` en su propio proyecto (para crear jobs)
- `BigQuery Data Editor` en `pph-central` (para escribir en `companies_consolidated`)
- `BigQuery Data Viewer` en todos los proyectos de compañías (para leer tablas bronze)
- `Cloud Run Invoker` (para que Cloud Scheduler pueda invocar el job)

## 🔍 Verificar Ejecución

### Ver logs del job

```bash
# Para DEV
gcloud run jobs executions list \
  --job=update-companies-consolidated-sync-dev \
  --region=us-east1 \
  --project=platform-partners-des \
  --limit=5

# Para QUA
gcloud run jobs executions list \
  --job=update-companies-consolidated-sync-qua \
  --region=us-east1 \
  --project=platform-partners-qua \
  --limit=5

# Para PRO
gcloud run jobs executions list \
  --job=update-companies-consolidated-sync \
  --region=us-east1 \
  --project=constant-height-455614-i0 \
  --limit=5
```

### Ver logs de una ejecución específica

```bash
# Reemplaza [PROJECT_ID] y [JOB_NAME] según el ambiente
gcloud run jobs executions logs read [EXECUTION_NAME] \
  --job=[JOB_NAME] \
  --region=us-east1 \
  --project=[PROJECT_ID]
```

### Ver schedulers

```bash
# Reemplaza [PROJECT_ID] según el ambiente
gcloud scheduler jobs list \
  --location=us-east1 \
  --project=[PROJECT_ID]
```

### Ejecutar manualmente

```bash
# Para DEV
gcloud run jobs execute update-companies-consolidated-sync-dev \
  --region=us-east1 \
  --project=platform-partners-des

# Para QUA
gcloud run jobs execute update-companies-consolidated-sync-qua \
  --region=us-east1 \
  --project=platform-partners-qua

# Para PRO
gcloud run jobs execute update-companies-consolidated-sync \
  --region=us-east1 \
  --project=constant-height-455614-i0
```

## 🛠️ Desarrollo Local

### Ejecutar localmente

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar script
python update_companies_consolidated_sync.py
```

### Variables de entorno (opcional)

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
python update_companies_consolidated_sync.py
```

## 📊 Qué hace el script

1. **Obtiene combinaciones:** Lee todas las combinaciones `company_id + table_name` desde `companies_consolidated`
2. **Obtiene project_id:** Para cada compañía, obtiene su `company_project_id` desde `settings.companies`
3. **Calcula sync data:** Para cada combinación, ejecuta:
   - `SELECT MAX(_etl_synced) FROM {company_project_id}.bronze.{table_name}`
   - `SELECT COUNT(*) FROM {company_project_id}.bronze.{table_name}`
4. **Actualiza tabla:** Actualiza `companies_consolidated` con los valores calculados

## ⚠️ Troubleshooting

### Error: "Permission denied"
- Verifica que la service account tenga los permisos necesarios
- Verifica que la service account esté configurada en el Cloud Run Job

### Error: "Table not found"
- Verifica que la tabla exista en `{company_project_id}.bronze.{table_name}`
- Verifica que el `company_project_id` sea correcto

### El job no se ejecuta
- Verifica que los schedulers estén activos: `gcloud scheduler jobs list`
- Verifica los logs de Cloud Scheduler
- Verifica que el job exista: `gcloud run jobs describe update-companies-consolidated-sync`
