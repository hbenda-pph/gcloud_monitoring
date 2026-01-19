# Script de Actualización de Sync Status

Este script actualiza automáticamente los campos `last_etl_synced` y `row_count` en la tabla `companies_consolidated` ejecutándose 4 veces al día (7am, 1pm, 7pm, 1am).

## 🚀 Deploy Rápido

### Paso 1: Deploy del Cloud Run Job

```bash
cd dashboard_etl_monitor
chmod +x deploy_sync_job.sh
./deploy_sync_job.sh
```

Este script:
- Construye la imagen Docker
- Crea/actualiza el Cloud Run Job
- Muestra los comandos para crear los schedulers

### Paso 2: Crear los Cloud Schedulers

```bash
chmod +x create_schedulers.sh
./create_schedulers.sh
```

Esto crea 4 schedulers que ejecutan el job en:
- **7:00 AM** (1 hora después del ETL de 6am)
- **1:00 PM** (1 hora después del ETL de 12pm)
- **7:00 PM** (1 hora después del ETL de 6pm)
- **1:00 AM** (1 hora después del ETL de 12am)

## 📋 Configuración

### Proyecto
- **Proyecto Central:** `pph-central`
- **Región:** `us-east1`
- **Service Account:** `etl-servicetitan@pph-central.iam.gserviceaccount.com`

### Permisos Necesarios

La service account debe tener:
- `BigQuery Data Editor` en `pph-central.settings.companies_consolidated`
- `BigQuery Data Viewer` en todos los proyectos de compañías (para leer tablas bronze)
- `Cloud Run Invoker` (para que Cloud Scheduler pueda invocar el job)

## 🔍 Verificar Ejecución

### Ver logs del job

```bash
gcloud run jobs executions list \
  --job=update-companies-consolidated-sync \
  --region=us-east1 \
  --project=pph-central \
  --limit=5
```

### Ver logs de una ejecución específica

```bash
gcloud run jobs executions logs read [EXECUTION_NAME] \
  --job=update-companies-consolidated-sync \
  --region=us-east1 \
  --project=pph-central
```

### Ver schedulers

```bash
gcloud scheduler jobs list \
  --location=us-east1 \
  --project=pph-central
```

### Ejecutar manualmente

```bash
gcloud run jobs execute update-companies-consolidated-sync \
  --region=us-east1 \
  --project=pph-central
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
