# Dashboard de Monitoreo ETL ServiceTitan

Dashboard en Streamlit para monitorear el estado de sincronización de las tablas Bronze en BigQuery.

## 🚀 Instalación

```bash
pip install -r requirements.txt
```

## 🔧 Configuración

El dashboard usa las credenciales de Google Cloud configuradas en tu ambiente. Asegúrate de tener:

1. **Autenticación de Google Cloud:**
   ```bash
   gcloud auth application-default login
   ```

2. **Permisos necesarios:**
   - Lectura en `{project_id}.settings.companies`
   - Lectura en `pph-central.management.metadata_consolidated_tables`
   - Lectura en todos los proyectos de compañías (dataset `bronze`)

## 🌍 Soporte Multiambiente

El dashboard detecta automáticamente el ambiente (dev, qua, pro) y ajusta las consultas según corresponda.

### Detección Automática

El ambiente se detecta en el siguiente orden de prioridad:

1. **Variable de entorno `ENVIRONMENT`**: `dev`, `qua` o `pro`
2. **Variable de entorno `GCP_PROJECT` o `GOOGLE_CLOUD_PROJECT`**: Detecta desde el nombre del proyecto
3. **Cliente BigQuery**: Detecta desde el proyecto activo
4. **Fallback**: `qua` por defecto

### Configuración de Ambientes

| Ambiente | Project Name | Project ID |
|----------|--------------|------------|
| **dev** | `platform-partners-dev` | `platform-partners-des` |
| **qua** | `platform-partners-qua` | `platform-partners-qua` |
| **pro** | `platform-partners-pro` | `constant-height-455614-i0` |

## 📊 Uso Local

### Ejecución Local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar dashboard
streamlit run streamlit_app.py
```

### Ejemplo de Uso por Ambiente (Local)

```bash
# Ambiente DEV
export ENVIRONMENT=dev
export GCP_PROJECT=platform-partners-des
streamlit run streamlit_app.py

# Ambiente QUA (por defecto)
export ENVIRONMENT=qua
export GCP_PROJECT=platform-partners-qua
streamlit run streamlit_app.py

# Ambiente PRO
export ENVIRONMENT=pro
export GCP_PROJECT=platform-partners-pro
streamlit run streamlit_app.py
```

## 🚀 Deploy en Google Cloud Run

### Prerrequisitos

1. **Google Cloud SDK instalado y configurado:**
   ```bash
   gcloud auth login
   gcloud config set project [PROJECT_ID]
   ```

2. **Permisos necesarios:**
   - Cloud Build Editor
   - Cloud Run Admin
   - Service Account User

3. **Service Account configurado:**
   - `streamlit-bigquery-sa@{PROJECT_ID}.iam.gserviceaccount.com`
   - Con permisos de BigQuery Data Viewer

### Deploy Automático

El script `build_deploy.sh` automatiza todo el proceso de build y deploy:

```bash
# Dar permisos de ejecución
chmod +x build_deploy.sh

# Deploy en ambiente específico
./build_deploy.sh dev    # Deploy en DEV
./build_deploy.sh qua    # Deploy en QUA
./build_deploy.sh pro    # Deploy en PRO

# O sin parámetros (detecta automáticamente desde gcloud)
./build_deploy.sh
```

### Qué hace el script

1. **Detecta el ambiente** (dev/qua/pro) desde el proyecto activo de gcloud
2. **Verifica archivos** necesarios (streamlit_app.py, requirements.txt, Dockerfile)
3. **Build de imagen Docker** usando Cloud Build
4. **Deploy en Cloud Run** con configuración optimizada

### Configuración del Servicio

- **Memoria:** 2Gi
- **CPU:** 2
- **Timeout:** 300s
- **Max Instances:** 10
- **Min Instances:** 0
- **Concurrency:** 80
- **Port:** 8501
- **Región:** us-east1

### Verificar Deploy

```bash
# Obtener URL del servicio
gcloud run services describe etl-monitor-dashboard-{ENV} \
  --region=us-east1 \
  --project={PROJECT_ID} \
  --format='value(status.url)'

# Ver logs
gcloud run services logs read etl-monitor-dashboard-{ENV} \
  --region=us-east1 \
  --project={PROJECT_ID} \
  --tail
```

El dashboard mostrará:
- **Tablas (Y-axis)**: ~70 tablas desde metadata
- **Compañías (X-axis)**: ~30 compañías activas
- **Valores**: MAX(_etl_synced) de cada tabla por compañía

## 📋 Funcionalidades

### Paso 1: Carga de Compañías
Obtiene todas las compañías activas desde `settings.companies`

### Paso 2: Carga de Tablas
Obtiene todas las tablas desde `metadata_consolidated_tables` donde `silver_use_bronze = TRUE`

### Paso 3-4: Construcción de Matriz
Para cada combinación tabla-compañía:
- Consulta `MAX(_etl_synced)` en `{project_id}.bronze.{table_name}`
- Construye matriz de timestamps

### Visualización
- 🔴 Rojo: Sincronización hace más de 7 días
- 🟡 Amarillo: Sincronización hace 1-7 días
- 🟢 Verde: Sincronización en últimas 24 horas
- ❌ No existe o sin datos
