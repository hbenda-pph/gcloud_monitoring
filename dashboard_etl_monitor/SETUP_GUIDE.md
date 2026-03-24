# 🔐 IAM Access Monitor - Guía Completa de Implementación

## 📋 Descripción General

Sistema completo para monitorear accesos y credenciales en **BigQuery** a nivel de Google Cloud. Proporciona:

- **Dashboard Interactivo**: Visualización en tiempo real de quién tiene acceso a qué recursos
- **Matriz de Acceso**: Roles (filas) vs Usuarios/Service Accounts (columnas)
- **Filtrado por Recurso**: Similar a GCloud Resource Manager
- **Auditoría Histórica**: Sincronización de snapshots para detectar cambios
- **Análisis SQL**: Consultas para análisis avanzados

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│            IAM Access Monitor System                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │   iam_access_monitor.py (Dashboard Streamlit)   │  │
│  │   - Visualización interactiva                   │  │
│  │   - Matriz Roles vs Usuarios                    │  │
│  │   - Filtrado por recurso                        │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  sync_iam_access.py (Sincronización)            │  │
│  │  - Captura snapshots de IAM                     │  │
│  │  - Guarda en BigQuery                           │  │
│  │  - Detecta cambios                              │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  sql_iam_access_analysis.sql (Análisis)         │  │
│  │  - Queries para insights                        │  │
│  │  - Patrones de acceso                           │  │
│  │  - Auditoría de cambios                         │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │  run_iam_monitor.sh (CLI de utilidad)           │  │
│  │  - Ejecuta dashboard                            │  │
│  │  - Gestiona sincronización                      │  │
│  │  - Verifica autenticación                       │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
        ⬇️                                        ⬇️
  ┌─────────────────┐               ┌─────────────────────┐
  │   GCP Projects  │               │  BigQuery Auditoria │
  │  (Dev/Qua/Pro)  │               │  (pph-central:...) │
  └─────────────────┘               └─────────────────────┘
```

## 🚀 Inicio Rápido

### Paso 1: Instalación Inicial

```bash
# Navegar a la carpeta del proyecto
cd dashboard_etl_monitor

# Instalar dependencias
pip install -r requirements.txt

# O usar el script de utilidad
bash run_iam_monitor.sh install
```

### Paso 2: Autenticación Google Cloud

```bash
# Autenticar con Google Cloud
gcloud auth application-default login

# O usar el script de utilidad
bash run_iam_monitor.sh check-auth
```

### Paso 3: Iniciar Dashboard

```bash
# Opción 1: Comando directo
streamlit run iam_access_monitor.py

# Opción 2: Usar script de utilidad
bash run_iam_monitor.sh dashboard
```

El dashboard se abrirá en `http://localhost:8501`

## 📱 Interfaz del Dashboard

### Sidebar - Configuración

```
⚙️ CONFIGURACIÓN
├─ Selecciona Ambiente: [dev ▼]
│  └─ Proyecto: platform-partners-dev
│
🔍 FILTROS
├─ Tipo de Recurso: • Dataset
│                   ○ Table
│                   ○ View
│
├─ ☑ Mostrar Service Accounts
├─ ☑ Mostrar permisos heredados
└─ [🔄 Refrescar]
```

### Área Principal

```
📊 MATRIZ DE ACCESO
┌─────────────────────────────────────────────────────┐
│ Datasets    │ 👤 user@co │ 🤖 sa@proj │ 👤 contractor│
├─────────────────────────────────────────────────────┤
│ data-prod   │     ✓      │     ✓      │      -      │
│ reports     │     ✓      │     ✓      │      ✓      │
│ sensitive   │     ✓      │     -      │      -      │
└─────────────────────────────────────────────────────┘

📊 ESTADÍSTICAS
├─ Total Datasets: 3
├─ Usuarios Únicos: 3
├─ Service Accounts: 1
└─ Recursos: 3

[📥 Descargar como CSV]
```

### Análisis Detallado

```
📋 ANÁLISIS DETALLADO POR DATASET

📂 data-prod ▼
  ┌──────────────────────────────────────────┐
  │ Tipo     │ Usuario         │ Email    │ Grupo
  ├──────────────────────────────────────────┤
  │ OWNER    │ user@company.com│ ✓       │ -
  │ READER   │ sa@project.iam  │ ✓       │ -
  └──────────────────────────────────────────┘
```

## 🔄 Sincronización de IAM

### Capturar Snapshot Actual

```bash
# Sincronizar todos los ambientes
bash run_iam_monitor.sh sync

# O comando directo
python sync_iam_access.py --environment all

# Solo dev
python sync_iam_access.py --environment dev

# Dry-run (ver qué se insertaría sin insertar)
python sync_iam_access.py --environment all --dry-run
```

### Detectar Cambios

```bash
# Sincronizar y comparar con snapshot anterior
python sync_iam_access.py --environment dev --compare
```

### Output Esperado

```
INFO:root:Iniciando sincronización IAM
INFO:root:Ambientes a procesar: dev, qua, pro
INFO:root:
=== Procesando dev (platform-partners-des) ===
INFO:root:Procesando 12 datasets en platform-partners-des
INFO:root:Capturados 45 registros de IAM para platform-partners-des
✓ Snapshot de dev inserado correctamente

=== Sincronización completada ===
Total de registros capturados: 128
```

## 📊 Consultas SQL Avanzadas

### Ejecutar Queries de Análisis

Las queries en `sql_iam_access_analysis.sql` incluyen:

1. **Acceso por Dataset**: Quién accede a cada dataset
2. **Service Accounts Activos**: Listado de SAs del sistema
3. **Matriz de Acceso**: Basada en logs
4. **Usuarios Inactivos**: Sin actividad en 30+ días
5. **Cambios Recientes**: Últimos 7 días
6. **Resumen de Permisos**: Por mes y usuario
7. **Patrones Anormales**: Detección de anomalías
8. **Auditoría IAM**: Histórico de cambios

### Ejemplo: Usuarios Inactivos

```sql
-- Ejecutar en BigQuery Console
-- Encuentra usuarios sin actividad en 30+ días

SELECT
    user_email,
    last_activity_date,
    DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) as days_inactive,
    CASE 
        WHEN DATE_DIFF(...) >= 30 THEN 'Inactivo'
        ELSE 'Poco Activo'
    END as status
FROM `pph-central.management.iam_access_history`
ORDER BY last_activity_date ASC;
```

## 🗂️ Estructura de Archivos

```
dashboard_etl_monitor/
├── iam_access_monitor.py              # Dashboard principal (Streamlit)
├── sync_iam_access.py                 # Script de sincronización
├── sql_iam_access_analysis.sql        # Queries de análisis
├── run_iam_monitor.sh                 # Script de utilidad
├── requirements.txt                   # Dependencias Python
├── README_IAM_ACCESS_MONITOR.md       # Guía de uso
├── SETUP_GUIDE.md                     # Este archivo
│
├── streamlit_app.py                   # Dashboard ETL (existente)
├── requirements.txt                   # Dependencias (actualizado)
│
└── (otros archivos del proyecto)
```

## 🔐 Configuración de Seguridad

### Permisos Requeridos

Minimal:
```
roles/bigquery.dataViewer
roles/bigquery.user
```

Recomendado (para auditoría completa):
```
roles/bigquery.admin
roles/logging.viewer
roles/iam.securityReviewer
```

### Autenticación

```bash
# Usuario local
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# O con gcloud
gcloud auth application-default login

# Verificar
gcloud auth application-default print-access-token
```

### Variables de Entorno

```bash
# Configurar ambiente
export ENVIRONMENT=dev
export GCP_PROJECT=platform-partners-des

# Opcional: Cloud Logging
export GOOGLE_CLOUD_PROJECT=pph-central
```

## 📋 Checklist de Configuración

- [ ] Python 3.8+ instalado
- [ ] `pip install -r requirements.txt` ejecutado
- [ ] `gcloud auth application-default login` ejecutado
- [ ] Verificado acceso a proyectos de GCP
- [ ] Dataset `pph-central.management` existe
- [ ] Tabla `management.iam_access_snapshot` creada (o se crea automáticamente)
- [ ] Tabla `management.iam_access_history` creada (o se crea automáticamente)
- [ ] Cloud Audit Logs habilitados (para análisis de cambios)

## 🐛 Troubleshooting

### Error: "PermissionDenied"

```
PermissionDenied: 403 project-id does not have bigquery resource
```

**Solución:**
```bash
gcloud auth application-default login
# O asignar rol
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member=user@email.com \
  --role=roles/bigquery.admin
```

### Error: "Dataset not found"

```
NotFound: 404 Not found: Dataset pph-central:management
```

**Solución:**
```bash
# Crear dataset manualmente en BigQuery
bq mk --dataset pph-central:management

# O en console: bigquery.cloud.google.com
```

### Error: "Streamlit not found"

```
command not found: streamlit
```

**Solución:**
```bash
pip install streamlit
# O
bash run_iam_monitor.sh install
```

### Error: "No datasets found"

**Causas:**
- Proyecto sin datasets
- Permisos insuficientes
- Proyecto incorrecto

**Solución:**
```bash
# Verificar proyecto actual
gcloud config get-value project

# Cambiar proyecto
gcloud config set project PROJECT_ID

# Listar datasets
bq ls
```

## 📈 Casos de Uso

### 1. Auditoría Cumplimiento

Verificar accesos a datos sensibles:
```python
# En el dashboard
1. Selecciona ambiente: "pro"
2. Selecciona datasets sensibles
3. Descarga CSV para auditoría
4. Reporta a compliance@...
```

### 2. Offboarding de Empleado

Verificar qué debe revocar:
```bash
# SQL
SELECT * FROM iam_access_snapshot
WHERE principal_email = "departing@company.com"
```

### 3. Análisis de Cambios

Detectar cuándo cambió algo:
```bash
python sync_iam_access.py --environment dev --compare
# Verifica qué service accounts se agregaron/removieron
```

### 4. Reporte de Control de Acceso

Generar reporte mensual:
```bash
# Ejecutar query de resumen
SELECT 
    environment,
    COUNT(DISTINCT principal_email) as total_users,
    COUNT(DISTINCT dataset_id) as total_datasets
FROM iam_access_snapshot
WHERE snapshot_date = CURRENT_DATE() - 1
GROUP BY environment;
```

## 🤝 Integración con Otros Sistemas

### Google Cloud Logging

Para análisis más detallado:
```bash
# Habilitar Cloud Audit Logs (si no está habilitado)
gcloud logging sinks create bigquery-export \
  bigquery.googleapis.com/projects/PROJECT_ID/datasets/audit_logs
```

### Slack/Email Alerts

Próxima fase: Integrar notificaciones cuando:
- Usuario nuevo con acceso sensible
- Service account inactivo detectado
- Patrón anómalo de acceso

```python
# Pseudocódigo para futura implementación
if anomaly_detected():
    send_slack_notification()
```

## 📚 Referencias

- [BigQuery Access Control](https://cloud.google.com/bigquery/docs/access-control)
- [Cloud IAM Roles](https://cloud.google.com/iam/docs/understanding-roles)
- [GCloud Resource Manager API](https://cloud.google.com/resource-manager/docs)
- [Streamlit Documentation](https://docs.streamlit.io/)

## 👥 Soporte

Para reportar bugs o solicitar features:

```bash
# 1. Describe el problema
# 2. Incluye logs (bash run_iam_monitor.sh check-deps)
# 3. Contacta a Platform Engineering
```

## 📝 Changelog

### v1.0.0 (Marzo 2026)

- ✅ Dashboard interactivo con Streamlit
- ✅ Matriz roles vs usuarios
- ✅ Sincronización de IAM a BigQuery
- ✅ Consultas SQL de análisis
- ✅ Filtrado por recurso
- ✅ Script CLI de utilidad

### Futuro

- ⏳ Alertas por Slack/Email
- ⏳ Histórico de cambios con timestamp
- ⏳ Integración Cloud Asset API
- ⏳ Detección de anomalías avanzada
- ⏳ API GraphQL

---

**Última actualización:** Marzo 2026  
**Versión:** 1.0.0
