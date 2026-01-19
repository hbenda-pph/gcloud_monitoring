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
| **dev** | `platform-partners-dev` | `platform-partners-dev` |
| **qua** | `platform-partners-qua` | `platform-partners-qua` |
| **pro** | `platform-partners-pro` | `constant-height-455614-i0` |

### Ejemplo de Uso por Ambiente

```bash
# Ambiente DEV
export ENVIRONMENT=dev
export GCP_PROJECT=platform-partners-dev
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

## 📊 Uso

```bash
streamlit run streamlit_app.py
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
