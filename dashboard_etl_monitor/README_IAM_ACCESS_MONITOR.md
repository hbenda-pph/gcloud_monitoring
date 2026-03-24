# 🔐 IAM Access Monitor - BigQuery

Dashboard de monitoreo de accesos y credenciales para BigQuery. Visualiza qué usuarios y service accounts tienen acceso a cada dataset, con una matriz interactiva similar a la pantalla de **Resource Manager** de Google Cloud.

## 📋 Características

- **Matriz de Acceso**: Visualiza Datasets (filas) vs Usuarios (columnas)
- **Filtrado por Recurso**: Selecciona múltiples datasets para análisis
- **Soporte Multiambiente**: Funciona con dev, qua y pro
- **Service Accounts**: Identifica claramente service accounts vs usuarios regulares
- **Análisis Detallado**: Expande cada dataset para ver entradas de acceso específicas
- **Exportación CSV**: Descarga la matriz para auditorías

## 🚀 Instalación

```bash
# Instalar dependencias
pip install -r requirements.txt
```

## 🔧 Configuración Previa

### 1. Autenticación de Google Cloud

```bash
gcloud auth application-default login
```

### 2. Permisos Requeridos

Tu usuario o service account necesita los siguientes permisos en cada proyecto:

```
roles/bigquery.dataViewer    # Ver datasets
roles/bigquery.admin         # Acceso completo (recomendado)
```

O permisos específicos:
- `bigquery.datasets.get`
- `bigquery.datasets.list`

### 3. Configurar Proyectos

Edita `ENVIRONMENT_CONFIG` en `iam_access_monitor.py` si necesitas agregar nuevos proyectos:

```python
ENVIRONMENT_CONFIG = {
    "dev": {
        "project_name": "your-dev-project",
        "project_id": "your-dev-project-id"
    },
    "qua": {
        "project_name": "your-qua-project",
        "project_id": "your-qua-project-id"
    },
    "pro": {
        "project_name": "your-pro-project",
        "project_id": "your-pro-project-id"
    }
}
```

## 📊 Uso Local

### Ejecutar el Dashboard

```bash
streamlit run iam_access_monitor.py
```

La aplicación se abrirá en `http://localhost:8501`

### Interfaz

1. **Sidebar - Configuración**:
   - Selecciona ambiente (dev/qua/pro)
   - Elige tipo de recurso (Dataset, Table, etc.)
   - Opciones de filtrado

2. **Área Principal**:
   - Selecciona datasets a analizar
   - Visualiza matriz de acceso
   - Expande para detalles

### Leyenda de Símbolos

- `🤖` - Service Account
- `👤` - Usuario regular
- `🔗` - Grupo u otra entidad
- `✓` - Acceso disponible

## 🔍 Casos de Uso

### Auditoría de Acceso
Verifica qué usuarios tienen acceso a qué datasets en cada ambiente.

```
Datasets         | usuario@company.com | sa@project.iam | contractor@guest.com
data-prod        |         ✓           |       ✓        |          
sensitive-data   |         ✓           |               |            
```

### Identificar Service Accounts
Localiza todos los service accounts con acceso a recursos sensibles.

```
Datasets         | bigquery-sa@... | functions-sa@... | app-server-sa@...
reports          |        ✓        |        ✓         |        ✓
```

### Gestión de Credenciales
Identifica usuarios inactivos o que deberían tener sus permisos revocados.

## 📥 Exportação de Datos

Usa el botón **Descargar como CSV** para exportar la matriz en formato:

```csv
Dataset,usuario@company.com,sa@project.iam,contractor@guest.com
data-prod,✓,✓,
sensitive-data,✓,,
insights,✓,✓,✓
```

## ⚠️ Limitaciones Actuales

- **Tablas**: Herencia desde dataset (no muestra acceso específico a nivel tabla)
- **Views**: Idem tablas
- **IAM de Proyecto**: Las políticas a nivel proyecto se heredan pero no se muestran separadamente
- **Histórico**: Solo muestra estado actual, no cambios históricos

## 🔐 Seguridad

- Las credenciales se usan solo para lectura (sin modificaciones)
- Los datos no se almacenan, solo se visualizan en tiempo real
- Usa `gcloud auth application-default login` para autenticación segura

## 🛠️ Troubleshooting

### Error: "PermissionDenied"
```
❌ Permisos insuficientes en el proyecto
```
**Solución**: Ejecuta `gcloud auth application-default login` y verifica permisos.

### Error: "No datasets found"
```
No hay datasets disponibles en este proyecto
```
**Solución**: Verifica que el proyecto tiene datasets creados.

### Error: "Unable to import required modules"
```
ModuleNotFoundError: No module named 'google.cloud'
```
**Solución**: Ejecuta `pip install -r requirements.txt`

## 📚 Referencia

- [Google Cloud IAM Roles](https://cloud.google.com/iam/docs/understanding-roles)
- [BigQuery Dataset Access Control](https://cloud.google.com/bigquery/docs/dataset-access-controls)
- [Streamlit Documentation](https://docs.streamlit.io/)

## 👤 Contacto / Soporte

Para reportar bugs o solicitar features, contacta al equipo de Platform Engineering.

---

**Última actualización**: Marzo 2026
