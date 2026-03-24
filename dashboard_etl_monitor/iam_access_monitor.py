"""
Dashboard de Monitoreo de Accesos y Credenciales BigQuery
Matriz: Roles (filas) vs Usuarios/Service Accounts (columnas)
Filtrado por Recurso similar a GCloud Resource Manager
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from google.cloud import bigquery, iam, resourcemanager
from google.api_core.exceptions import NotFound, PermissionDenied
import os
from typing import Dict, List, Tuple, Set
from collections import defaultdict

# ========== CONFIGURACIÓN ==========
st.set_page_config(
    page_title="IAM Access Monitor - BigQuery",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes para metadata
ENVIRONMENT_CONFIG = {
    "dev": {
        "project_name": "platform-partners-dev",
        "project_id": "platform-partners-des"
    },
    "qua": {
        "project_name": "platform-partners-qua",
        "project_id": "platform-partners-qua"
    },
    "pro": {
        "project_name": "platform-partners-pro",
        "project_id": "constant-height-455614-i0"
    }
}

# Mapeo de tipos de recursos BigQuery
RESOURCE_TYPES = {
    "Project": "projects",
    "Dataset": "datasets",
    "Table": "tables",
    "View": "views",
}

# Colores para roles
ROLE_COLORS = {
    "roles/bigquery.admin": "#FF6B6B",
    "roles/bigquery.dataEditor": "#4ECDC4",
    "roles/bigquery.dataViewer": "#45B7D1",
    "roles/bigquery.jobUser": "#96CEB4",
    "roles/bigquery.user": "#FFEAA7",
    "roles/owner": "#A29BFE",
    "roles/editor": "#6C5CE7",
    "roles/viewer": "#74B9FF",
}

# ========== FUNCIONES AUXILIARES ==========

def detect_environment() -> str:
    """Detecta el ambiente actual (dev, qua, pro)."""
    env = os.environ.get('ENVIRONMENT', '').lower()
    if env in ['dev', 'qua', 'pro']:
        return env
    
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if project:
        project_lower = project.lower()
        if 'platform-partners-des' in project_lower or ('dev' in project_lower and 'des' not in project_lower):
            return 'dev'
        elif 'constant-height-455614-i0' in project_lower or ('pro' in project_lower):
            return 'pro'
        elif 'qua' in project_lower or 'qa' in project_lower:
            return 'qua'
    
    try:
        client = bigquery.Client()
        project = client.project
        if project:
            project_lower = project.lower()
            if 'platform-partners-des' in project_lower:
                return 'dev'
            elif 'constant-height-455614-i0' in project_lower:
                return 'pro'
            elif 'platform-partners-qua' in project_lower:
                return 'qua'
    except Exception:
        pass
    
    return 'dev'

@st.cache_resource
def get_bigquery_client():
    """Obtiene cliente de BigQuery."""
    return bigquery.Client()

@st.cache_resource
def get_iam_client():
    """Obtiene cliente de IAM."""
    return iam.GetIamPolicyRequest()

@st.cache_data(ttl=3600)
def get_all_projects() -> List[str]:
    """Obtiene lista de todos los proyectos configurados."""
    return [config["project_id"] for config in ENVIRONMENT_CONFIG.values()]

@st.cache_data(ttl=3600)
def get_project_iam_policy(project_id: str) -> Dict:
    """
    Obtiene la política IAM de un proyecto.
    
    Args:
        project_id: ID del proyecto GCP
        
    Returns:
        Dict con bindings de IAM
    """
    try:
        client = bigquery.Client(project=project_id)
        
        # Crear una consulta para obtener información de datasets
        datasets = []
        try:
            for dataset in client.list_datasets():
                datasets.append(dataset.dataset_id)
        except PermissionDenied:
            st.warning(f"Permisos insuficientes para listar datasets en {project_id}")
        
        return {
            "project_id": project_id,
            "datasets": datasets
        }
    except Exception as e:
        st.error(f"Error obteniendo información del proyecto {project_id}: {str(e)}")
        return {"project_id": project_id, "datasets": []}

@st.cache_data(ttl=3600)
def get_dataset_iam_policy(project_id: str, dataset_id: str) -> Dict:
    """
    Obtiene la política IAM de un dataset (acceso).
    
    Args:
        project_id: ID del proyecto
        dataset_id: ID del dataset
        
    Returns:
        Dict con información de acceso
    """
    try:
        client = bigquery.Client(project=project_id)
        dataset = client.get_dataset(f"{project_id}.{dataset_id}")
        
        # Obtener acceso del dataset
        access_entries = dataset.access_entries or []
        
        return {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "access_entries": access_entries
        }
    except Exception as e:
        return {"project_id": project_id, "dataset_id": dataset_id, "access_entries": []}

@st.cache_data(ttl=3600)
def get_dataset_tables(project_id: str, dataset_id: str) -> List[str]:
    """
    Obtiene lista de tablas en un dataset.
    
    Args:
        project_id: ID del proyecto
        dataset_id: ID del dataset
        
    Returns:
        Lista de IDs de tablas
    """
    try:
        client = bigquery.Client(project=project_id)
        tables = []
        for table in client.list_tables(dataset_id):
            tables.append(table.table_id)
        return tables
    except Exception:
        return []

def build_access_matrix(
    project_id: str,
    selected_resources: List[str],
    resource_type: str
) -> Tuple[pd.DataFrame, Dict]:
    """
    Construye una matriz de acceso: Roles (filas) vs Usuarios (columnas).
    
    Args:
        project_id: ID del proyecto
        selected_resources: Lista de recursos seleccionados
        resource_type: Tipo de recurso ('Dataset', 'Table', etc)
        
    Returns:
        Tupla con (DataFrame de matriz, diccionario de roles por recurso)
    """
    
    roles_dict = defaultdict(lambda: defaultdict(list))
    all_users = set()
    all_roles = set()
    
    client = bigquery.Client(project=project_id)
    
    if resource_type == "Dataset":
        for dataset_id in selected_resources:
            try:
                dataset = client.get_dataset(f"{project_id}.{dataset_id}")
                access_entries = dataset.access_entries or []
                
                for entry in access_entries:
                    role = entry.role or "Inherited"
                    user_id = entry.user_by_email or entry.group_by_email or entry.special_group or "Unknown"
                    
                    if user_id and user_id != "Unknown":
                        roles_dict[dataset_id][role].append(user_id)
                        all_users.add(user_id)
                        all_roles.add(role)
            except Exception as e:
                st.warning(f"Error procesando dataset {dataset_id}: {str(e)}")
    
    # Crear matriz
    if not all_users or not all_roles:
        return pd.DataFrame(), roles_dict
    
    # Reordenar usuarios (service accounts primero)
    sorted_users = sorted(list(all_users), 
                          key=lambda x: (not x.endswith('@gserviceaccount.com'), x))
    sorted_roles = sorted(list(all_roles))
    
    # Construir matriz
    matrix_data = {}
    for resource in selected_resources:
        resource_row = {}
        for user in sorted_users:
            has_access = False
            for role in sorted_roles:
                if user in roles_dict[resource].get(role, []):
                    has_access = True
                    break
            resource_row[user] = "✓" if has_access else ""
        matrix_data[resource] = resource_row
    
    df_matrix = pd.DataFrame(matrix_data).T
    df_matrix.columns = sorted_users
    
    return df_matrix, roles_dict

def format_user_display(user_id: str) -> str:
    """Formatea el display del usuario."""
    if user_id.endswith('@gserviceaccount.com'):
        return f"🤖 {user_id}"
    elif '@' in user_id:
        return f"👤 {user_id}"
    else:
        return f"🔗 {user_id}"

# ========== INTERFAZ PRINCIPAL ==========

def main():
    st.title("🔐 IAM Access Monitor - BigQuery")
    st.markdown("Monitorea accesos y credenciales de usuarios en BigQuery")
    
    # Sidebar para configuración
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        # Seleccionar ambiente
        environment = st.selectbox(
            "Selecciona Ambiente:",
            ["dev", "qua", "pro"],
            index=0,
            help="Elige el ambiente GCP a monitorear"
        )
        
        selected_projects = [ENVIRONMENT_CONFIG[environment]["project_id"]]
        project_name = ENVIRONMENT_CONFIG[environment]["project_name"]
        
        st.markdown(f"**Proyecto:** `{project_name}`")
        st.markdown(f"**Project ID:** `{selected_projects[0]}`")
        
        # Opciones de filtrado
        st.subheader("🔍 Filtros")
        
        resource_type = st.radio(
            "Tipo de Recurso:",
            list(RESOURCE_TYPES.keys()),
            help="Selecciona el tipo de recurso a monitorear"
        )
        
        show_service_accounts = st.checkbox(
            "Mostrar Service Accounts",
            value=True,
            help="Incluir service accounts en el análisis"
        )
        
        show_inherited = st.checkbox(
            "Mostrar permisos heredados",
            value=True,
            help="Incluir permisos heredados de nivel superior"
        )
    
    # Área principal
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.header("📊 Matriz de Acceso")
    
    with col2:
        if st.button("🔄 Refrescar", use_container_width=True):
            st.cache_data.clear()
    
    # Obtener recursos disponibles
    project_id = selected_projects[0]
    
    try:
        client = bigquery.Client(project=project_id)
        
        if resource_type == "Dataset":
            st.subheader("Datasets Disponibles")
            
            # Listar datasets
            datasets = []
            try:
                for dataset in client.list_datasets():
                    datasets.append(dataset.dataset_id)
            except PermissionDenied:
                st.error("Permisos insuficientes para listar datasets")
                return
            
            if not datasets:
                st.info("No hay datasets disponibles en este proyecto")
                return
            
            # Multiselect de datasets
            selected_datasets = st.multiselect(
                f"Selecciona datasets a analizar ({len(datasets)} disponibles):",
                options=datasets,
                default=datasets[:min(5, len(datasets))],
                help="Elige los datasets para ver sus accesos"
            )
            
            if selected_datasets:
                # Construir matriz
                with st.spinner("Construyendo matriz de acceso..."):
                    df_matrix, roles_dict = build_access_matrix(
                        project_id,
                        selected_datasets,
                        resource_type
                    )
                
                if not df_matrix.empty:
                    # Mostrar estadísticas
                    st.write("")
                    stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
                    
                    with stats_col1:
                        st.metric("Total Datasets", len(selected_datasets))
                    
                    with stats_col2:
                        total_users = df_matrix.shape[1]
                        st.metric("Usuarios Únicos", total_users)
                    
                    with stats_col3:
                        sa_count = sum(1 for col in df_matrix.columns if col.endswith('@gserviceaccount.com'))
                        st.metric("Service Accounts", sa_count)
                    
                    with stats_col4:
                        st.metric("Recursos", len(selected_datasets))
                    
                    st.write("")
                    
                    # Tabla interactiva
                    st.subheader("Matriz: Datasets vs Usuarios")
                    
                    # Formatar columnas con iconos
                    df_display = df_matrix.copy()
                    df_display.columns = [format_user_display(col) for col in df_display.columns]
                    
                    st.dataframe(
                        df_display,
                        use_container_width=True,
                        height=400
                    )
                    
                    # Descargar como CSV
                    csv = df_display.to_csv(index=True)
                    st.download_button(
                        label="📥 Descargar como CSV",
                        data=csv,
                        file_name=f"iam_access_matrix_{environment}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
                    # Análsis detallado
                    st.write("")
                    st.subheader("📋 Análisis Detallado por Dataset")
                    
                    for dataset_id in selected_datasets:
                        with st.expander(f"📂 {dataset_id}"):
                            dataset_obj = client.get_dataset(f"{project_id}.{dataset_id}")
                            access_entries = dataset_obj.access_entries or []
                            
                            if access_entries:
                                access_df = pd.DataFrame([
                                    {
                                        "Tipo": entry.role or "Inherited",
                                        "Usuario": entry.user_by_email or entry.group_by_email or entry.special_group or "Unknown",
                                        "Email": entry.user_by_email or "-",
                                        "Grupo": entry.group_by_email or "-",
                                    }
                                    for entry in access_entries
                                ])
                                st.dataframe(access_df, use_container_width=True)
                            else:
                                st.info("Sin entradas de acceso específicas")
                else:
                    st.warning("No se pudieron construir la matriz. Verifica los permisos.")
        
        else:
            st.info(f"El tipo de recurso '{resource_type}' aún no está disponible. Comienza con 'Dataset'.")
    
    except PermissionDenied:
        st.error(f"❌ Permisos insuficientes en el proyecto {project_id}")
        st.markdown("""
        **Acciones sugeridas:**
        1. Verifica que tu usuario tenga rol `roles/bigquery.admin` en el proyecto
        2. Ejecuta: `gcloud auth application-default login`
        3. Reinicia la aplicación
        """)
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)

if __name__ == "__main__":
    main()
