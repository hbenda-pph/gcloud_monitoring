"""
Dashboard de Monitoreo ETL ServiceTitan
Matriz: Tablas (Y) vs Compañías (X) con MAX(_etl_synced)
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os

# ========== CONFIGURACIÓN ==========
st.set_page_config(
    page_title="ETL Monitor - ServiceTitan",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes para metadata
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"

# ========== FUNCIONES AUXILIARES ==========

def get_project_source():
    """
    Obtiene el proyecto del ambiente actual.
    Prioridad: variable de entorno > cliente BigQuery > fallback
    """
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if project:
        return project
    
    try:
        client = bigquery.Client()
        return client.project
    except:
        pass
    
    return "platform-partners-qua"  # Fallback

def get_bigquery_project_id():
    """
    Obtiene el project_id real para usar en queries SQL.
    Maneja el caso especial de PRO donde project_name != project_id.
    """
    project_source = get_project_source()
    
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    
    return project_source

# ========== PASO 1: OBTENER COMPAÑÍAS ==========

@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_companies():
    """
    Obtiene todas las compañías activas desde BigQuery.
    
    Retorna:
        DataFrame con columns: company_id, company_name, company_project_id
    """
    try:
        PROJECT_ID = get_bigquery_project_id()
        client = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
            SELECT 
                company_id,
                company_name,
                company_project_id
            FROM `{PROJECT_ID}.settings.companies`
            WHERE company_fivetran_status = TRUE
            ORDER BY company_name
        """
        
        df = client.query(query).to_dataframe()
        return df
        
    except Exception as e:
        st.error(f"❌ Error obteniendo compañías: {str(e)}")
        return pd.DataFrame()

# ========== PASO 2: OBTENER TABLAS ==========

@st.cache_data(ttl=3600)  # Cache por 1 hora (metadata cambia poco)
def get_tables_from_metadata():
    """
    Obtiene todas las tablas desde metadata que deben estar en Bronze.
    
    Retorna:
        Lista de nombres de tablas ordenadas
    """
    try:
        client = bigquery.Client(project=METADATA_PROJECT)
        
        query = f"""
            SELECT 
                table_name
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE endpoint IS NOT NULL
              AND active = TRUE
              AND silver_use_bronze = TRUE
            ORDER BY table_name
        """
        
        df = client.query(query).to_dataframe()
        return df['table_name'].tolist()
        
    except Exception as e:
        st.error(f"❌ Error obteniendo tablas desde metadata: {str(e)}")
        return []

# ========== PASO 3: OBTENER MAX(_etl_synced) POR TABLA ==========

def get_last_sync_timestamp(project_id, table_name):
    """
    Obtiene el MAX(_etl_synced) de una tabla Bronze en un proyecto específico.
    
    Args:
        project_id: ID del proyecto de BigQuery (ej: "company-project-123")
        table_name: Nombre de la tabla en dataset 'bronze' (ej: "jobs")
        
    Retorna:
        datetime con el último timestamp de sincronización, o None si no existe
    """
    try:
        client = bigquery.Client(project=project_id)
        dataset_id = "bronze"
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        
        # Verificar si la tabla existe
        try:
            client.get_table(table_ref)
        except NotFound:
            return None
        
        # Obtener MAX(_etl_synced)
        query = f"""
            SELECT MAX(_etl_synced) as max_sync
            FROM `{table_ref}`
        """
        
        result = client.query(query).to_dataframe()
        
        if result.empty or result.iloc[0]['max_sync'] is None:
            return None
            
        return pd.to_datetime(result.iloc[0]['max_sync'])
        
    except Exception as e:
        # Si hay error (sin permisos, etc.), retornar None
        return None

# ========== PASO 4: CONSTRUIR MATRIZ ==========

def build_sync_matrix(companies_df, tables_list):
    """
    Construye la matriz de sincronización: Tablas (filas) vs Compañías (columnas).
    
    Para cada combinación tabla-compañía:
    - Consulta MAX(_etl_synced) en {company_project_id}.bronze.{table_name}
    - Almacena el timestamp resultante
    
    Args:
        companies_df: DataFrame con compañías (debe tener company_project_id)
        tables_list: Lista de nombres de tablas
        
    Retorna:
        DataFrame con:
            - Índices (filas) = nombres de tablas
            - Columnas = nombres de compañías
            - Valores = timestamps de MAX(_etl_synced) o None
    """
    # Estructura: {table_name: {company_name: timestamp}}
    matrix_data = {}
    
    # Barra de progreso
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_cells = len(companies_df) * len(tables_list)
    current_cell = 0
    
    # Iterar sobre cada TABLA (serán las FILAS de la matriz)
    for table_name in tables_list:
        row_data = {}
        
        # Iterar sobre cada COMPAÑÍA (serán las COLUMNAS de la matriz)
        for _, company in companies_df.iterrows():
            company_name = company['company_name']
            project_id = company['company_project_id']
            
            # Obtener último timestamp de sincronización
            last_sync = get_last_sync_timestamp(project_id, table_name)
            row_data[company_name] = last_sync
            
            # Actualizar progreso
            current_cell += 1
            progress = current_cell / total_cells
            progress_bar.progress(progress)
            status_text.text(f"Procesando: {table_name} - {company_name} ({current_cell}/{total_cells})")
        
        # Guardar la fila completa para esta tabla
        matrix_data[table_name] = row_data
    
    progress_bar.empty()
    status_text.empty()
    
    # Convertir a DataFrame
    # matrix_data es un dict: {table: {company: timestamp}}
    # Al crear DataFrame(matrix_data).T obtenemos:
    #   - Filas (índices) = tablas
    #   - Columnas = compañías
    #   - Valores = timestamps
    matrix_df = pd.DataFrame(matrix_data).T
    matrix_df.index.name = 'Tabla'
    
    return matrix_df

# ========== FORMATO PARA VISUALIZACIÓN ==========

def format_timestamp_for_display(ts):
    """
    Formatea el timestamp para mostrar en la matriz con colores según antigüedad.
    
    - 🟢 Verde: Últimas 24 horas
    - 🟡 Amarillo: 1-7 días
    - 🔴 Rojo: Más de 7 días
    - ❌ Sin datos
    """
    if ts is None or pd.isna(ts):
        return "❌"
    
    # Convertir a datetime si es necesario
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    
    # Calcular hace cuánto tiempo fue la última sincronización
    try:
        # Normalizar timezone
        if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
            now = datetime.now(ts.tzinfo)
        else:
            now = datetime.now()
            if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                ts = ts.replace(tzinfo=None)
        
        time_diff = now - ts
        
        # Formatear según antigüedad
        if time_diff.days > 7:
            return f"🔴 {ts.strftime('%Y-%m-%d')}"
        elif time_diff.days > 1:
            return f"🟡 {ts.strftime('%m-%d %H:%M')}"
        else:
            return f"🟢 {ts.strftime('%m-%d %H:%M')}"
    except Exception:
        # Si hay error al formatear, mostrar solo la fecha
        try:
            return f"📅 {ts.strftime('%Y-%m-%d')}"
        except:
            return str(ts)

# ========== INTERFAZ STREAMLIT ==========

st.title("📊 Dashboard de Monitoreo ETL ServiceTitan")
st.markdown("**Matriz: Tablas (Y) vs Compañías (X) - Último MAX(_etl_synced)**")
st.markdown("---")

# Sidebar con controles
with st.sidebar:
    st.header("⚙️ Configuración")
    st.markdown(f"**Proyecto activo:** {get_project_source()}")
    st.markdown(f"**Project ID:** {get_bigquery_project_id()}")
    
    if st.button("🔄 Actualizar Datos", type="primary"):
        st.cache_data.clear()

# ========== PASO 1: CARGAR COMPAÑÍAS ==========
st.subheader("📋 Paso 1: Cargando Compañías...")
companies_df = get_companies()

if companies_df.empty:
    st.error("❌ No se encontraron compañías. Verifica la conexión a BigQuery.")
    st.stop()

st.success(f"✅ {len(companies_df)} compañías encontradas")
with st.expander("Ver lista de compañías"):
    st.dataframe(companies_df, use_container_width=True)

# ========== PASO 2: CARGAR TABLAS ==========
st.subheader("📋 Paso 2: Cargando Tablas desde Metadata...")
tables_list = get_tables_from_metadata()

if not tables_list:
    st.error("❌ No se encontraron tablas en metadata. Verifica la conexión.")
    st.stop()

st.success(f"✅ {len(tables_list)} tablas encontradas en metadata")
with st.expander("Ver lista de tablas"):
    st.write(tables_list)

# ========== PASO 3-4: CONSTRUIR Y MOSTRAR MATRIZ ==========
st.subheader("📊 Paso 3-4: Construyendo Matriz de Sincronización...")
st.info("⏳ Esto puede tomar varios minutos. Consultando MAX(_etl_synced) para cada combinación tabla-compañía...")

# Construir la matriz
matrix_df = build_sync_matrix(companies_df, tables_list)

# Mostrar matriz
st.subheader("📊 Matriz: Tablas vs Compañías (MAX(_etl_synced))")

# Crear versión formateada para visualización
display_df = matrix_df.copy()
for col in display_df.columns:
    display_df[col] = display_df[col].apply(format_timestamp_for_display)

# Mostrar la matriz
st.dataframe(
    display_df,
    use_container_width=True,
    height=600
)

# ========== ESTADÍSTICAS ==========
st.subheader("📈 Estadísticas")
col1, col2, col3 = st.columns(3)

with col1:
    total_cells = len(tables_list) * len(companies_df)
    synced_cells = matrix_df.notna().sum().sum()
    st.metric("Tablas Sincronizadas", f"{synced_cells}/{total_cells}")

with col2:
    recent_syncs = 0
    now = datetime.now()
    for col in matrix_df.columns:
        for val in matrix_df[col]:
            if val is not None and not pd.isna(val):
                try:
                    if isinstance(val, pd.Timestamp):
                        val = val.to_pydatetime()
                    if hasattr(val, 'tzinfo') and val.tzinfo is not None:
                        val = val.replace(tzinfo=None)
                    time_diff = now - val
                    if time_diff.days <= 1:
                        recent_syncs += 1
                except:
                    pass
    st.metric("Sincronizadas últimas 24h", recent_syncs)

with col3:
    missing_cells = total_cells - synced_cells
    st.metric("Tablas Faltantes", missing_cells)
