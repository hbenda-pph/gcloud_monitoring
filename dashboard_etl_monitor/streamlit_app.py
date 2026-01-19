"""
Dashboard de Monitoreo ETL ServiceTitan
Matriz: Compañías (Y) vs Tablas (X) con MAX(_etl_synced)
Monitoreo de las 11 tablas de Bronze
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

# ========== CONFIGURACIÓN DE AMBIENTES ==========

# Mapeo de ambientes a project_ids
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

# ========== FUNCIONES AUXILIARES ==========

def detect_environment():
    """
    Detecta el ambiente actual (dev, qua, pro).
    
    Prioridad:
    1. Variable de entorno ENVIRONMENT
    2. Variable de entorno GCP_PROJECT o GOOGLE_CLOUD_PROJECT
    3. Cliente BigQuery
    4. Fallback a 'dev' (más seguro que qua para desarrollo local)
    
    Retorna:
        str: 'dev', 'qua' o 'pro'
    """
    # 1. Intentar desde variable de entorno explícita
    env = os.environ.get('ENVIRONMENT', '').lower()
    if env in ['dev', 'qua', 'pro']:
        return env
    
    # 2. Intentar desde project name o project_id
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if project:
        project_lower = project.lower()
        # Mapear project name/ID a environment
        # DEV: platform-partners-des o platform-partners-dev
        if 'platform-partners-des' in project_lower or ('dev' in project_lower and 'des' not in project_lower):
            return 'dev'
        # PRO: constant-height-455614-i0 o platform-partners-pro
        elif 'constant-height-455614-i0' in project_lower or ('pro' in project_lower and 'production' in project_lower):
            return 'pro'
        # QUA: platform-partners-qua
        elif 'qua' in project_lower or 'qa' in project_lower:
            return 'qua'
    
    # 3. Intentar desde cliente BigQuery
    try:
        client = bigquery.Client()
        project = client.project
        if project:
            project_lower = project.lower()
            # DEV: platform-partners-des
            if 'platform-partners-des' in project_lower:
                return 'dev'
            # PRO: constant-height-455614-i0
            elif 'constant-height-455614-i0' in project_lower:
                return 'pro'
            # QUA: platform-partners-qua
            elif 'platform-partners-qua' in project_lower:
                return 'qua'
            # Fallback por nombre
            elif 'dev' in project_lower:
                return 'dev'
            elif 'pro' in project_lower:
                return 'pro'
            elif 'qua' in project_lower or 'qa' in project_lower:
                return 'qua'
    except:
        pass
    
    # 4. Fallback a 'dev' (más seguro para desarrollo local)
    # Si no se puede detectar, asumir DEV en lugar de QUA
    return 'dev'

def get_environment_config():
    """
    Obtiene la configuración del ambiente actual.
    
    Retorna:
        dict: Configuración con project_name y project_id
    """
    env = detect_environment()
    # Si el ambiente detectado no existe en la config, usar 'dev' como fallback
    return ENVIRONMENT_CONFIG.get(env, ENVIRONMENT_CONFIG['dev'])

def get_project_source():
    """
    Obtiene el project_name del ambiente actual.
    (Nombre legible del proyecto)
    """
    config = get_environment_config()
    return config['project_name']

def get_bigquery_project_id():
    """
    Obtiene el project_id real para usar en queries SQL.
    (ID técnico del proyecto, puede diferir del nombre en PRO)
    """
    config = get_environment_config()
    return config['project_id']

def get_current_environment():
    """
    Obtiene el nombre del ambiente actual (dev/qua/pro).
    """
    return detect_environment()

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
    Obtiene las 11 tablas de Bronze desde metadata.
    
    Retorna:
        Lista de nombres de tablas ordenadas (máximo 11 tablas)
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
            LIMIT 11
        """
        
        df = client.query(query).to_dataframe()
        tables = df['table_name'].tolist()
        
        # Si hay más de 11, tomar solo las primeras 11
        return tables[:11] if len(tables) > 11 else tables
        
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
            table = client.get_table(table_ref)
        except NotFound:
            # Tabla no existe
            return None
        except Exception as e:
            # Error de permisos u otro error
            return None
        
        # Verificar que la tabla tenga filas
        count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{table_ref}`
        """
        
        try:
            count_result = client.query(count_query).to_dataframe()
            if count_result.empty or count_result.iloc[0]['row_count'] == 0:
                # Tabla existe pero está vacía
                return None
        except Exception:
            # Si no podemos contar, intentamos igual con MAX
            pass
        
        # Obtener MAX(_etl_synced) - usar COALESCE para manejar NULLs
        query = f"""
            SELECT MAX(_etl_synced) as max_sync
            FROM `{table_ref}`
            WHERE _etl_synced IS NOT NULL
        """
        
        try:
            result = client.query(query).to_dataframe()
            
            # Verificar que tenemos resultados
            if result.empty:
                return None
            
            max_sync_value = result.iloc[0]['max_sync']
            
            # Si es None o NaN, la tabla no tiene valores en _etl_synced
            if max_sync_value is None or pd.isna(max_sync_value):
                return None
            
            # Convertir a datetime
            return pd.to_datetime(max_sync_value)
            
        except Exception as query_error:
            # Error en la query - puede ser que el campo no exista o error de permisos
            # Intentar una query alternativa para verificar si el campo existe
            try:
                # Query para verificar estructura de la tabla
                schema_check = f"""
                    SELECT column_name 
                    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_name = '{table_name}' 
                      AND column_name = '_etl_synced'
                """
                schema_result = client.query(schema_check).to_dataframe()
                if schema_result.empty:
                    # El campo _etl_synced no existe en esta tabla
                    return None
            except:
                pass
            
            # Si llegamos aquí, hubo un error pero no sabemos exactamente qué
            return None
        
    except Exception as e:
        # Error general (permisos, conexión, etc.)
        return None

# ========== PASO 4: CONSTRUIR MATRIZ ==========

def build_sync_matrix(companies_df, tables_list, debug_mode=False):
    """
    Construye la matriz de sincronización: Compañías (filas) vs Tablas (columnas).
    
    Para cada combinación compañía-tabla:
    - Consulta MAX(_etl_synced) en {company_project_id}.bronze.{table_name}
    - Almacena el timestamp resultante
    
    Args:
        companies_df: DataFrame con compañías (debe tener company_project_id)
        tables_list: Lista de nombres de tablas (las 11 tablas de Bronze)
        debug_mode: Si es True, muestra información detallada de errores
        
    Retorna:
        DataFrame con:
            - Índices (filas) = nombres de compañías
            - Columnas = nombres de tablas
            - Valores = timestamps de MAX(_etl_synced) o None
    """
    # Estructura: {company_name: {table_name: timestamp}}
    matrix_data = {}
    error_log = []  # Para registrar errores si debug_mode está activo
    
    # Barra de progreso
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_cells = len(companies_df) * len(tables_list)
    current_cell = 0
    
    # Iterar sobre cada COMPAÑÍA (serán las FILAS de la matriz)
    for _, company in companies_df.iterrows():
        company_name = company['company_name']
        project_id = company['company_project_id']
        row_data = {}
        
        # Iterar sobre cada TABLA (serán las COLUMNAS de la matriz)
        for table_name in tables_list:
            # Obtener último timestamp de sincronización
            last_sync = get_last_sync_timestamp(project_id, table_name)
            row_data[table_name] = last_sync
            
            # Si es None y debug_mode, registrar el error
            if last_sync is None and debug_mode:
                table_ref = f"{project_id}.bronze.{table_name}"
                error_log.append(f"{company_name} - {table_name} ({table_ref}): No se pudo obtener MAX(_etl_synced)")
            
            # Actualizar progreso
            current_cell += 1
            progress = current_cell / total_cells
            progress_bar.progress(progress)
            status_text.text(f"Procesando: {company_name} - {table_name} ({current_cell}/{total_cells})")
        
        # Guardar la fila completa para esta compañía
        matrix_data[company_name] = row_data
    
    progress_bar.empty()
    status_text.empty()
    
    # Mostrar errores si hay y debug_mode está activo
    if debug_mode and error_log:
        with st.expander("🔍 Debug - Errores Detectados", expanded=False):
            for error in error_log:
                st.text(error)
    
    # Convertir a DataFrame
    # matrix_data es un dict: {company: {table: timestamp}}
    # Al crear DataFrame(matrix_data) obtenemos:
    #   - Filas (índices) = compañías
    #   - Columnas = tablas
    #   - Valores = timestamps
    matrix_df = pd.DataFrame(matrix_data).T
    matrix_df.index.name = 'Compañía'
    
    return matrix_df

# ========== FORMATO PARA VISUALIZACIÓN ==========

def format_timestamp_for_display(ts):
    """
    Formatea el timestamp para mostrar en la matriz con colores según antigüedad.
    
    - 🟢 Verde: Últimas 24 horas
    - 🟡 Amarillo: 1-7 días
    - 🔴 Rojo: Más de 7 días
    - ❌ Sin datos (tabla no existe o no tiene _etl_synced)
    """
    if ts is None or pd.isna(ts):
        return "❌"  # Tabla no existe o no tiene datos en _etl_synced
    
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

# Título con ambiente
current_env = get_current_environment().upper()
st.title(f"📊 Dashboard de Monitoreo ETL ServiceTitan - {current_env}")
st.markdown("**Matriz: Compañías (Y) vs Tablas Bronze (X) - Último MAX(_etl_synced)**")
st.markdown("---")

# Sidebar con controles
with st.sidebar:
    st.header("⚙️ Configuración")
    
    # Información del ambiente
    current_env = get_current_environment().upper()
    project_name = get_project_source()
    project_id = get_bigquery_project_id()
    
    st.markdown("### 🌍 Ambiente")
    st.markdown(f"**Ambiente detectado:** `{current_env}`")
    st.markdown(f"**Project Name:** `{project_name}`")
    st.markdown(f"**Project ID:** `{project_id}`")
    
    # Información de debug
    with st.expander("🔍 Debug - Variables de Entorno"):
        env_var = os.environ.get('ENVIRONMENT', 'No configurada')
        gcp_proj = os.environ.get('GCP_PROJECT', os.environ.get('GOOGLE_CLOUD_PROJECT', 'No configurada'))
        st.caption(f"ENVIRONMENT: `{env_var}`")
        st.caption(f"GCP_PROJECT: `{gcp_proj}`")
        try:
            bq_client = bigquery.Client()
            bq_proj = bq_client.project
            st.caption(f"BigQuery Client Project: `{bq_proj}`")
        except Exception as e:
            st.caption(f"BigQuery Client: Error - {str(e)}")
    
    st.markdown("---")
    
    # Selector de ambiente (solo informativo por ahora)
    st.markdown("### 🔧 Opciones")
    st.info(f"💡 Ambiente detectado automáticamente: **{current_env}**")
    st.caption("El ambiente se detecta desde variables de entorno o configuración GCP")
    
    # Modo debug
    debug_mode = st.checkbox("🔍 Modo Debug", value=False, help="Muestra información detallada de errores cuando aparecen ❌")
    
    if st.button("🔄 Actualizar Datos", type="primary"):
        st.cache_data.clear()
        st.rerun()

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

st.success(f"✅ {len(tables_list)} tablas de Bronze encontradas en metadata")
with st.expander("Ver lista de tablas de Bronze"):
    st.write(tables_list)
    if len(tables_list) > 11:
        st.warning(f"⚠️ Se encontraron {len(tables_list)} tablas, mostrando solo las primeras 11")

# ========== PASO 3-4: CONSTRUIR Y MOSTRAR MATRIZ ==========
st.subheader("📊 Paso 3-4: Construyendo Matriz de Sincronización...")
st.info("⏳ Esto puede tomar varios minutos. Consultando MAX(_etl_synced) para cada combinación tabla-compañía...")

# Construir la matriz
matrix_df = build_sync_matrix(companies_df, tables_list, debug_mode=debug_mode)

# Mostrar matriz
st.subheader("📊 Matriz: Compañías vs Tablas Bronze (MAX(_etl_synced))")
st.caption("❌ = Tabla no existe o no tiene datos en _etl_synced")

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
