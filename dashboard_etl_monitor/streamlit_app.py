"""
Dashboard de Monitoreo ETL ServiceTitan
Matriz: Compañías (Y) vs Tablas (X) con MAX(_etl_synced)
Monitoreo de todas las tablas de Bronze
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os
import concurrent.futures
import pytz
from datetime import timedelta

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

def get_bigquery_client(project_id):
    """
    Crea un cliente BigQuery.
    La cuenta de servicio se configura a nivel de Cloud Run, no aquí.
    
    Args:
        project_id: ID del proyecto de BigQuery
        
    Retorna:
        Cliente BigQuery
    """
    return bigquery.Client(project=project_id)

def to_cdmx(ts):
    """
    Convierte un timestamp (aware o naive) a la zona horaria de Ciudad de México (America/Mexico_City).
    """
    if ts is None or pd.isna(ts):
        return None
    
    # Asegurar que sea aware (si es naive, asumir UTC)
    if ts.tzinfo is None:
        ts = pytz.utc.localize(ts)
    
    # Convertir a Ciudad de México
    cdmx = pytz.timezone('America/Mexico_City')
    return ts.astimezone(cdmx)

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
        client = get_bigquery_client(PROJECT_ID)
        
        query = f"""
            SELECT 
                company_id,
                company_name,
                company_project_id
            FROM `{PROJECT_ID}.settings.companies`
            WHERE company_fivetran_status = TRUE
            ORDER BY company_id
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
    Obtiene la lista de endpoint.name activos desde metadata.
    Estos son los que el ETL procesa y monitorea.
    
    Retorna:
        list: [endpoint_name, ...] ordenados alfabéticamente
    """
    try:
        client = get_bigquery_client(METADATA_PROJECT)
        
        query = f"""
            SELECT 
                endpoint.name AS endpoint_name
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE endpoint.name IS NOT NULL
              AND active = TRUE
            ORDER BY endpoint.name
        """
        
        df = client.query(query).to_dataframe()
        return sorted(df['endpoint_name'].tolist())
        
    except Exception as e:
        st.error(f"❌ Error obteniendo endpoints desde metadata: {str(e)}")
        return []

@st.cache_data(ttl=900)  # Cache por 15 minutos para la carga rápida
def get_snapshot_matrix(debug_mode=False):
    """
    Obtiene la última fotografía completa desde la tabla de snapshot.
    """
    try:
        # CORREGIDO: Usar METADATA_PROJECT (pph-central), igual que get_tables_from_metadata
        # Antes usaba get_bigquery_project_id() que devuelve el proyecto del ambiente (ej: platform-partners-des)
        # y si ese proyecto no tiene permisos sobre pph-central, falla silenciosamente.
        client = get_bigquery_client(METADATA_PROJECT)
        
        query = f"""
            SELECT 
                company_id,
                endpoint_name,
                max_sync,
                actual_rows,
                actual_duration,
                actual_status,
                last_rows,
                last_duration,
                last_status,
                updated_at
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.etl_monitoring_snapshot`
        """
        
        df = client.query(query).to_dataframe()
        return df
        
    except Exception as e:
        if debug_mode:
            st.error(f"🔍 Error en BigQuery (Snapshot): {type(e).__name__} - {str(e)}")
            # Mostrar la query para verificar el path de la tabla
            st.code(query, language="sql")
        return pd.DataFrame()

# ========== PASO 3: OBTENER MAX(_etl_synced) POR TABLA ==========

def get_last_sync_timestamp(project_id, table_name, debug_mode=False):
    """
    Obtiene el MAX(_etl_synced) de una tabla Bronze en un proyecto específico.
    Usa exactamente la misma query que funciona en BigQuery Studio.
    
    Args:
        project_id: ID del proyecto de BigQuery (ej: "company-project-123")
        table_name: Nombre de la tabla en dataset 'bronze' (ej: "business_unit")
        debug_mode: Si es True, captura y retorna información de errores
        
    Retorna:
        datetime con el último timestamp de sincronización, o None si no existe
        Si debug_mode=True y hay error, retorna (None, error_info)
    """
    error_info = None
    try:
        # Validar parámetros
        if not project_id or not table_name:
            if debug_mode:
                error_info = f"Parámetros inválidos: project_id={project_id}, table_name={table_name}"
                return None, error_info
            return None
        
        # Crear cliente BigQuery con la cuenta de servicio correcta
        client = get_bigquery_client(project_id)
        
        # Query exacta que funciona en BigQuery Studio
        # Formato: `project_id.dataset.table_name`
        table_ref = f"{project_id}.bronze.{table_name}"
        query = f"""
            SELECT MAX(_etl_synced) as max_sync
            FROM `{table_ref}`
            WHERE _etl_synced IS NOT NULL
        """
        
        # Ejecutar query con configuración explícita
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        
        query_job = client.query(query, job_config=job_config)
        
        # Esperar a que termine y obtener resultado
        result = query_job.result().to_dataframe()
        
        # Verificar resultado
        if result.empty:
            if debug_mode:
                error_info = f"Query ejecutada pero resultado vacío: {table_ref}"
                return None, error_info
            return None
        
        max_sync_value = result.iloc[0]['max_sync']
        
        # Si es None o NaN, retornar None
        if max_sync_value is None or pd.isna(max_sync_value):
            if debug_mode:
                error_info = f"Query ejecutada pero max_sync es NULL: {table_ref}"
                return None, error_info
            return None
        
        # Convertir a datetime y retornar
        return pd.to_datetime(max_sync_value)
        
    except NotFound as e:
        # Tabla no existe
        if debug_mode:
            error_info = f"Tabla no encontrada: {project_id}.bronze.{table_name} - {str(e)}"
            return None, error_info
        return None
    except Exception as e:
        # Cualquier otro error (permisos, campo no existe, etc.)
        if debug_mode:
            error_info = f"Error en {project_id}.bronze.{table_name}: {type(e).__name__} - {str(e)}"
            return None, error_info
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
        tables_list: Lista de nombres de tablas de Bronze
        debug_mode: Si es True, muestra información detallada de errores
        
    Retorna:
        DataFrame con:
            - Índices (filas) = nombres de compañías
            - Columnas = nombres de tablas
            - Valores = timestamps de MAX(_etl_synced) o None
    """
    # Estructura inicializada
    matrix_data = {company['company_name']: {} for _, company in companies_df.iterrows()}
    error_log = []  # Para registrar errores si debug_mode está activo
    sql_log = []  # Para registrar las queries SQL ejecutadas
    
    # Crear mapeo de company_name a company_id para ordenar después
    company_id_map = dict(zip(companies_df['company_name'], companies_df['company_id']))
    
    # Preparar lista de tareas
    tasks = []
    for _, company in companies_df.iterrows():
        company_name = company['company_name']
        project_id = company['company_project_id']
        for table_name in tables_list:
            tasks.append({
                'company_name': company_name,
                'project_id': project_id,
                'table_name': table_name
            })
            
    # Barra de progreso
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_cells = len(tasks)
    current_cell = 0
    
    # Función auxiliar para el hilo
    def _fetch_task(task):
        res = get_last_sync_timestamp(task['project_id'], task['table_name'], debug_mode=debug_mode)
        return task, res
        
    # Procesamiento en paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(_fetch_task, task): task for task in tasks}
        
        for future in concurrent.futures.as_completed(futures):
            task, result = future.result()
            company_name = task['company_name']
            table_name = task['table_name']
            project_id = task['project_id']
            
            # Si debug_mode, reconstruimos el log
            if debug_mode:
                table_ref = f"{project_id}.bronze.{table_name}"
                sql_query = f"SELECT MAX(_etl_synced) as max_sync FROM `{table_ref}` WHERE _etl_synced IS NOT NULL"
                sql_log.append(f"**{company_name} - {table_name}**\n```sql\n{sql_query}\n```\n")
            
            # Manejar resultado (puede ser tuple si debug_mode está activo)
            if isinstance(result, tuple):
                last_sync, error_msg = result
                if error_msg:
                    error_log.append(f"{company_name} - {table_name}: {error_msg}")
            else:
                last_sync = result
            
            # Guardamos el resultado en la matriz
            matrix_data[company_name][table_name] = last_sync
            
            # Actualizar progreso
            current_cell += 1
            progress = current_cell / total_cells
            progress_bar.progress(progress)
            status_text.text(f"Procesando en paralelo: {company_name} - {table_name} ({current_cell}/{total_cells})")
    
    
    progress_bar.empty()
    status_text.empty()
    
    # Mostrar SQL y errores si debug_mode está activo
    if debug_mode:
        with st.expander("🔍 Debug - Queries SQL Ejecutadas", expanded=True):
            if sql_log:
                for sql_entry in sql_log:
                    st.markdown(sql_entry)
                    st.markdown("---")
            else:
                st.text("No se ejecutaron queries")
        
        if error_log:
            with st.expander("🔍 Debug - Errores Detectados", expanded=True):
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
    
    # Ordenar por company_id (mantener el orden de companies_df)
    # Crear una columna temporal con company_id para ordenar
    matrix_df['_sort_order'] = matrix_df.index.map(company_id_map)
    matrix_df = matrix_df.sort_values('_sort_order')
    matrix_df = matrix_df.drop('_sort_order', axis=1)
    
    # IMPORTANTE: Reordenar las columnas (Endpoints) en el orden alfabético original
    # (El paralelismo altera el orden de inserción y las columnas salen desordenadas)
    columns_ordered = [t for t in tables_list if t in matrix_df.columns]
    matrix_df = matrix_df[columns_ordered]
    
    return matrix_df

# ========== FORMATO PARA VISUALIZACIÓN ==========

def format_cell_data(data, show_rows=True, show_duration=True, show_delta=True):
    """
    Formatea la celda:
      Línea 1: 🟢 04-16 14:30   (icono + fecha en CDMX)
      Línea 2: Δ:+150 | τ:+2s  (métricas, solo si hay datos reales)
    """
    if not isinstance(data, dict) or data.get('max_sync') is None or pd.isna(data.get('max_sync')):
        return "❌"
    
    # Línea 1: Icono + Fecha
    ts = to_cdmx(data['max_sync'])
    sts = data.get('actual_status')
    status = str(sts).upper() if not pd.isna(sts) else ''
    
    if status == 'SUCCESS':
        icon = "🟢"
    elif status == 'FAILED':
        icon = "🔴"
    else:
        diff = datetime.now(ts.tzinfo) - ts
        icon = "🔴" if diff.days >= 2 else ("🟡" if diff.days >= 1 else "🟢")
    
    line1 = f"{icon} {ts.strftime('%m-%d %H:%M')}"
    
    # Línea 2: métricas (solo si hay datos reales, excluyendo Null, NaN, pd.NA)
    parts = []
    
    act_r = data.get('actual_rows')
    lst_r = data.get('last_rows')
    if show_rows and not pd.isna(act_r):
        if show_delta and not pd.isna(lst_r):
            d = int(act_r) - int(lst_r)
            parts.append(f"Δ:{'+' if d >= 0 else ''}{d}")
        else:
            parts.append(f"R:{int(act_r)}")
    
    act_d = data.get('actual_duration')
    lst_d = data.get('last_duration')
    if show_duration and not pd.isna(act_d):
        if show_delta and not pd.isna(lst_d):
            d = float(lst_d) - float(act_d)
            parts.append(f"{'⚠️' if d < 0 else 'τ:'}{'+' if d >= 0 else ''}{d:.1f}s")
        else:
            parts.append(f"τ:{float(act_d):.1f}s")
    
    return f"{line1}\n{' | '.join(parts)}" if parts else line1

# ========== INTERFAZ STREAMLIT ==========

# CSS personalizado para reducir tamaños de texto y eliminar scroll
st.markdown("""
    <style>
    h1 {font-size: 1.5rem !important;}
    h2 {font-size: 1.2rem !important;}
    h3 {font-size: 1rem !important;}
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {margin-top: 0.5rem; margin-bottom: 0.5rem;}
    /* Reducir interlineado en sidebar */
    [data-testid="stSidebar"] .stMarkdown {margin: 0.2rem 0 !important; line-height: 1.2 !important;}
    [data-testid="stSidebar"] p {margin: 0.1rem 0 !important; font-size: 0.85rem !important;}
    [data-testid="stSidebar"] .stCaption {margin: 0.1rem 0 !important; font-size: 0.75rem !important;}
    
    /* Configuración para que la tabla quepa sin scroll (Vista de Pájaro) */
    [data-testid="stTable"] {overflow: visible !important; display: flex !important; justify-content: center !important;}
    table {font-size: 0.7rem !important; width: 100% !important; border-collapse: collapse !important;}
    th {font-size: 0.65rem !important; padding: 0.1rem 0.2rem !important; white-space: nowrap !important; text-align: center !important;}
    td {padding: 0.1rem 0.2rem !important; white-space: pre-wrap !important; text-align: center !important; line-height: 1.1 !important;}
    </style>
""", unsafe_allow_html=True)

# Título con ambiente (reducido)
current_env = get_current_environment().upper()
st.markdown(f"### 📊 Dashboard ETL ServiceTitan - {current_env}")
st.markdown("**Compañías (Y) vs Tablas Bronze (X) - MAX(_etl_synced)**")
st.markdown("---")

# Sidebar con controles y logs
with st.sidebar:
    st.markdown("#### ⚙️ Configuración")
    
    # Información del ambiente
    current_env = get_current_environment().upper()
    project_name = get_project_source()
    project_id = get_bigquery_project_id()
    
    st.markdown("##### 🌍 Ambiente")
    st.markdown(f"**Ambiente:** `{current_env}`")
    st.markdown(f"**Project:** `{project_name}`")
    st.markdown(f"**ID:** `{project_id}`")
    
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
    st.markdown("##### 🔧 Opciones")
    st.caption(f"Ambiente: **{current_env}**")
    
    # NUEVOS: Selectores de métricas
    st.markdown("##### 📊 Visualización")
    show_rows = st.checkbox("Mostrar Filas (Δ)", value=True)
    show_duration = st.checkbox("Mostrar Duración (τ)", value=True)
    show_delta = st.checkbox("Mostrar Diferenciales", value=True, help="Muestra la resta contra la ejecución anterior")
    
    # Modo debug
    debug_mode = st.checkbox("🔍 Modo Debug", value=False, help="Muestra información detallada de errores cuando aparecen ❌")
    
    if st.button("🔄 Actualizar Datos (LIVE)", type="primary"):
        # Al presionar, forzamos limpieza pero el recálculo será paralelo
        st.cache_data.clear()
        st.session_state['data_source'] = 'live'
        st.rerun()
    
    st.markdown("---")
    
    # DIAGNÓSTICO DIRECTO - Para depurar el problema de X
    with st.expander("🩺 Diagnóstico Snapshot", expanded=False):
        if st.button("🔎 Ver datos crudos del Snapshot"):
            try:
                diag_client = bigquery.Client(project=METADATA_PROJECT)
                diag_query = f"""
                    SELECT company_id, endpoint_name, max_sync, actual_status
                    FROM `{METADATA_PROJECT}.{METADATA_DATASET}.etl_monitoring_snapshot`
                    LIMIT 20
                """
                diag_df = diag_client.query(diag_query).to_dataframe()
                if diag_df.empty:
                    st.error("❌ La tabla etl_monitoring_snapshot está VACÍA")
                else:
                    st.success(f"✅ {len(diag_df)} registros encontrados")
                    st.write(diag_df)
                    st.write("**company_id dtype:**", diag_df['company_id'].dtype)
                    st.write("**max_sync nulos:**", diag_df['max_sync'].isna().sum())
            except Exception as e:
                st.error(f"Error: {type(e).__name__}: {str(e)}")
    
    st.markdown("##### 📋 Logs")

    
    # ========== PASO 1: CARGAR COMPAÑÍAS ==========
    st.caption("📋 Paso 1: Cargando Compañías...")
    companies_df = get_companies()
    
    if companies_df.empty:
        st.caption("❌ No se encontraron compañías")
        st.stop()
    else:
        st.caption(f"✅ {len(companies_df)} compañías encontradas")
    
    # ========== PASO 2: CARGAR ENDPOINTS ==========
    st.caption("📋 Paso 2: Cargando Endpoints desde Metadata...")
    tables_list = get_tables_from_metadata()  # Lista de endpoint.name
    
    if not tables_list:
        st.caption("❌ No se encontraron endpoints en metadata")
        st.stop()
    else:
        st.caption(f"✅ {len(tables_list)} endpoints encontrados en metadata")
    
# ========== PROCESAMIENTO E INTERFAZ ==========

# 1. Decidir origen de datos
if 'data_source' not in st.session_state:
    st.session_state['data_source'] = 'snapshot'

# 2. Cargar datos base
with st.spinner("Cargando matriz..."):
    if st.session_state['data_source'] == 'live':
        matrix_df = build_sync_matrix(companies_df, tables_list, debug_mode=debug_mode)
        # Convertir a formato dict para el formateador
        processed_matrix = matrix_df.applymap(lambda x: {'max_sync': x})
    else:
        snapshot_df = get_snapshot_matrix(debug_mode=debug_mode)
        
        # Validar si el snapshot tiene datos
        if snapshot_df.empty:
            st.warning("⚠️ No se encontraron registros en la tabla de snapshot. Realizando carga LIVE...")
            matrix_df = build_sync_matrix(companies_df, tables_list, debug_mode=debug_mode)
            processed_matrix = matrix_df.applymap(lambda x: {'max_sync': x})
        else:
            # NORMALIZACIÓN ROBUSTA DE IDs (Convertir a String y quitar .0 si existe)
            snapshot_df['id_clean'] = snapshot_df['company_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            companies_df['id_clean'] = companies_df['company_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
            # Crear mapeo
            id_to_name = dict(zip(companies_df['id_clean'], companies_df['company_name']))
            
            # Mapear compañías
            snapshot_df['Compañía'] = snapshot_df['id_clean'].map(id_to_name)
            
            # Contar coincidencias
            mapped_rows = snapshot_df[snapshot_df['Compañía'].notna()].copy()
            
            if len(mapped_rows) == 0:
                st.error("❌ Error de Mapeo Crítico: Ningún ID del Snapshot coincide con las compañías activas.")
                if debug_mode:
                    st.write("IDs en Snapshot (Clean):", snapshot_df['id_clean'].unique())
                    st.write("IDs en Catálogo (Clean):", companies_df['id_clean'].unique())
                
                st.info("💡 Cambiando automáticamente a modo LIVE para obtener datos frescos...")
                matrix_df = build_sync_matrix(companies_df, tables_list, debug_mode=debug_mode)
                processed_matrix = matrix_df.applymap(lambda x: {'max_sync': x})
            else:
                if debug_mode:
                    st.success(f"✅ Snapshot vinculado: {len(mapped_rows)} registros coinciden con compañías.")
                    with st.expander("📦 Vista previa datos Snapshot vinculados", expanded=False):
                        st.write(mapped_rows.head(20))
                
                # Normalizar endpoint_name del snapshot (lowercase + strip)
                # El snapshot guarda directamente el endpoint.name del proceso ETL
                mapped_rows['ep_key'] = (
                    mapped_rows['endpoint_name']
                    .astype(str)
                    .str.lower()
                    .str.strip()
                )
                
                # Generar matriz usando endpoint_name directamente como columna
                pivoted = {}
                for _, row in mapped_rows.iterrows():
                    comp = row['Compañía']
                    ep = row['ep_key']
                    if not ep: continue
                    if comp not in pivoted: pivoted[comp] = {}
                    pivoted[comp][ep] = {
                        'max_sync': row['max_sync'],
                        'actual_rows': row['actual_rows'],
                        'last_rows': row['last_rows'],
                        'actual_duration': row['actual_duration'],
                        'last_duration': row['last_duration'],
                        'actual_status': row['actual_status']
                    }
                
                processed_matrix = pd.DataFrame(pivoted).T
                
                # Columnas: unión de endpoints del snapshot + endpoints de metadata
                # Ambos ya normalizados (lowercase)
                metadata_eps = sorted([e.lower().strip() for e in tables_list])
                snapshot_eps = sorted(processed_matrix.columns.tolist())
                all_cols = sorted(set(metadata_eps) | set(snapshot_eps))
                
                for col in all_cols:
                    if col not in processed_matrix.columns:
                        processed_matrix[col] = None
                
                processed_matrix = processed_matrix[all_cols]
                
                # Ordenar filas por company_id
                company_id_map = dict(zip(companies_df['company_name'], companies_df['company_id']))
                processed_matrix['_sort'] = processed_matrix.index.map(company_id_map)
                processed_matrix = processed_matrix.sort_values('_sort').drop('_sort', axis=1)

# Mostrar matriz
st.markdown(f"**📊 Matriz: Compañías vs Tablas Bronze (Origen: {st.session_state['data_source'].upper()})**")
st.caption("Icono representa el estatus de la última corrida. Δ = Diferencia de filas. τ = Efectividad de tiempo (Positivo es mejor).")

# Crear versión formateada para visualización
display_df = processed_matrix.copy()
for col in display_df.columns:
    display_df[col] = display_df[col].apply(lambda x: format_cell_data(x, show_rows, show_duration, show_delta))

# Mostrar la matriz usando st.table() con CSS personalizado para evitar scroll
st.table(display_df)

# ========== ESTADÍSTICAS ==========
st.markdown("**📈 Estadísticas**")
col1, col2, col3 = st.columns(3)

# Función auxiliar para contar celdas válidas (que tienen max_sync)
def is_synced(cell):
    return cell is not None and isinstance(cell, dict) and cell.get('max_sync') is not None and not pd.isna(cell.get('max_sync'))

with col1:
    total_cells = len(tables_list) * len(companies_df)
    # Contar celdas que tienen data
    synced_cells = 0
    for col in processed_matrix.columns:
        synced_cells += processed_matrix[col].apply(is_synced).sum()
    st.metric("Tablas Sincronizadas", f"{synced_cells}/{total_cells}")

with col2:
    recent_syncs = 0
    now = datetime.now(pytz.utc) # Comparar en UTC
    for col in processed_matrix.columns:
        for val in processed_matrix[col]:
            if is_synced(val):
                try:
                    ts = val['max_sync']
                    if isinstance(ts, pd.Timestamp):
                        ts = ts.to_pydatetime()
                    
                    # Asegurar que ts sea aware
                    if ts.tzinfo is None:
                        ts = pytz.utc.localize(ts)
                    
                    time_diff = now - ts
                    if time_diff.days <= 1:
                        recent_syncs += 1
                except:
                    pass
    st.metric("Sincronizadas últimas 24h", recent_syncs)

with col3:
    missing_cells = total_cells - synced_cells
    st.metric("Tablas Faltantes", missing_cells)
