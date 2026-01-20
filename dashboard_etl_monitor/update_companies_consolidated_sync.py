"""
Script para actualizar last_etl_synced y row_count en companies_consolidated

Este script:
1. Obtiene las 11 tablas de Bronze desde metadata (silver_use_bronze = TRUE)
2. Obtiene combinaciones company_id + table_name desde companies_consolidated (solo para las 11 tablas)
3. Para cada combinación, obtiene el company_project_id
4. Calcula MAX(_etl_synced) y COUNT(*) desde {company_project_id}.bronze.{table_name}
5. Actualiza companies_consolidated con esos valores

Ejecutar como Scheduled Query o Cloud Function:
- Horarios: 7am, 1pm, 7pm, 1am (1 hora después del ETL)
"""

from google.cloud import bigquery
from datetime import datetime
import logging

# Configuración
CENTRAL_PROJECT = "pph-central"
CENTRAL_DATASET = "settings"
CONSOLIDATED_TABLE = "companies_consolidated"
COMPANIES_TABLE = "companies"  # Se buscará en cada project_id
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_bronze_tables(client):
    """
    Obtiene las 11 tablas de Bronze desde metadata (las que tienen silver_use_bronze = TRUE).
    
    Retorna:
        Lista de nombres de tablas (máximo 11)
    """
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
    
    try:
        df = client.query(query).to_dataframe()
        tables = df['table_name'].tolist()
        logger.info(f"📋 Encontradas {len(tables)} tablas de Bronze en metadata")
        return tables[:11] if len(tables) > 11 else tables
    except Exception as e:
        logger.error(f"❌ Error obteniendo tablas desde metadata: {str(e)}")
        return []


def get_all_combinations(client, bronze_tables):
    """
    Obtiene las combinaciones company_id + table_name desde companies_consolidated,
    pero SOLO para las tablas de Bronze (las 11 tablas).
    Luego obtiene el company_project_id para cada company_id desde cualquiera de los proyectos.
    
    Args:
        client: Cliente BigQuery
        bronze_tables: Lista de nombres de tablas de Bronze (las 11 tablas)
    
    Retorna:
        Lista de dicts: [{
            'company_id': int,
            'table_name': str,
            'company_project_id': str
        }]
    """
    if not bronze_tables:
        logger.warning("⚠️ No hay tablas de Bronze para procesar")
        return []
    
    # Filtrar solo las combinaciones de las 11 tablas de Bronze
    tables_list = "', '".join(bronze_tables)
    query_combinations = f"""
        SELECT DISTINCT
            company_id,
            table_name
        FROM `{CENTRAL_PROJECT}.{CENTRAL_DATASET}.{CONSOLIDATED_TABLE}`
        WHERE table_name IN ('{tables_list}')
        ORDER BY company_id, table_name
    """
    
    try:
        df_combinations = client.query(query_combinations).to_dataframe()
        if df_combinations.empty:
            logger.warning("⚠️ No se encontraron combinaciones en companies_consolidated")
            return []
        
        logger.info(f"📋 Encontradas {len(df_combinations)} combinaciones en companies_consolidated")
        
        # Obtener company_project_id para cada company_id único
        # Intentar desde cada proyecto hasta encontrar el company_id
        unique_company_ids = df_combinations['company_id'].unique().tolist()
        company_project_map = {}
        
        environments = [
            "platform-partners-des",
            "platform-partners-qua",
            "constant-height-455614-i0"
        ]
        
        for env_project_id in environments:
            if len(company_project_map) == len(unique_company_ids):
                break  # Ya encontramos todos
            
            query_companies = f"""
                SELECT 
                    company_id,
                    company_project_id
                FROM `{env_project_id}.{CENTRAL_DATASET}.{COMPANIES_TABLE}`
                WHERE company_fivetran_status = TRUE
                  AND company_id IN ({','.join(map(str, unique_company_ids))})
            """
            
            try:
                df_companies = client.query(query_companies).to_dataframe()
                for _, row in df_companies.iterrows():
                    if row['company_id'] not in company_project_map:
                        company_project_map[row['company_id']] = row['company_project_id']
            except Exception as e:
                logger.warning(f"⚠️ No se pudo obtener companies desde {env_project_id}: {str(e)}")
                continue
        
        # Combinar resultados
        results = []
        for _, row in df_combinations.iterrows():
            company_id = row['company_id']
            table_name = row['table_name']
            company_project_id = company_project_map.get(company_id)
            
            if company_project_id:
                results.append({
                    'company_id': company_id,
                    'table_name': table_name,
                    'company_project_id': company_project_id
                })
            else:
                logger.warning(f"⚠️ No se encontró company_project_id para company_id={company_id}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo combinaciones: {str(e)}")
        return []


def get_sync_data(client, company_project_id, table_name):
    """
    Obtiene MAX(_etl_synced) y COUNT(*) desde una tabla bronze específica.
    
    Args:
        client: Cliente BigQuery
        company_project_id: ID del proyecto de la compañía
        table_name: Nombre de la tabla en bronze
        
    Retorna:
        dict: {
            'max_sync': datetime o None,
            'row_count': int
        }
    """
    table_ref = f"{company_project_id}.bronze.{table_name}"
    
    query = f"""
        SELECT 
            MAX(_etl_synced) as max_sync,
            COUNT(*) as row_count
        FROM `{table_ref}`
        WHERE _etl_synced IS NOT NULL
    """
    
    try:
        result = client.query(query).to_dataframe()
        if result.empty or result.iloc[0]['max_sync'] is None:
            return {'max_sync': None, 'row_count': 0}
        
        return {
            'max_sync': result.iloc[0]['max_sync'],
            'row_count': int(result.iloc[0]['row_count'])
        }
    except Exception as e:
        # Si la tabla no existe, solo loguear y retornar None (no es un error crítico)
        error_msg = str(e)
        if "not found" in error_msg.lower() or "notfound" in error_msg.lower():
            logger.debug(f"ℹ️  Tabla {table_ref} no existe en Bronze (puede ser normal)")
        else:
            logger.warning(f"⚠️ Error obteniendo sync data para {table_ref}: {error_msg}")
        return {'max_sync': None, 'row_count': 0}


def update_companies_consolidated(client, company_id, table_name, max_sync, row_count):
    """
    Actualiza last_etl_synced y row_count en companies_consolidated.
    
    Args:
        client: Cliente BigQuery
        company_id: ID de la compañía
        table_name: Nombre de la tabla
        max_sync: Timestamp de última sincronización
        row_count: Cantidad de filas
    """
    table_ref = f"{CENTRAL_PROJECT}.{CENTRAL_DATASET}.{CONSOLIDATED_TABLE}"
    
    # Construir query MERGE
    if max_sync is None:
        max_sync_sql = "NULL"
    else:
        max_sync_sql = f"TIMESTAMP('{max_sync.isoformat()}')"
    
    query = f"""
        MERGE `{table_ref}` cc
        USING (
            SELECT 
                {company_id} as company_id,
                '{table_name}' as table_name,
                {max_sync_sql} as max_sync,
                {row_count} as row_count
        ) sync_data
        ON cc.company_id = sync_data.company_id 
            AND cc.table_name = sync_data.table_name
        WHEN MATCHED THEN
            UPDATE SET
                last_etl_synced = sync_data.max_sync,
                row_count = sync_data.row_count,
                updated_at = CURRENT_TIMESTAMP()
    """
    
    try:
        client.query(query).result()
        logger.info(f"✅ Actualizado: company_id={company_id}, table={table_name}")
    except Exception as e:
        logger.error(f"❌ Error actualizando {company_id}/{table_name}: {str(e)}")


def main():
    """
    Función principal que ejecuta el proceso completo.
    """
    logger.info("🚀 Iniciando actualización de companies_consolidated...")
    
    # Crear cliente BigQuery
    # NOTA: Asegúrate de que la cuenta de servicio tenga permisos
    #       en todos los proyectos (pph-central y los company_project_id)
    client = bigquery.Client(project=CENTRAL_PROJECT)
    
    # Obtener las 11 tablas de Bronze desde metadata
    logger.info("📊 Obteniendo tablas de Bronze desde metadata...")
    bronze_tables = get_bronze_tables(client)
    
    if not bronze_tables:
        logger.error("❌ No se encontraron tablas de Bronze en metadata")
        return
    
    logger.info(f"✅ Tablas de Bronze a procesar: {', '.join(bronze_tables)}")
    
    # Obtener combinaciones solo para las tablas de Bronze
    logger.info("📊 Obteniendo combinaciones company_id + table_name (solo tablas de Bronze)...")
    combinations = get_all_combinations(client, bronze_tables)
    
    if not combinations:
        logger.error("❌ No se encontraron combinaciones para procesar")
        return
    
    logger.info(f"📋 Encontradas {len(combinations)} combinaciones para procesar")
    
    total_updated = 0
    total_errors = 0
    
    # Procesar cada combinación
    for combo in combinations:
        company_id = combo['company_id']
        table_name = combo['table_name']
        company_project_id = combo['company_project_id']
        
        logger.info(f"🔄 Procesando: company_id={company_id}, table={table_name}, project={company_project_id}")
        
        # Obtener datos de sincronización
        sync_data = get_sync_data(client, company_project_id, table_name)
        
        # Actualizar companies_consolidated
        try:
            update_companies_consolidated(
                client,
                company_id,
                table_name,
                sync_data['max_sync'],
                sync_data['row_count']
            )
            total_updated += 1
        except Exception as e:
            logger.error(f"❌ Error procesando {company_id}/{table_name}: {str(e)}")
            total_errors += 1
    
    logger.info(f"✅ Proceso completado: {total_updated} actualizados, {total_errors} errores")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Error fatal en el proceso: {str(e)}", exc_info=True)
        raise  # Re-lanzar para que Cloud Run Job marque como fallido
