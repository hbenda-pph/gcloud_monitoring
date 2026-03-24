#!/usr/bin/env python3
"""
Script: IAM Access Snapshot Sync
Función: Captura información de accesos en BigQuery y la almacena en una tabla
         para crear un histórico de auditoría
"""

import os
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from collections import defaultdict

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PermissionDenied
import logging

# ========== CONFIGURACIÓN LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== CONFIGURACIÓN ==========
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

# Proyecto de auditoría donde se guarda el histórico
AUDIT_PROJECT = "pph-central"
AUDIT_DATASET = "management"
AUDIT_TABLE_IAM_SNAPSHOT = "iam_access_snapshot"
AUDIT_TABLE_IAM_HISTORY = "iam_access_history"


# ========== ESQUEMA DE TABLAS ==========
SCHEMA_SNAPSHOT = [
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("snapshot_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dataset_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("principal_email", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("principal_type", "STRING", mode="REQUIRED"),  # USER, SERVICE_ACCOUNT, GROUP
    bigquery.SchemaField("role", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("access_type", "STRING", mode="NULLABLE"),  # OWNER, READER, WRITER, etc
    bigquery.SchemaField("special_group", "STRING", mode="NULLABLE"),  # projectOwners, projectReaders, etc
    bigquery.SchemaField("table_count", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("last_modified", "TIMESTAMP", mode="NULLABLE"),
]

SCHEMA_HISTORY = SCHEMA_SNAPSHOT + [
    bigquery.SchemaField("change_type", "STRING", mode="REQUIRED"),  # ADDED, REMOVED, MODIFIED
    bigquery.SchemaField("previous_value", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("new_value", "JSON", mode="NULLABLE"),
]


# ========== FUNCIONES AUXILIARES ==========

def get_bigquery_client(project_id: str) -> bigquery.Client:
    """Obtiene cliente de BigQuery para un proyecto específico."""
    return bigquery.Client(project=project_id)


def ensure_audit_tables(client: bigquery.Client) -> bool:
    """
    Crea las tablas de auditoría si no existen.
    
    Returns:
        True si se crearon o ya existen, False si hay error
    """
    try:
        dataset = client.get_dataset(f"{AUDIT_PROJECT}.{AUDIT_DATASET}")
        logger.info(f"Dataset {AUDIT_DATASET} ya existe")
    except NotFound:
        logger.error(f"Dataset {AUDIT_DATASET} no encontrado en {AUDIT_PROJECT}")
        logger.info("Crea el dataset manualmente antes de ejecutar este script")
        return False
    
    # Crear tabla de snapshots
    table_id = f"{AUDIT_PROJECT}.{AUDIT_DATASET}.{AUDIT_TABLE_IAM_SNAPSHOT}"
    try:
        client.get_table(table_id)
        logger.info(f"Tabla {AUDIT_TABLE_IAM_SNAPSHOT} ya existe")
    except NotFound:
        table = bigquery.Table(table_id, schema=SCHEMA_SNAPSHOT)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date"
        )
        client.create_table(table)
        logger.info(f"Tabla {AUDIT_TABLE_IAM_SNAPSHOT} creada")
    
    # Crear tabla de histórico
    table_id_history = f"{AUDIT_PROJECT}.{AUDIT_DATASET}.{AUDIT_TABLE_IAM_HISTORY}"
    try:
        client.get_table(table_id_history)
        logger.info(f"Tabla {AUDIT_TABLE_IAM_HISTORY} ya existe")
    except NotFound:
        table = bigquery.Table(table_id_history, schema=SCHEMA_HISTORY)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date"
        )
        client.create_table(table)
        logger.info(f"Tabla {AUDIT_TABLE_IAM_HISTORY} creada")
    
    return True


def get_dataset_access_entries(
    project_id: str,
    dataset_id: str
) -> List[Dict]:
    """
    Obtiene las entradas de acceso de un dataset.
    
    Args:
        project_id: ID del proyecto
        dataset_id: ID del dataset
        
    Returns:
        Lista de diccionarios con información de acceso
    """
    try:
        client = get_bigquery_client(project_id)
        dataset = client.get_dataset(f"{project_id}.{dataset_id}")
        
        access_entries = []
        for entry in dataset.access_entries or []:
            access_info = {
                "principal_email": entry.user_by_email,
                "principal_type": "USER" if entry.user_by_email else
                                "GROUP" if entry.group_by_email else
                                "SPECIAL",
                "role": entry.role,
                "access_type": None,
                "special_group": entry.special_group,
            }
            access_entries.append(access_info)
        
        return access_entries
    except PermissionDenied:
        logger.warning(f"Permisos insuficientes para leer {project_id}.{dataset_id}")
        return []
    except Exception as e:
        logger.error(f"Error leyendo acceso de {project_id}.{dataset_id}: {str(e)}")
        return []


def get_dataset_table_count(project_id: str, dataset_id: str) -> int:
    """Obtiene el número de tablas en un dataset."""
    try:
        client = get_bigquery_client(project_id)
        table_count = 0
        for _ in client.list_tables(dataset_id):
            table_count += 1
        return table_count
    except Exception as e:
        logger.warning(f"Error contando tablas en {project_id}.{dataset_id}: {str(e)}")
        return 0


def capture_iam_snapshot(
    environment: str,
    project_id: str,
    snapshot_timestamp: datetime
) -> List[Dict]:
    """
    Captura un snapshot de IAM para todos los datasets del proyecto.
    
    Args:
        environment: Ambiente (dev, qua, pro)
        project_id: ID del proyecto
        snapshot_timestamp: Timestamp del snapshot
        
    Returns:
        Lista de registros para insertar en BigQuery
    """
    records = []
    
    try:
        client = get_bigquery_client(project_id)
        
        # Listar todos los datasets
        datasets = []
        for dataset in client.list_datasets():
            datasets.append(dataset.dataset_id)
        
        logger.info(f"Procesando {len(datasets)} datasets en {project_id}")
        
        for dataset_id in datasets:
            # Obtener entradas de acceso
            access_entries = get_dataset_access_entries(project_id, dataset_id)
            
            # Contar tablas
            table_count = get_dataset_table_count(project_id, dataset_id)
            
            # Crear registros
            for entry in access_entries:
                if entry["principal_email"] or entry["special_group"]:
                    record = {
                        "snapshot_date": snapshot_timestamp.date(),
                        "snapshot_timestamp": snapshot_timestamp,
                        "environment": environment,
                        "source_project_id": project_id,
                        "dataset_id": dataset_id,
                        "principal_email": entry["principal_email"] or entry["special_group"],
                        "principal_type": entry["principal_type"],
                        "role": entry["role"],
                        "access_type": entry["access_type"],
                        "special_group": entry["special_group"],
                        "table_count": table_count,
                        "last_modified": snapshot_timestamp,
                    }
                    records.append(record)
        
        logger.info(f"Capturados {len(records)} registros de IAM para {project_id}")
        return records
    
    except Exception as e:
        logger.error(f"Error capturando snapshot de {project_id}: {str(e)}")
        return []


def insert_snapshot_records(
    client: bigquery.Client,
    records: List[Dict]
) -> bool:
    """
    Inserta registros en la tabla de snapshots.
    
    Args:
        client: Cliente de BigQuery
        records: Lista de registros a insertar
        
    Returns:
        True si se insertaron correctamente
    """
    if not records:
        logger.warning("No hay registros para insertar")
        return True
    
    try:
        table_id = f"{AUDIT_PROJECT}.{AUDIT_DATASET}.{AUDIT_TABLE_IAM_SNAPSHOT}"
        errors = client.insert_rows_json(table_id, records)
        
        if errors:
            logger.error(f"Errores insertando registros: {errors}")
            return False
        
        logger.info(f"Insertados {len(records)} registros en {table_id}")
        return True
    except Exception as e:
        logger.error(f"Error insertando registros: {str(e)}")
        return False


def compare_snapshots_and_record_changes(
    client: bigquery.Client,
    environment: str
) -> bool:
    """
    Compara snapshots consecutivos y registra cambios.
    
    Args:
        client: Cliente de BigQuery
        environment: Ambiente a procesar
        
    Returns:
        True si se procesaron correctamente
    """
    try:
        # Consulta para detectar cambios
        query = f"""
        WITH latest_two AS (
            SELECT
                snapshot_date,
                source_project_id,
                dataset_id,
                principal_email,
                principal_type,
                role,
                custom_table_num,
                ROW_NUMBER() OVER (
                    PARTITION BY source_project_id, dataset_id, principal_email 
                    ORDER BY snapshot_timestamp DESC
                ) as rn
            FROM `{AUDIT_PROJECT}.{AUDIT_DATASET}.{AUDIT_TABLE_IAM_SNAPSHOT}`
            WHERE environment = '{environment}'
            AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        ),
        previous AS (
            SELECT * FROM latest_two WHERE rn = 2
        ),
        current AS (
            SELECT * FROM latest_two WHERE rn = 1
        )
        SELECT
            CURRENT_TIMESTAMP() as snapshot_timestamp,
            c.environment,
            COALESCE(c.source_project_id, p.source_project_id) as source_project_id,
            COALESCE(c.dataset_id, p.dataset_id) as dataset_id,
            COALESCE(c.principal_email, p.principal_email) as principal_email,
            CASE
                WHEN p.principal_email IS NULL THEN 'ADDED'
                WHEN c.principal_email IS NULL THEN 'REMOVED'
                ELSE 'MODIFIED'
            END as change_type,
            TO_JSON_STRING(p) as previous_value,
            TO_JSON_STRING(c) as new_value
        FROM current c
        FULL OUTER JOIN previous p
            ON c.source_project_id = p.source_project_id
            AND c.dataset_id = p.dataset_id
            AND c.principal_email = p.principal_email
        WHERE c.principal_email IS NULL OR p.principal_email IS NULL 
            OR c.role != p.role
        """
        
        # Ejecutar consulta y obtener cambios
        query_job = client.query(query)
        changes = list(query_job)
        
        if changes:
            logger.info(f"Encontrados {len(changes)} cambios en IAM")
            # Aquí podrías insertar los cambios en la tabla de history
        else:
            logger.info("Sin cambios detectados")
        
        return True
    except Exception as e:
        logger.error(f"Error comparando snapshots: {str(e)}")
        return False


# ========== CLI ==========

def main():
    parser = argparse.ArgumentParser(
        description="IAM Access Snapshot Sync - Captura accesos en BigQuery"
    )
    
    parser.add_argument(
        "--environment",
        choices=["dev", "qua", "pro", "all"],
        default="all",
        help="Ambiente a procesar (default: all)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta sin insertar datos en BigQuery"
    )
    
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compara con snapshot anterior y detecta cambios"
    )
    
    args = parser.parse_args()
    
    # Obtener environments a procesar
    environments = [args.environment] if args.environment != "all" else list(ENVIRONMENT_CONFIG.keys())
    
    logger.info(f"Iniciando sincronización IAM")
    logger.info(f"Ambientes a procesar: {', '.join(environments)}")
    logger.info(f"Dry-run: {args.dry_run}")
    
    # Cliente de auditoría
    audit_client = get_bigquery_client(AUDIT_PROJECT)
    
    # Crear tablas si no existen
    if not ensure_audit_tables(audit_client):
        logger.error("No se pudieron crear las tablas de auditoría")
        return 1
    
    # Procesar cada ambiente
    all_records = []
    snapshot_timestamp = datetime.now()
    
    for env in environments:
        try:
            config = ENVIRONMENT_CONFIG[env]
            project_id = config["project_id"]
            
            logger.info(f"\n=== Procesando {env} ({project_id}) ===")
            
            # Capturar snapshot
            records = capture_iam_snapshot(env, project_id, snapshot_timestamp)
            
            if records:
                all_records.extend(records)
                
                if not args.dry_run:
                    # Insertar en BigQuery
                    if insert_snapshot_records(audit_client, records):
                        logger.info(f"✓ Snapshot de {env} inserado correctamente")
                    else:
                        logger.error(f"✗ Error insertando snapshot de {env}")
                else:
                    logger.info(f"[DRY-RUN] Se insertarían {len(records)} registros")
        
        except Exception as e:
            logger.error(f"Error procesando {env}: {str(e)}")
            continue
    
    # Comparar si se especifica
    if args.compare and not args.dry_run:
        for env in environments:
            logger.info(f"\nComparando snapshots de {env}")
            compare_snapshots_and_record_changes(audit_client, env)
    
    logger.info(f"\n=== Sincronización completada ===")
    logger.info(f"Total de registros capturados: {len(all_records)}")
    
    return 0


if __name__ == "__main__":
    exit(main())
