-- ============================================================
-- QUERY PRÁCTICO: Actualizar last_etl_synced y row_count
-- ============================================================
-- Este query actualiza companies_consolidated para un ambiente específico
-- usando las 11 tablas de Bronze conocidas.
--
-- NOTA: Este query debe ejecutarse 4 veces al día (7am, 1pm, 7pm, 1am)
--       una vez por cada ambiente (DEV, QUA, PRO)
--
-- IMPORTANTE: Reemplaza {ENVIRONMENT_PROJECT_ID} con:
--   - 'platform-partners-des' para DEV
--   - 'platform-partners-qua' para QUA  
--   - 'constant-height-455614-i0' para PRO
--
-- IMPORTANTE: Reemplaza {TABLE_NAME} con cada una de las 11 tablas:
--   business_unit, appointment, customer, invoice, job, payment, 
--   technician, tag, location, estimate, invoice_line_item
--   (o las que estén en tu metadata)

-- ============================================================
-- EJEMPLO: Para una tabla específica (business_unit) en DEV
-- ============================================================

MERGE `pph-central.settings.companies_consolidated` cc
USING (
  SELECT 
    c.company_id,
    'business_unit' as table_name,
    MAX(b._etl_synced) as max_sync,
    COUNT(*) as row_count
  FROM `platform-partners-des.settings.companies` c
  CROSS JOIN `platform-partners-des.bronze.business_unit` b
  WHERE c.company_fivetran_status = TRUE
    AND c.company_project_id = 'platform-partners-des'
    AND b._etl_synced IS NOT NULL
  GROUP BY c.company_id
) sync_data
ON cc.company_id = sync_data.company_id 
  AND cc.table_name = sync_data.table_name
WHEN MATCHED THEN
  UPDATE SET
    last_etl_synced = sync_data.max_sync,
    row_count = sync_data.row_count,
    updated_at = CURRENT_TIMESTAMP();

-- ============================================================
-- QUERY COMPLETO: Para todas las 11 tablas usando UNION ALL
-- ============================================================
-- Este query procesa todas las tablas de una vez
-- Reemplaza las referencias a tablas con tus 11 tablas reales

MERGE `pph-central.settings.companies_consolidated` cc
USING (
  -- Tabla 1: business_unit
  SELECT 
    c.company_id,
    'business_unit' as table_name,
    MAX(b._etl_synced) as max_sync,
    COUNT(*) as row_count
  FROM `platform-partners-des.settings.companies` c
  CROSS JOIN `platform-partners-des.bronze.business_unit` b
  WHERE c.company_fivetran_status = TRUE
    AND c.company_project_id = 'platform-partners-des'
    AND b._etl_synced IS NOT NULL
  GROUP BY c.company_id
  
  UNION ALL
  
  -- Tabla 2: appointment (reemplaza con tu segunda tabla)
  SELECT 
    c.company_id,
    'appointment' as table_name,
    MAX(b._etl_synced) as max_sync,
    COUNT(*) as row_count
  FROM `platform-partners-des.settings.companies` c
  CROSS JOIN `platform-partners-des.bronze.appointment` b
  WHERE c.company_fivetran_status = TRUE
    AND c.company_project_id = 'platform-partners-des'
    AND b._etl_synced IS NOT NULL
  GROUP BY c.company_id
  
  UNION ALL
  
  -- Repetir para las otras 9 tablas...
  -- (Agregar un UNION ALL por cada tabla restante)
  
) sync_data
ON cc.company_id = sync_data.company_id 
  AND cc.table_name = sync_data.table_name
WHEN MATCHED THEN
  UPDATE SET
    last_etl_synced = sync_data.max_sync,
    row_count = sync_data.row_count,
    updated_at = CURRENT_TIMESTAMP();

-- ============================================================
-- NOTA FINAL:
-- ============================================================
-- Para automatizar esto completamente, la mejor opción es:
-- 1. Usar el script Python (update_companies_consolidated_sync.py)
-- 2. O crear 3 Scheduled Queries (uno por ambiente) que ejecuten
--    el MERGE completo con las 11 tablas
-- 3. Programar cada Scheduled Query para ejecutarse 4 veces al día:
--    - 7:00 AM
--    - 1:00 PM  
--    - 7:00 PM
--    - 1:00 AM
