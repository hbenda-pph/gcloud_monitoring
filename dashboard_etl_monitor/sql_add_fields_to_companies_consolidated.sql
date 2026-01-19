-- ============================================================
-- QUERY 1: Agregar campos a la tabla companies_consolidated
-- ============================================================
-- Ejecutar este query primero para agregar los nuevos campos
-- Tabla: pph-central.settings.companies_consolidated

ALTER TABLE `pph-central.settings.companies_consolidated`
ADD COLUMN IF NOT EXISTS last_etl_synced TIMESTAMP,
ADD COLUMN IF NOT EXISTS row_count INTEGER;

-- ============================================================
-- QUERY 2: Actualizar last_etl_synced y row_count usando MERGE
-- ============================================================
-- Este query calcula MAX(_etl_synced) y COUNT(*) desde cada tabla bronze
-- y actualiza la tabla companies_consolidated usando MERGE
-- 
-- NOTA: Este query debe ejecutarse periódicamente (Scheduled Query)
--       Horarios sugeridos: 7am, 1pm, 7pm, 1am (1 hora después del ETL)
--
-- IMPORTANTE: Este query necesita ser ejecutado por ambiente o 
--             usar un script Python que itere sobre cada combinación
--             company_id + table_name, ya que cada compañía tiene
--             su propio company_project_id

-- ============================================================
-- QUERY 2A: MERGE para un ambiente específico (ejemplo para DEV)
-- ============================================================
-- Este es un ejemplo para DEV. Necesitarás crear queries similares
-- para QUA y PRO, o mejor aún, usar un script Python que itere

MERGE `pph-central.settings.companies_consolidated` cc
USING (
  -- Obtener todas las combinaciones company_id + table_name
  -- y calcular MAX(_etl_synced) y COUNT(*) desde cada tabla bronze
  SELECT 
    cc.company_id,
    cc.table_name,
    -- Usar CASE para construir dinámicamente la referencia a la tabla
    -- NOTA: Esto requiere que conozcas las 11 tablas específicas
    -- o uses un script Python que genere este query dinámicamente
    
    -- Ejemplo para tabla 'business_unit':
    CASE 
      WHEN cc.table_name = 'business_unit' THEN (
        SELECT MAX(_etl_synced) 
        FROM `platform-partners-des.bronze.business_unit`
        WHERE _etl_synced IS NOT NULL
      )
      -- Agregar más CASE para cada una de las 11 tablas
      ELSE NULL
    END as max_sync,
    
    CASE 
      WHEN cc.table_name = 'business_unit' THEN (
        SELECT COUNT(*) 
        FROM `platform-partners-des.bronze.business_unit`
      )
      -- Agregar más CASE para cada una de las 11 tablas
      ELSE NULL
    END as row_count
    
  FROM `pph-central.settings.companies_consolidated` cc
  INNER JOIN `platform-partners-des.settings.companies` c
    ON cc.company_id = c.company_id
  WHERE c.company_fivetran_status = TRUE
    AND c.company_project_id = 'platform-partners-des'
) sync_data
ON cc.company_id = sync_data.company_id 
  AND cc.table_name = sync_data.table_name
WHEN MATCHED THEN
  UPDATE SET
    last_etl_synced = sync_data.max_sync,
    row_count = sync_data.row_count,
    updated_at = CURRENT_TIMESTAMP();

-- ============================================================
-- QUERY 2B: Enfoque alternativo usando UNION ALL por tabla
-- ============================================================
-- Este enfoque es más explícito pero requiere conocer las 11 tablas
-- Reemplaza 'business_unit' con cada una de las 11 tablas de Bronze

MERGE `pph-central.settings.companies_consolidated` cc
USING (
  -- Para cada tabla, obtener MAX(_etl_synced) y COUNT(*)
  -- Ejemplo para 'business_unit':
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
  
  -- Repetir para cada una de las otras 10 tablas:
  -- 'appointment', 'customer', 'invoice', etc.
  -- (Agregar un UNION ALL por cada tabla)
  
) sync_data
ON cc.company_id = sync_data.company_id 
  AND cc.table_name = sync_data.table_name
WHEN MATCHED THEN
  UPDATE SET
    last_etl_synced = sync_data.max_sync,
    row_count = sync_data.row_count,
    updated_at = CURRENT_TIMESTAMP();

-- ============================================================
-- NOTA IMPORTANTE:
-- ============================================================
-- Debido a que cada compañía tiene su propio company_project_id,
-- y las tablas están en {company_project_id}.bronze.{table_name},
-- la mejor solución es crear un SCRIPT PYTHON que:
-- 
-- 1. Obtenga todas las combinaciones company_id + table_name
--    desde companies_consolidated
-- 2. Para cada combinación:
--    a. Obtenga el company_project_id desde settings.companies
--    b. Ejecute: SELECT MAX(_etl_synced), COUNT(*) 
--       FROM {company_project_id}.bronze.{table_name}
--    c. Actualice companies_consolidated con esos valores
--
-- Este script puede ejecutarse como Cloud Function o Scheduled Query
-- con Python UDF, o como parte del proceso ETL.
