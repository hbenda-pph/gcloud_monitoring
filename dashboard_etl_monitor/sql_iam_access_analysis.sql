-- ============================================================================
-- SQL: IAM Access Analysis for BigQuery
-- Descripción: Consultas para analizar accesos y credenciales en BigQuery
-- ============================================================================

-- ============================================================================
-- QUERY 1: Obtener todos los usuarios y sus accesos por dataset
-- ============================================================================
-- Ejecutar en: pph-central (o tu proyecto de monitoreo)
-- Nota: Requiere acceso a INFORMATION_SCHEMA en todos los proyectos

SELECT
    project_id,
    dataset_id,
    EXTRACT(DATE FROM CURRENT_TIMESTAMP()) as analysis_date,
    COUNT(*) as accessed_tables,
    MIN(TABLE_SCHEMA) as first_dataset_access,
    MAX(TIMESTAMP_MILLIS(creation_time)) as latest_table_creation
FROM
    `project_id.dataset_id.INFORMATION_SCHEMA.TABLES`
WHERE
    DATE(TIMESTAMP_MILLIS(creation_time)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY
    project_id,
    dataset_id
ORDER BY
    project_id,
    dataset_id;

-- ============================================================================
-- QUERY 2: Listado de Service Accounts activos
-- ============================================================================

SELECT
    'service-account' as principal_type,
    email as principal_email,
    CURRENT_TIMESTAMP() as last_checked,
    CASE 
        WHEN email LIKE '%@appspot.gserviceaccount.com' THEN 'App Engine'
        WHEN email LIKE '%functions@appspot.gserviceaccount.com' THEN 'Cloud Functions'
        WHEN email LIKE '%@iam.gserviceaccount.com' THEN 'Service Account'
        WHEN email LIKE 'cloud-sql-%@cloudservices.gserviceaccount.com' THEN 'Cloud SQL'
        WHEN email LIKE '%@cloudbuild.gserviceaccount.com' THEN 'Cloud Build'
        ELSE 'Other Service Account'
    END as sa_type
FROM (
    -- Esta es una tabla virtual que necesita ser poblada con datos reales
    -- Por ejemplo, obtenida de Cloud Asset API o IAM Policy Logs
    SELECT 'sa-1@project.iam.gserviceaccount.com' as email
    UNION ALL
    SELECT 'sa-2@project.iam.gserviceaccount.com' as email
)
ORDER BY
    sa_type,
    principal_email;

-- ============================================================================
-- QUERY 3: Matriz de acceso - similatado (requiere integración con logs)
-- ============================================================================

WITH user_dataset_access AS (
    SELECT
        referenced_dataset.project_id,
        referenced_dataset.dataset_id,
        COALESCE(
            protoPayload.authenticationInfo.principalEmail,
            protoPayload.request.destinationDataset.projectId
        ) as user_email,
        COUNT(*) as access_count,
        MIN(timestamp) as first_access,
        MAX(timestamp) as last_access
    FROM
        `project_id.dataset_id.cloudaudit_googleapis_com_activity`
    WHERE
        serviceName = 'bigquery.googleapis.com'
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND protoPayload.status.code IS NULL  -- Accesos exitosos
    GROUP BY
        1, 2, 3
)
SELECT
    project_id,
    dataset_id,
    user_email,
    access_count,
    first_access,
    last_access,
    EXTRACT(HOUR FROM last_access) as last_access_hour
FROM
    user_dataset_access
ORDER BY
    project_id,
    dataset_id,
    user_email;

-- ============================================================================
-- QUERY 4: Identificar usuarios inactivos (sin acceso en últimos X días)
-- ============================================================================

WITH recent_access AS (
    SELECT
        protoPayload.authenticationInfo.principalEmail as user_email,
        DATE(MAX(timestamp)) as last_activity_date
    FROM
        `project_id.dataset_id.cloudaudit_googleapis_com_activity`
    WHERE
        serviceName = 'bigquery.googleapis.com'
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY
        1
)
SELECT
    user_email,
    last_activity_date,
    DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) as days_inactive,
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) >= 30 THEN 'Inactivo'
        WHEN DATE_DIFF(CURRENT_DATE(), last_activity_date, DAY) >= 7 THEN 'Poco Activo'
        ELSE 'Activo'
    END as status
FROM
    recent_access
WHERE
    last_activity_date IS NOT NULL
ORDER BY
    last_activity_date ASC;

-- ============================================================================
-- QUERY 5: Análisis de cambios en accesos (últimos 7 días)
-- ============================================================================

WITH daily_changes AS (
    SELECT
        DATE(timestamp) as activity_date,
        COALESCE(
            protoPayload.authenticationInfo.principalEmail,
            'unknown-user'
        ) as principal_email,
        protoPayload.methodName as operation,
        resource.labels.dataset_id as dataset_id,
        COUNT(*) as operation_count,
        CASE 
            WHEN protoPayload.status.code IS NULL THEN 'SUCCESS'
            ELSE 'FAILURE'
        END as status
    FROM
        `project_id.dataset_id.cloudaudit_googleapis_com_activity`
    WHERE
        serviceName = 'bigquery.googleapis.com'
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        AND protoPayload.methodName IN (
            'jobservice.insert',
            'jobservice.get',
            'datasets.get',
            'tables.get',
            'datasets.patch'  -- Cambios en permisos
        )
    GROUP BY
        1, 2, 3, 4, 5
)
SELECT
    activity_date,
    principal_email,
    operation,
    dataset_id,
    operation_count,
    status,
    RANK() OVER (PARTITION BY activity_date ORDER BY operation_count DESC) as daily_rank
FROM
    daily_changes
WHERE
    status = 'SUCCESS'
ORDER BY
    activity_date DESC,
    operation_count DESC;

-- ============================================================================
-- QUERY 6: Resumen de permisos por dataset (últimos 30 días)
-- ============================================================================

WITH permission_summary AS (
    SELECT
        DATE_TRUNC(DATE(timestamp), MONTH) as month,
        protoPayload.authenticationInfo.principalEmail as principal_email,
        resource.labels.dataset_id as dataset_id,
        protoPayload.request.dataset.datasetReference.datasetId as dataset_ref,
        COUNT(DISTINCT DATE(timestamp)) as active_days,
        COUNT(*) as total_queries
    FROM
        `project_id.dataset_id.cloudaudit_googleapis_com_activity`
    WHERE
        serviceName = 'bigquery.googleapis.com'
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND protoPayload.status.code IS NULL
    GROUP BY
        1, 2, 3, 4
)
SELECT
    month,
    principal_email,
    COALESCE(dataset_id, dataset_ref) as dataset_id,
    active_days,
    total_queries,
    ROUND(total_queries / NULLIF(active_days, 0), 2) as avg_queries_per_day
FROM
    permission_summary
WHERE
    principal_email IS NOT NULL
ORDER BY
    month DESC,
    total_queries DESC;

-- ============================================================================
-- QUERY 7: Detectar patrones anormales de acceso
-- ============================================================================

WITH hourly_access AS (
    SELECT
        protoPayload.authenticationInfo.principalEmail as principal_email,
        resource.labels.dataset_id as dataset_id,
        EXTRACT(HOUR FROM timestamp) as access_hour,
        COUNT(*) as access_count,
        CURRENT_TIMESTAMP() as analysis_timestamp
    FROM
        `project_id.dataset_id.cloudaudit_googleapis_com_activity`
    WHERE
        serviceName = 'bigquery.googleapis.com'
        AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY
        1, 2, 3
),
statistics AS (
    SELECT
        principal_email,
        dataset_id,
        AVG(access_count) as avg_access_count,
        STDDEV_POP(access_count) as stddev_access_count,
        MAX(access_count) as max_access_count
    FROM
        hourly_access
    GROUP BY
        1, 2
)
SELECT
    h.principal_email,
    h.dataset_id,
    h.access_hour,
    h.access_count,
    s.avg_access_count,
    s.stddev_access_count,
    CASE 
        WHEN h.access_count > (s.avg_access_count + 2 * s.stddev_access_count) THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as pattern_status
FROM
    hourly_access h
JOIN
    statistics s
    ON h.principal_email = s.principal_email
    AND h.dataset_id = s.dataset_id
WHERE
    h.access_count > (s.avg_access_count + 2 * s.stddev_access_count)
ORDER BY
    h.principal_email,
    h.access_hour;

-- ============================================================================
-- QUERY 8: Auditoría de cambios en políticas IAM
-- ============================================================================

SELECT
    DATE(timestamp) as change_date,
    protoPayload.authenticationInfo.principalEmail as changed_by,
    resource.labels.dataset_id as dataset_id,
    protoPayload.methodName as operation,
    CASE 
        WHEN protoPayload.methodName LIKE '%patch%' THEN 'MODIFIED'
        WHEN protoPayload.methodName LIKE '%.insert%' THEN 'CREATED'
        WHEN protoPayload.methodName LIKE '%delete%' THEN 'DELETED'
        ELSE 'OTHER'
    END as operation_type,
    protoPayload.request as request_detail,
    COUNT(*) as change_count
FROM
    `project_id.dataset_id.cloudaudit_googleapis_com_activity`
WHERE
    serviceName = 'bigquery.googleapis.com'
    AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND protoPayload.methodName IN (
        'datasets.patch',
        'datasets.update',
        'tables.patch',
        'tables.update'
    )
GROUP BY
    1, 2, 3, 4, 5, 6
ORDER BY
    change_date DESC;

-- ============================================================================
-- Notas importantes:
-- ============================================================================
-- 1. Las queries que usan cloudaudit_googleapis_com_activity requieren que
--    Cloud Audit Logs esté habilitado en el proyecto
--
-- 2. Actualiza "project_id" y "dataset_id" con tus valores reales
--
-- 3. Algunas información está limitada a proyectos donde tu usuario tiene
--    permisos suficientes
--
-- 4. Para obtener información de IAM más detallada, considera usar:
--    - Cloud Asset API (asset.googleapis.com)
--    - Cloud Logging (logging.googleapis.com)
--    - IAM Policy API
-- ============================================================================
