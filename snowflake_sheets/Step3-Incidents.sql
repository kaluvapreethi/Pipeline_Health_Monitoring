USE ROLE tb_data_engineer;
USE DATABASE snowflake_intelligence;
USE SCHEMA franchise_intelligent_agent_pipeline_monitor;
USE WAREHOUSE tb_de_wh;

CREATE OR REPLACE TABLE dbt_pipeline_incidents (
    incident_id           NUMBER AUTOINCREMENT PRIMARY KEY,
    query_id              VARCHAR,
    dbt_model_name        VARCHAR,
    dbt_resource_type     VARCHAR,
    dbt_materialization   VARCHAR,
    dbt_invocation_id     VARCHAR,
    warehouse_name        VARCHAR,
    warehouse_size        VARCHAR,
    start_time            TIMESTAMP_NTZ,
    total_elapsed_seconds FLOAT,
    gb_scanned            FLOAT,
    execution_status      VARCHAR,
    error_code            VARCHAR,
    error_message         VARCHAR,
    slow_run_flag         VARCHAR,
    has_spill             BOOLEAN,
    had_queue_wait        BOOLEAN,
    -- AI enrichment columns
    failure_category      VARCHAR,  -- from AI_CLASSIFY
    fix_suggestion        VARCHAR,  -- from AI_COMPLETE
    incident_summary      VARCHAR,  -- combined summary for Cortex Search
    enriched_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--=================================================================================
INSERT INTO dbt_pipeline_incidents (
    query_id, dbt_model_name, dbt_resource_type, dbt_materialization,
    dbt_invocation_id, warehouse_name, warehouse_size, start_time,
    total_elapsed_seconds, gb_scanned, execution_status, error_code,
    error_message, slow_run_flag, has_spill, had_queue_wait,
    failure_category, fix_suggestion, incident_summary
)
SELECT
    query_id,
    dbt_model_name,
    dbt_resource_type,
    dbt_materialization,
    dbt_invocation_id,
    warehouse_name,
    warehouse_size,
    start_time,
    total_elapsed_seconds,
    gb_scanned,
    execution_status,
    error_code,
    error_message,
    slow_run_flag,
    has_spill,
    had_queue_wait,

    -- AI_CLASSIFY: bucket the incident into a failure category
    -- Returns JSON like {"labels": ["timeout"]}
    AI_CLASSIFY(
        incident_description,
        [
            'timeout',
            'schema_drift',
            'logic_error',
            'warehouse_too_small',
            'warehouse_overloaded',
            'missing_object',
            'permission_denied',
            'cost_spike',
            'data_quality_failure',
            'network_error'
        ],
        {
            'task_description': 'Classify this dbt pipeline failure or performance issue into the most appropriate category based on the description. Choose the single category that best explains the root cause.'
        }
    ):labels[0]::STRING AS failure_category,

    -- AI_COMPLETE: write a plain-English fix suggestion
    TRIM(AI_COMPLETE(
        'mistral-large2',
        'You are a senior data engineer reviewing a dbt pipeline incident. ' ||
        'Read the following incident description and write a clear, actionable fix suggestion in 2-3 sentences. ' ||
        'Be specific about what to check and what to change. ' ||
        'Do not repeat the incident back. Write only the fix suggestion. ' ||
        'Incident: ' || incident_description
    )) AS fix_suggestion,

    -- Build a combined summary for Cortex Search indexing
    -- (combines the description + category + fix into one searchable document)
    'Incident for dbt model ' || COALESCE(dbt_model_name, 'unknown') ||
    '. Category will be classified as AI_CLASSIFY result. ' ||
    incident_description AS incident_summary

FROM dbt_incident_input_v
WHERE query_id NOT IN (SELECT query_id FROM dbt_pipeline_incidents);

--=================================================================================

UPDATE dbt_pipeline_incidents
SET incident_summary =
    'Model: ' || COALESCE(dbt_model_name, 'unknown') || '. ' ||
    'Category: ' || COALESCE(failure_category, 'uncategorised') || '. ' ||
    'Warehouse: ' || COALESCE(warehouse_name, 'unknown') ||
    ' (' || COALESCE(warehouse_size, 'unknown') || '). ' ||
    'Duration: ' || ROUND(total_elapsed_seconds, 1) || ' seconds. ' ||
    'Data scanned: ' || COALESCE(gb_scanned::STRING, '0') || ' GB. ' ||
    'Fix: ' || COALESCE(fix_suggestion, 'no suggestion available')
WHERE incident_summary IS NOT NULL;

--=================================================================================

SELECT
    dbt_model_name,
    execution_status,
    failure_category,
    LEFT(fix_suggestion, 200) AS fix_preview,
    -- start_time,
    *
FROM dbt_pipeline_incidents
ORDER BY start_time DESC
LIMIT 10;

SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.DBT_PIPELINE_INCIDENTS

Model: fct_dbt_run_metrics. Category: permission_denied. Warehouse: TB_DE_WH (unknown). Duration: 0.1 seconds. Data scanned: 0 GB. Fix: Check the schema name 'TB_101.PIPELINE_MONITOR' in the dbt model `fct_dbt_run_metrics` to ensure it exists and the user has the necessary permissions. If the schema does not exist, create it; if it exists, grant the required permissions to the user running the dbt pipeline.