CREATE OR REPLACE VIEW dbt_incident_input_v AS
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

    -- Assembled natural language description for AI_CLASSIFY and AI_COMPLETE
    -- We build a single coherent paragraph from the structured fields
    CASE
        WHEN execution_status = 'FAIL' THEN
            'dbt model ' || COALESCE(dbt_model_name, 'unknown') ||
            ' (' || COALESCE(dbt_materialization, 'unknown') || ' materialization) ' ||
            'failed on warehouse ' || COALESCE(warehouse_name, 'unknown') ||
            ' (size: ' || COALESCE(warehouse_size, 'unknown') || '). ' ||
            'It ran for ' || ROUND(total_elapsed_seconds, 1) || ' seconds before failing. ' ||
            'Error code: ' || COALESCE(error_code, 'none') || '. ' ||
            'Error message: ' || COALESCE(LEFT(error_message, 400), 'no error message recorded') || '. ' ||
            CASE WHEN has_spill THEN 'The query spilled data to storage, indicating the warehouse was undersized. ' ELSE '' END ||
            CASE WHEN had_queue_wait THEN 'The query waited in queue before executing, indicating warehouse contention. ' ELSE '' END ||
            'Scanned ' || COALESCE(gb_scanned::STRING, '0') || ' GB of data.'

        WHEN slow_run_flag IS NOT NULL THEN
            'dbt model ' || COALESCE(dbt_model_name, 'unknown') ||
            ' (' || COALESCE(dbt_materialization, 'unknown') || ' materialization) ' ||
            'completed successfully but was flagged as ' || slow_run_flag || '. ' ||
            'It ran for ' || ROUND(total_elapsed_seconds, 1) || ' seconds on warehouse ' ||
            COALESCE(warehouse_name, 'unknown') || ' (size: ' || COALESCE(warehouse_size, 'unknown') || '). ' ||
            CASE WHEN has_spill THEN 'The query spilled data to disk. ' ELSE '' END ||
            CASE WHEN had_queue_wait THEN 'The query waited in queue. ' ELSE '' END ||
            'Scanned ' || COALESCE(gb_scanned::STRING, '0') || ' GB of data.'

        ELSE
            'dbt model ' || COALESCE(dbt_model_name, 'unknown') || ' had an unusual execution pattern.'
    END AS incident_description

FROM SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.STG_DBT_QUERY_HISTORY
WHERE 
execution_status = 'FAIL'
   OR 
   slow_run_flag IS NOT NULL
   OR has_spill = TRUE;

select * from dbt_incident_input_v;

select execution_status,slow_run_flag,has_spill,* from SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.STG_DBT_QUERY_HISTORY;

select query_tag,* from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY order by start_time desc;