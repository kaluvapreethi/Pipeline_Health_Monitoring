USE ROLE accountadmin;
USE DATABASE snowflake_intelligence;
USE SCHEMA FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR;

CREATE OR REPLACE TASK refresh_pipeline_monitor
    WAREHOUSE = tb_de_wh
    SCHEDULE  = 'USING CRON 0 * * * * UTC'  -- every hour
    COMMENT   = 'Refreshes dbt pipeline health monitor: dbt models + AI enrichment'
AS
EXECUTE IMMEDIATE
$$
BEGIN
    -- Step 1: refresh the staging model (incremental)
    -- In a real dbt CLI setup, trigger via dbt Cloud API or a stored proc wrapper
    -- For trial accounts without dbt Cloud, run the SQL directly:
    INSERT INTO snowflake_intelligence.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.stg_dbt_query_history
    SELECT
        query_id,
        query_text,
        query_type,
        query_tag,
        database_name,
        schema_name,
        user_name,
        role_name,
        warehouse_name,
        warehouse_size,
        execution_status,
        error_code,
        error_message,
        start_time,
        end_time,
        total_elapsed_time / 1000.0                         AS total_elapsed_seconds,
        compilation_time / 1000.0                           AS compilation_seconds,
        execution_time / 1000.0                             AS execution_seconds,
        queued_overload_time / 1000.0                       AS queued_overload_seconds,
        bytes_scanned,
        bytes_written,
        partitions_scanned,
        partitions_total,
        rows_produced,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,
        TRY_PARSE_JSON(query_tag):node_name::STRING         AS dbt_model_name,
        TRY_PARSE_JSON(query_tag):node_id::STRING           AS dbt_node_id,
        TRY_PARSE_JSON(query_tag):invocation_id::STRING     AS dbt_invocation_id,
        TRY_PARSE_JSON(query_tag):node_resource_type::STRING AS dbt_resource_type,
        TRY_PARSE_JSON(query_tag):materialized::STRING      AS dbt_materialization,
        TRY_PARSE_JSON(query_tag):target_name::STRING       AS dbt_target,
        TRY_PARSE_JSON(query_tag):project_name::STRING      AS dbt_project_name,
        TRY_PARSE_JSON(query_tag):dbt_version::STRING       AS dbt_version,
        TRY_PARSE_JSON(query_tag):full_refresh::BOOLEAN     AS is_full_refresh,
        execution_status = 'FAIL'                           AS is_failure,
        execution_status = 'SUCCESS'                        AS is_success,
        CASE WHEN total_elapsed_time/1000 > 600 THEN 'critical_slow'
             WHEN total_elapsed_time/1000 > 300 THEN 'slow' END AS slow_run_flag,
        ROUND(bytes_scanned / 1073741824.0, 2)              AS gb_scanned,
        (bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0) AS has_spill,
        queued_overload_time / 1000.0 > 30                  AS had_queue_wait,
        DATE_TRUNC('day', start_time)                       AS run_date,
        DATE_TRUNC('hour', start_time)                      AS run_hour
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE TRY_PARSE_JSON(query_tag):app::STRING = 'dbt'
      AND TRY_PARSE_JSON(query_tag):node_resource_type::STRING IN ('model','test','seed','snapshot')
      AND (execution_status = 'FAIL' OR total_elapsed_time/1000 > 300)
      AND start_time > (SELECT MAX(start_time) FROM snowflake_intelligence.FRANCHISE_INTELLIGENT_AGENT_FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.stg_dbt_query_history);

    -- Step 2: enrich new incidents
    INSERT INTO snowflake_intelligence.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.dbt_pipeline_incidents (
        query_id, dbt_model_name, dbt_resource_type, dbt_materialization,
        dbt_invocation_id, warehouse_name, warehouse_size, start_time,
        total_elapsed_seconds, gb_scanned, execution_status, error_code,
        error_message, slow_run_flag, has_spill, had_queue_wait,
        failure_category, fix_suggestion, incident_summary
    )
    SELECT
        query_id, dbt_model_name, dbt_resource_type, dbt_materialization,
        dbt_invocation_id, warehouse_name, warehouse_size, start_time,
        total_elapsed_seconds, gb_scanned, execution_status, error_code,
        error_message, slow_run_flag, has_spill, had_queue_wait,
        AI_CLASSIFY(incident_description,
            ['timeout','schema_drift','logic_error','warehouse_too_small',
             'warehouse_overloaded','missing_object','permission_denied',
             'cost_spike','data_quality_failure','network_error'],
            {'task_description': 'Classify this dbt failure into its root cause category.'}
        ):labels[0]::STRING,
        TRIM(AI_COMPLETE('mistral-large2',
            'You are a senior data engineer. Provide a 2-3 sentence fix for this dbt incident: ' || incident_description
        )),
        incident_description
    FROM snowflake_intelligence.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.dbt_incident_input_v
    WHERE query_id NOT IN (SELECT query_id FROM snowflake_intelligence.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.dbt_pipeline_incidents);

END;
$$
;

-- Start the task
ALTER TASK refresh_pipeline_monitor RESUME;