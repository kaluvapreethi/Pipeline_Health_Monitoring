USE ROLE tb_data_engineer;
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA franchise_intelligent_agent_pipeline_monitor;
USE WAREHOUSE tb_de_wh;

CREATE OR REPLACE CORTEX SEARCH SERVICE dbt_incident_search
    ON incident_summary
    ATTRIBUTES dbt_model_name, failure_category, warehouse_name,
               warehouse_size, execution_status, start_time
    WAREHOUSE = tb_de_wh
    TARGET_LAG = '1 hour'
    AS (
        SELECT
            incident_id,
            query_id,
            dbt_model_name,
            dbt_resource_type,
            warehouse_name,
            warehouse_size,
            execution_status,
            failure_category,
            fix_suggestion,
            incident_summary,
            start_time,
            total_elapsed_seconds,
            gb_scanned
        FROM SNOWFLAKE_INTELLIGENCE.franchise_intelligent_agent_pipeline_monitor.dbt_pipeline_incidents
    );

    --=======================================================================
    SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'dbt_incident_search',
        '{"query": "timeout failures last week", "columns": ["dbt_model_name","failure_category","fix_suggestion"], "limit": 3}'
    )
) AS results;