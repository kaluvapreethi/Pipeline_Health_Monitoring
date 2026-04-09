{{
  config(
    materialized = 'table',
    schema       = 'pipeline_monitor_marts'
  )
}}

with runs as (
    select * from {{ ref('stg_dbt_query_history') }}
),

incidents as (
    select
        query_id,
        failure_category,
        fix_suggestion
    from 
    SNOWFLAKE_INTELLIGENCE.FRANCHISE_INTELLIGENT_AGENT_PIPELINE_MONITOR.DBT_PIPELINE_INCIDENTS
),

joined as (
    select
        r.query_id,
        r.dbt_model_name,
        r.dbt_resource_type,
        r.dbt_materialization,
        r.dbt_invocation_id,
        r.warehouse_name,
        r.warehouse_size,
        r.execution_status,
        r.error_message,
        r.start_time,
        r.run_date,
        r.total_elapsed_seconds,
        r.compilation_seconds,
        r.execution_seconds,
        r.gb_scanned,
        r.bytes_spilled_to_local_storage,
        r.bytes_spilled_to_remote_storage,
        r.slow_run_flag,
        r.has_spill,
        r.had_queue_wait,
        r.is_failure,
        r.is_success,
        i.failure_category
    from runs r
    left join incidents i on r.query_id = i.query_id
)

select * from joined
