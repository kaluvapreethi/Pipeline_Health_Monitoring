{{
  config(
    materialized = 'incremental',
    unique_key   = 'query_id',
    schema       = 'pipeline_monitor'
  )
}}

with raw_history as (

    select
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
        -- time in seconds
        total_elapsed_time / 1000.0             as total_elapsed_seconds,
        compilation_time / 1000.0               as compilation_seconds,
        execution_time / 1000.0                 as execution_seconds,
        queued_overload_time / 1000.0           as queued_overload_seconds,
        -- data volume
        bytes_scanned,
        bytes_written,
        partitions_scanned,
        partitions_total,
        rows_produced,
        -- spill (indicates undersized warehouse)
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage

    from {{ source('snowflake_account_usage', 'query_history') }}

    where
        -- only dbt-issued queries
        TRY_PARSE_JSON(query_tag):app::STRING = 'dbt'
        -- only model builds and tests (exclude dbt metadata queries)
        and TRY_PARSE_JSON(query_tag):node_resource_type::STRING
            in ('model', 'test', 'seed', 'snapshot')

    {% if is_incremental() %}
        -- incremental load: only new queries since last run
        and start_time > (select max(start_time) from {{ this }})
    {% else %}
        -- initial load: last 30 days
        and start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    {% endif %}

),

parsed as (

    select
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
        total_elapsed_seconds,
        compilation_seconds,
        execution_seconds,
        queued_overload_seconds,
        bytes_scanned,
        bytes_written,
        partitions_scanned,
        partitions_total,
        rows_produced,
        bytes_spilled_to_local_storage,
        bytes_spilled_to_remote_storage,

        -- parse dbt metadata from query_tag JSON
        TRY_PARSE_JSON(query_tag):node_name::STRING         as dbt_model_name,
        TRY_PARSE_JSON(query_tag):node_id::STRING           as dbt_node_id,
        TRY_PARSE_JSON(query_tag):invocation_id::STRING     as dbt_invocation_id,
        TRY_PARSE_JSON(query_tag):node_resource_type::STRING as dbt_resource_type,
        TRY_PARSE_JSON(query_tag):materialized::STRING      as dbt_materialization,
        TRY_PARSE_JSON(query_tag):target_name::STRING       as dbt_target,
        TRY_PARSE_JSON(query_tag):project_name::STRING      as dbt_project_name,
        TRY_PARSE_JSON(query_tag):dbt_version::STRING       as dbt_version,
        TRY_PARSE_JSON(query_tag):full_refresh::BOOLEAN     as is_full_refresh,

        -- derived flags
        execution_status = 'FAIL'                            as is_failure,
        execution_status = 'SUCCESS'                         as is_success,

        -- slow run flag: >300s is a warning, >600s is critical
        case
            when total_elapsed_seconds > 600 then 'critical_slow'
            when total_elapsed_seconds > 300 then 'slow'
            else null
        end                                                  as slow_run_flag,

        -- cost proxy: bytes scanned / 1GB (rough unit)
        round(bytes_scanned / 1073741824.0, 2)               as gb_scanned,

        -- spill flag (warehouse too small)
        (bytes_spilled_to_local_storage > 0
         or bytes_spilled_to_remote_storage > 0)             as has_spill,

        -- queue wait flag (warehouse overloaded)
        queued_overload_seconds > 30                         as had_queue_wait,

        date_trunc('day', start_time)                        as run_date,
        date_trunc('hour', start_time)                       as run_hour

    from raw_history

)

select * from parsed