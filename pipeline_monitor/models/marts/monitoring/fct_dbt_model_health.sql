{{
  config(
    materialized = 'table',
    schema       = 'pipeline_monitor_marts'
  )
}}

with runs as (
    select * from {{ ref('fct_dbt_run_metrics') }}
    where dbt_resource_type = 'model'
),

model_health as (
    select
        dbt_model_name,
        dbt_materialization,
        warehouse_name,

        -- run counts
        count(query_id)                               as total_runs,
        sum(case when is_failure then 1 else 0 end)   as total_failures,
        sum(case when is_success then 1 else 0 end)   as total_successes,
        round(
            100.0 * sum(case when is_success then 1 else 0 end) / nullif(count(query_id), 0),
            1
        )                                             as success_rate_pct,

        -- performance (last 30 days)
        avg(total_elapsed_seconds)                    as avg_elapsed_seconds,
        max(total_elapsed_seconds)                    as max_elapsed_seconds,
        min(total_elapsed_seconds)                    as min_elapsed_seconds,
        percentile_cont(0.95) within group
            (order by total_elapsed_seconds)          as p95_elapsed_seconds,

        -- cost proxy
        avg(gb_scanned)                               as avg_gb_scanned,
        sum(gb_scanned)                               as total_gb_scanned,
        sum(case when has_spill then 1 else 0 end)    as spill_run_count,
        sum(case when had_queue_wait then 1 else 0 end) as queue_wait_count,

        -- recent activity
        max(start_time)                               as last_run_at,
        min(start_time)                               as first_run_at,

        -- 7-day vs 30-day avg to detect regressions
        avg(case when start_time >= dateadd('day', -7, current_timestamp())
                 then total_elapsed_seconds end)       as avg_elapsed_last_7d,
        avg(case when start_time >= dateadd('day', -30, current_timestamp())
                 then total_elapsed_seconds end)       as avg_elapsed_last_30d

    from runs
    group by 1, 2, 3
),

with_trend as (
    select
        *,
        -- regression flag: last 7d avg > 30d avg by >25%
        case
            when avg_elapsed_last_7d > avg_elapsed_last_30d * 1.25
             and avg_elapsed_last_7d is not null
            then true
            else false
        end as is_slowing_down,

        -- health score: composite of success rate + spill + slow
        round(
            success_rate_pct
            - (10 * case when spill_run_count > 0 then 1 else 0 end)
            - (5  * case when is_slowing_down   then 1 else 0 end),
            1
        ) as health_score

    from model_health
)

select * from with_trend